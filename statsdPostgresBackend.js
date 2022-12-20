/*
    This is a backend for the etsy/statsd service designed to dump stats
    into PostgreSQL
*/

module.exports = (function () {
  "use strict";
  const fs = require("fs");
  const { Pool, Client } = require("pg");
  const path = require("path");

  // Items we don't want to store but are sent with every statsd flush
  const IGNORED_STATSD_METRICS = [
    "statsd.bad_lines_seen",
    "statsd.packets_received",
    "statsd.metrics_received",
    "statsd.timestamp_lag",
    "processing_time",
  ];

  // The various statsd types as per https://github.com/etsy/statsd/blob/master/docs/metric_types.md
  const STATSD_TYPES = {
    counting: "count",
    timing: "ms",
    gauges: "gauge",
    sets: "set",
  };

  // The path to the SQL script that initializes the table and functions
  // set this to undefined or null to NOT run initializations via node.
  let INITIALIZE_SQL_SCRIPT_FILE_PATH = path.join(
    __dirname,
    "psql",
    "init.sql"
  );

  const STAT_SCHEMA = [
    "topic",
    "category",
    "subcategory",
    "identity",
    "metric",
  ];

  // PostgreSQL configuration properties for module-wide access
  let pghost;
  let pgdb;
  let pgport;
  let pguser;
  let pgpass;
  let pool;

  // Calling this method grabs a connection to PostgreSQL from the connection pool
  // then returns a client to be used. Done must be called at the end of using the
  // connection to return it to the pool.
  const conn = function (callback) {
    pool = new Pool({
      user: pguser,
      host: pghost,
      database: pgdb,
      password: pgpass,
      port: pgport,
    });
    pool.connect(function (err, client, done) {
      return callback(err, client, done);
    });
  };

  // Create stats table and functions should they not exist
  const initializePSQL = function (callback) {
    // If initialization script isn't set then don't attempt to run it. I mean
    // trying to run something that doesn't exist wouldn't make sense, right?
    if (INITIALIZE_SQL_SCRIPT_FILE_PATH == undefined) {
      return callback(null, null);
    }
    conn(function (err, client, done) {
      if (err) {
        return callback(err);
      }
      client.query(
        fs.readFileSync(INITIALIZE_SQL_SCRIPT_FILE_PATH, { encoding: "utf8" }),
        function (queryErr, queryResult) {
          if (queryErr) {
            done();
            return callback(queryErr);
          }
          done();
          return callback(null, queryResult);
        }
      );
    });
    pool.end();
  };

  // Insert new metrics values
  const insertMetric = function (obj, callback) {
    conn(function (err, client, done) {
      if (err) {
        return callback(err);
      }

      if (obj.type == "count" && obj.value == 0) {
        return callback(null, 0);
      }

      if (obj.type == "ms" && obj.value.length == 0) {
        return callback(null, 0);
      }

      client.query(
        {
          text: "SELECT add_stat($1, $2, $3, $4, $5, $6, $7, $8)",
          values: [
            obj.collected,
            obj.topic,
            obj.category,
            obj.subcategory,
            obj.identity,
            obj.metric,
            obj.type,
            obj.value,
          ],
        },
        function (queryErr, queryResult) {
          done();
          if (queryErr) {
            return callback(queryErr);
          }
          return callback(null, queryResult);
        }
      );
    });
    pool.end();
  };

  // Inserts multiple metrics records
  const insertMetrics = function (metrics, callback) {
    const context = this;
    const metrics_copy = (metrics || []).slice(0);
    if (metrics_copy.length === 0) {
      return callback([], []);
    }
    const errResult = [];
    const goodResult = [];
    const metric = metrics_copy.shift();

    const processMetric = function (metric) {
      insertMetric.apply(context, [
        metric,
        function (err, result) {
          if (err) {
            errResult.push(err);
          } else {
            goodResult.push(result);
          }

          metric = metrics_copy.shift();
          if (metric === undefined) {
            return callback(errResult, goodResult);
          }
          return processMetric(metric);
        },
      ]);
    };
    processMetric(metric);
  };

  const parseStatFields = function (statString) {
    const result = {};
    const splitStats = statString.split(".");
    for (const index in splitStats) {
      result[STAT_SCHEMA[index]] = splitStats[index];
    }

    return result;
  };

  // Extracts stats appropriately and returns an array of objects
  const extractor = function (timestamp, stats, type) {
    const results = [];
    for (const statString in stats) {
      if (
        !stats.hasOwnProperty(statString) ||
        IGNORED_STATSD_METRICS.indexOf(statString) !== -1 ||
        statString.indexOf(".") === -1
      )
        continue;

      const stat = {
        collected: new Date(timestamp * 1000).toISOString(),
        type: type,
        value: stats[statString],
        ...parseStatFields(statString),
      };

      results.push(stat);
    }
    return results;
  };

  const extractor_timer_data = function (timestamp, stats) {
    const results = [];
    for (const timer in stats) {
      if (
        !stats.hasOwnProperty(timer) ||
        IGNORED_STATSD_METRICS.indexOf(timer) !== -1 ||
        timer.indexOf(".") !== -1
      )
        continue;

      for (const key in stats[timer]) {
        const stat = {
          collected: new Date(timestamp * 1000).toISOString(),
          type: key,
          value: stats[timer][key],
          ...parseStatFields(timer),
        };

        results.push(stat);
      }
    }
    return results;
  };

  return {
    init: function (startup_time, config, events, logger) {
      pgdb = config.pgdb;
      pghost = config.pghost;
      pgport = config.pgport || 5432;
      pguser = config.pguser;
      pgpass = config.pgpass;

      if (config.pginit !== true) {
        INITIALIZE_SQL_SCRIPT_FILE_PATH = undefined;
      }

      initializePSQL(function (err) {
        if (err) {
          return console.error(err);
        }
      });

      events.on("flush", function (timestamp, statsdMetrics) {
        console.log(statsdMetrics);

        const metrics = extractor(
          timestamp,
          statsdMetrics.counters,
          STATSD_TYPES.counting
        );
        metrics = metrics.concat(
          extractor(timestamp, statsdMetrics.gauges, STATSD_TYPES.gauges)
        );
        metrics = metrics.concat(
          extractor(timestamp, statsdMetrics.sets, STATSD_TYPES.set)
        );
        metrics = metrics.concat(
          extractor(timestamp, statsdMetrics.timers, STATSD_TYPES.timing)
        );
        metrics = metrics.concat(
          extractor_timer_data(timestamp, statsdMetrics.timer_data)
        );

        insertMetrics(metrics, function (errs, goods) {
          if (errs.length > 0) {
            console.error(errs);
          }
        });
      });

      events.on("status", function (callback) {
        callback(null, "postgresBackend", null, null);
      });

      return true;
    },
  };
})();
