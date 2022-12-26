/*
    This is a backend for the etsy/statsd service designed to dump stats
    into PostgreSQL
*/

module.exports = (function () {
  "use strict";
  const { Pool } = require("pg");

  // Items we don't want to store but are sent with every statsd flush
  var IGNORED_STATSD_METRICS = [
    "statsd.bad_lines_seen",
    "statsd.packets_received",
    "statsd.metrics_received",
    "statsd.timestamp_lag",
    "processing_time",
  ];

  // The various statsd types as per https://github.com/etsy/statsd/blob/master/docs/metric_types.md
  var STATSD_TYPES = {
    counting: "count",
    timing: "ms",
    gauges: "gauge",
    sets: "set",
  };

  // PostgreSQL configuration properties for module-wide access
  var pghost;
  var pgdb;
  var pgport;
  var pguser;
  var pgpass;
  var pool;

  // Calling this method grabs a connection to PostgreSQL from the connection pool
  // then returns a client to be used. Done must be called at the end of using the
  // connection to return it to the pool.
  var conn = function (callback) {
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

  // Insert new metrics values
  var insertMetric = function (obj, callback) {
    conn(function (err, client, done) {
      if (err) {
        console.error("Unable to connect to postgres:", err);
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
  var insertMetrics = function (metrics, callback) {
    var context = this;
    var metrics_copy = (metrics || []).slice(0);
    if (metrics_copy.length === 0) {
      return callback([], []);
    }
    var errResult = [];
    var goodResult = [];
    var metric = metrics_copy.shift();

    var processMetric = function (metric) {
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

  // Extracts stats appropriately and returns an array of objects
  var extractor = function (timestamp, stats, type) {
    var results = [];

    for (var key in stats) {
      if (!stats.hasOwnProperty(key)) continue;
      if (IGNORED_STATSD_METRICS.indexOf(key) !== -1) continue;

      var stat = {
        collected: new Date(timestamp * 1000).toISOString(),
        type: type,
        value: stats[key],
      };

      if (key.indexOf(".") !== -1) {
        // Assume the metric format is: topic.category.subcategory.identity.metric
        var splits = key.split(".");
        stat.metric = splits.pop();
        stat.topic = splits[0];
        stat.category = splits[1];
        stat.subcategory = splits[2];
        stat.identity = splits[3];
      } else {
        stat.metric = key;
      }
      results.push(stat);
    }
    return results;
  };

  var extractor_timer_data = function (timestamp, stats) {
    var results = [];
    for (var timer in stats) {
      if (!stats.hasOwnProperty(timer)) continue;
      if (IGNORED_STATSD_METRICS.indexOf(timer) !== -1) continue;

      for (var key in stats[timer]) {
        var stat = {
          collected: new Date(timestamp * 1000).toISOString(),
          type: key,
          value: stats[timer][key],
        };
        if (timer.indexOf(".") !== -1) {
          // Assume the metric format is: topic.category.subcategory.identity.metric
          var splits = timer.split(".");
          stat.metric = splits.pop();
          stat.topic = splits[0];
          stat.category = splits[1];
          stat.subcategory = splits[2];
          stat.identity = splits[3];
        } else {
          stat.metric = timer;
        }
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

      events.on("flush", function (timestamp, statsdMetrics) {
        var metrics = extractor(
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
