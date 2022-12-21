/*
    This is a backend for the etsy/statsd service designed to dump stats
    into PostgreSQL
*/

module.exports = (function () {
  "use strict";
  const fs = require("fs");
  const { Pool } = require("pg");
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
    count: "count",
    timer: "timer",
    gauges: "gauge",
    sets: "sets",
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
  let pgPool = null;

  // Calling this method grabs a connection to PostgreSQL from the connection pool
  // then returns a client to be used. Done must be called at the end of using the
  // connection to return it to the pool.
  const initConnectionPool = async function () {
    const newPool = new Pool({
      user: pguser,
      host: pghost,
      database: pgdb,
      password: pgpass,
      port: pgport,
    });

    await newPool.connect();
    return newPool;
  };

  // Insert new metrics values
  const insertMetric = async function (obj) {
    if (obj.type == "count" && obj.value == 0) {
      return console.log(null, 0);
    }

    if (obj.type == "ms" && obj.value.length == 0) {
      return console.log(null, 0);
    }

    await pgPool.query({
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
    });
  };

  // Inserts multiple metrics records
  const insertMetrics = async function (metrics) {
    const metrics_copy = (metrics || []).slice(0);

    if (metrics_copy.length === 0) {
      return console.log("No metrics to insert");
    }

    for (const index in metrics_copy) {
      try {
        await insertMetric(metrics_copy[index]);
      } catch (error) {
        console.log(error);
      }
    }
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
    if (type === STATSD_TYPES.timer) {
      console.log(stats);
    }

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

  return {
    init: async function (startup_time, config, events, logger) {
      pgdb = config.pgdb;
      pghost = config.pghost;
      pgport = config.pgport || 5432;
      pguser = config.pguser;
      pgpass = config.pgpass;

      if (pgPool === null) {
        pgPool = await initConnectionPool();
      }

      events.on("flush", function (timestamp, statsdMetrics) {
        let metrics = extractor(
          timestamp,
          statsdMetrics.counters,
          STATSD_TYPES.count
        );
        metrics = metrics.concat(
          extractor(timestamp, statsdMetrics.gauges, STATSD_TYPES.gauges)
        );

        metrics = metrics.concat(
          extractor(timestamp, statsdMetrics.sets, STATSD_TYPES.sets)
        );

        metrics = metrics.concat(
          extractor(timestamp, statsdMetrics.timers, STATSD_TYPES.timers)
        );

        insertMetrics(metrics);
      });

      events.on("status", function (callback) {
        callback(null, "postgresBackend", null, null);
      });

      return true;
    },
    stop: function (callback) {
      if (pgPool !== null) {
        pgPool.end();
      }
      callback();
    },
  };
})();
