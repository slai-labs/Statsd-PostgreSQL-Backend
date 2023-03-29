/*
    This is a backend for the etsy/statsd service designed to dump stats
    into PostgreSQL
*/

module.exports = (function () {
  "use strict";
  const { Pool } = require("pg");

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

  const STAT_SCHEMA = [
    "topic",
    "category",
    "subcategory",
    "identity",
    "metric",
  ];

  let pgPool;
  let pgdb;
  let pghost;
  let pgport;
  let pguser;
  let pgpass;

  // Calling this method grabs a connection to PostgreSQL from the connection pool
  // then returns a client to be used. Done must be called at the end of using the
  // connection to return it to the pool.
  const initConnectionPool = async function (defaultConfig) {
    pgdb = defaultConfig.pgdb;
    pghost = defaultConfig.pghost;
    pgport = defaultConfig.pgport || 5432;
    pguser = defaultConfig.pguser;
    pgpass = defaultConfig.pgpass;

    // If config path is set, override config with values from secrets (from externalsecrets)
    if (process.env.CONFIG_PATH) {
      console.log(
        "Using config values from CONFIG_PATH: ",
        process.env.CONFIG_PATH
      );

      require("dotenv").config({ path: process.env.CONFIG_PATH });
      pgdb = process.env.DB_NAME;
      pghost = process.env.DB_HOST;
      pgport = process.env.DB_PORT;
      pguser = process.env.DB_USER;
      pgpass = process.env.DB_PASS;
    }

    const newPool = new Pool({
      user: pguser,
      host: pghost,
      database: pgdb,
      password: pgpass,
      port: pgport,
      keepAlive: true,
    });

    await newPool.connect();
    return newPool;
  };

  // Insert new metrics values
  const insertMetric = async function (obj) {
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

    for (const index in metrics_copy) {
      try {
        await insertMetric(metrics_copy[index]);
        console.log("Inserted metric: ", metrics_copy[index].metric);
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

  // Ignore values that are either empty arrays or 0
  const isEmptyValue = function (value, type) {
    if (type === STATSD_TYPES.count && value === 0) {
      return true;
    }

    if (type === STATSD_TYPES.timer && value.length === 0) {
      return true;
    }

    return false;
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

      if (isEmptyValue(stats[statString], type)) continue;

      const stat = {
        collected: new Date(timestamp * 1000).toISOString(),
        type: type,
        value: JSON.stringify(stats[statString]),
        ...parseStatFields(statString),
      };

      results.push(stat);
    }
    return results;
  };

  return {
    init: async function (startup_time, config, events, logger) {
      if (!pgPool) {
        pgPool = await initConnectionPool(config);
      }

      console.log(events);

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
          extractor(timestamp, statsdMetrics.timers, STATSD_TYPES.timer)
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
