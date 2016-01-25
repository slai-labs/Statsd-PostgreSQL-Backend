/*
    This is a backend for the etsy/statsd service designed to dump stats
    into PostgreSQL
*/

module.exports = (function() {
    "use strict";
    var fs = require("fs");
    var pg = require("pg");
    var path = require("path");

    // Items we don't want to store but are sent with every statsd flush
    var IGNORED_STATSD_METRICS = [
        "statsd.bad_lines_seen",
        "statsd.packets_received",
        "statsd.metrics_received",
        "statsd.timestamp_lag",
        "processing_time"
    ];

    // The various statsd types as per https://github.com/etsy/statsd/blob/master/docs/metric_types.md
    var STATSD_TYPES = {
        counting: "c",
        timing: "ms",
        gauges: "g",
        sets: "s"
    };

    // The path to the SQL script that initializes the table and functions
    var INITIALIZE_SQL_SCRIPT_FILE_PATH = path.join(__dirname, "psql", "init.sql");

    // PostgreSQL configuration properties for module-wide access
    var pghost;
    var pgdb;
    var pgport;
    var pguser;
    var pgpass;

    // Generated and cached PostgreSQL connection string
    var connStr;

    // Return connection string; this lets it be lazy loaded
    var connectionString = function() {
        if (connStr === undefined) {
            connStr = "postgres://";
            connStr += (pguser) ? pguser : "";
            connStr += (pguser && pgpass) ? ":" + pgpass : "";
            connStr += (pguser) ? "@" : "";
            connStr += (pghost) ? pghost + ":" + pgport : "";
            connStr += (pgdb) ? "/" + pgdb : "";
        }
        return connStr;
    }

    // Calling this method grabs a connection to PostgreSQL from the connection pool
    // then returns a client to be used.
    var conn = function(callback) {
        pg.connect(connectionString(), function(err, client, done) {
            return callback(err, client, done);
        });
    };

    // Create stats table and functions should they not exist
    var initializePSQL = function(callback) {
        conn(function(err, client, done) {
            if (err) {
                return callback(err);
            }
            client.query(fs.readFileSync(INITIALIZE_SQL_SCRIPT_FILE_PATH, { encoding: "utf8" }), function(queryErr, queryResult) {
                if (queryErr) {
                    done();
                    return callback(queryErr);
                }
                done();
                return callback(null, queryResult);
            });
        });
    };

    // Insert new metrics values
    var insertMetric = function(timestamp, metric, mtype, value, callback) {
        conn(function(err, client, done) {
            if (err) {
                return callback(err);
            }

            client.query({
                text: "SELECT add_stat($1, $2, $3)",
                values: [metric, mtype, value]
            }, function(queryErr, queryResult) {
                done();
                if (queryErr) {
                    return callback(queryErr);
                }
                return callback(null, queryResult);
            });
        });
    };

    // Inserts multiple metrics records
    var insertMetrics = function(metrics, callback) {
        var context = this;
        var metrics_copy = (metrics || []).slice(0);
        if (metrics_copy.length === 0) {
            return callback([], []);
        }
        var errResult = [];
        var goodResult = [];
        var metric = metrics_copy.shift();

        var processMetric = function(metric) {
            insertMetric.apply(context, metric.concat(function(err, result) {
                if (err) {
                    errResult.push(err);
                } else {
                    goodResult.push(result);
                }

                var metric = metrics_copy.shift();
                if (metric === undefined) {
                    return callback(errResult, goodResult);
                }
                return processMetric(metric);
            }));
        };
        processMetric(metric);
    };

    return {
        init: function(startup_time, config, events, logger) {
            pgdb = config.pgdb;
            pghost = config.pghost;
            pgport = config.pgport || 5432;
            pguser = config.pguser;
            pgpass = config.pgpass;

            initializePSQL(function(err) {
                if (err) {
                    return console.error(err);
                }
            });

            events.on("flush", function(timestamp, statsdMetrics) {
                var metrics = [];
                for (var key in statsdMetrics.counters) {
                    if (statsdMetrics.counters.hasOwnProperty(key) && IGNORED_STATSD_METRICS.indexOf(key) === -1) {
                        metrics.push([timestamp, STATSD_TYPES.counting, key, statsdMetrics.counters[key]]);
                    }
                }

                insertMetrics(metrics, function(errs, goods) {
                    if (errs.length > 0) {
                        console.error(errs);
                    }
                });
            });

            events.on("status", function(callback) {
                callback(null, "postgresBackend", null, null);
            });

            return true;
        }
    };
}());
