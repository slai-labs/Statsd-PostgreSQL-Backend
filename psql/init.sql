/* Drops the tables if they exist */
DROP TABLE IF EXISTS stats CASCADE;

/*
    Create a table to hold all bakula stats

    This table is going to be written to with high frequency but since it needs
    to provide stats in real time we have to both write and read with high frequency.
    Therefore there is a single index in this first version to accommodate write
    moreso than read. This will probably be revisited later (possibly to remove
    the existing index or to make more indexes marked as CONCURRENT)
*/
CREATE TABLE IF NOT EXISTS stats (
    collected TIMESTAMP NOT NULL UNIQUE DEFAULT CURRENT_TIMESTAMP,
    metric TEXT NOT NULL,
    type TEXT NOT NULL,
    value TEXT NOT NULL
);

/* Create add_stat function that takes the timestamp plus the stats */
DROP FUNCTION IF EXISTS add_stat(TIMESTAMP, TEXT, TEXT, TEXT);
CREATE FUNCTION add_stat (
    collected TIMESTAMP,
    metric TEXT,
    type TEXT,
    value TEXT
) RETURNS void AS $$
BEGIN
    INSERT INTO stats (collected, metric, type, value) VALUES (collected, metric, type, value);
END;
$$ LANGUAGE plpgsql;

/* Create another add_stat function that uses the default timestamp and takes the stats */
DROP FUNCTION IF EXISTS add_stat(TEXT, TEXT, TEXT);
CREATE FUNCTION add_stat (
    metric TEXT,
    type TEXT,
    value TEXT
) RETURNS void AS $$
BEGIN
    INSERT INTO stats (metric, type, value) VALUES (metric, type, value);
END;
$$ LANGUAGE plpgsql;

/* Create get_stat function that returns everything because no one was specific */
DROP FUNCTION IF EXISTS get_stat();
CREATE FUNCTION get_stat ()
RETURNS TABLE(
    collected TIMESTAMP,
    metric TEXT,
    type TEXT,
    value TEXT
) AS $$
BEGIN
    RETURN QUERY
        SELECT stats.collected,
            stats.metric,
            stats.type,
            stats.value
        FROM stats;
END;
$$ LANGUAGE plpgsql;

/* Create get_stat function limits what's returned to a start and ending timestamp */
DROP FUNCTION IF EXISTS get_stat(TIMESTAMP, TIMESTAMP);
CREATE FUNCTION get_stat (
    start_time TIMESTAMP,
    end_time TIMESTAMP
)
RETURNS TABLE(
    collected TIMESTAMP,
    metric TEXT,
    type TEXT,
    value TEXT
) AS $$
BEGIN
    RETURN QUERY
        SELECT stats.collected,
            stats.metric,
            stats.type,
            stats.value
        FROM stats
        WHERE stats.collected BETWEEN start_time AND end_time;
END;
$$ LANGUAGE plpgsql;

/* Create get_stat function limits what's returned to a start, ending timestamp and metric name */
DROP FUNCTION IF EXISTS get_stat(TIMESTAMP, TIMESTAMP, TEXT);
CREATE FUNCTION get_stat (
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    matric TEXT
)
RETURNS TABLE(
    collected TIMESTAMP,
    metric TEXT,
    type TEXT,
    value TEXT
) AS $$
BEGIN
    RETURN QUERY
        SELECT stats.collected,
            stats.metric,
            stats.type,
            stats.value
        FROM stats
        WHERE stats.collected BETWEEN start_time AND end_time
            AND stats.metric = metric;
END;
$$ LANGUAGE plpgsql;

/* Create get_stat function limits what's returned to a start, ending timestamp, metric name and type */
DROP FUNCTION IF EXISTS get_stat(TIMESTAMP, TIMESTAMP, TEXT, TEXT);
CREATE FUNCTION get_stat (
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    matric TEXT,
    metric_type TEXT
)
RETURNS TABLE(
    collected TIMESTAMP,
    metric TEXT,
    type TEXT,
    value TEXT
) AS $$
BEGIN
    RETURN QUERY
        SELECT stats.collected,
            stats.metric,
            stats.type,
            stats.value
        FROM stats
        WHERE stats.collected BETWEEN start_time AND end_time
            AND stats.metric = metric
            AND stats.type = metric_type;
END;
$$ LANGUAGE plpgsql;
