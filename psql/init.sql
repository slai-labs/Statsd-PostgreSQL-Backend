/*
    Create a table to hold all statsd stats

    This table is going to be written to with high frequency but since it needs
    to provide stats in real time we have to both write and read with high frequency.
    Therefore let's limit indexing to as much as possible.
*/
CREATE TABLE IF NOT EXISTS stats (
    collected TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    topic TEXT NULL,
    category TEXT NULL,
    subcategory TEXT NULL,
    metric TEXT NOT NULL,
    type TEXT NOT NULL,
    value TEXT NOT NULL,
    CONSTRAINT stats_pk PRIMARY KEY (collected, metric, type)
);

/* Create a add_stat overload */
DROP FUNCTION IF EXISTS add_stat(TIMESTAMP, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT);
CREATE FUNCTION add_stat (
    vcollected TIMESTAMP,
    vtopic TEXT,
    vcategory TEXT,
    vsubcategory TEXT,
    vmetric TEXT,
    vtype TEXT,
    vvalue TEXT
) RETURNS void AS $$
BEGIN
    INSERT INTO stats (collected, topic, category, subcategory, metric, type, value)
    SELECT vcollected, vtopic, vcategory, vsubcategory, vmetric, vtype, vvalue
    WHERE NOT EXISTS (SELECT 1
        FROM stats
        WHERE stats.collected = vcollected
        AND stats.topic = vtopic
        AND stats.category = vcategory
        AND stats.subcategory = vsubcategory
        AND stats.metric = vmetric
        AND stats.type = vtype
        AND stats.value = vvalue
    );
END;
$$ LANGUAGE plpgsql;

/* Create a get_stat overload */
DROP FUNCTION IF EXISTS get_stat();
CREATE FUNCTION get_stat ()
RETURNS TABLE(
    collected TIMESTAMP,
    topic TEXT,
    category TEXT,
    subcategory TEXT,
    metric TEXT,
    type TEXT,
    value TEXT
) AS $$
BEGIN
    RETURN QUERY
        SELECT stats.collected,
            stats.topic,
            stats.category,
            stats.subcategory,
            stats.metric,
            stats.type,
            stats.value
        FROM stats;
END;
$$ LANGUAGE plpgsql;

/* Create a get_stat overload */
DROP FUNCTION IF EXISTS get_stat(TIMESTAMP, TIMESTAMP);
CREATE FUNCTION get_stat (
    start_time TIMESTAMP,
    end_time TIMESTAMP
)
RETURNS TABLE(
    collected TIMESTAMP,
    topic TEXT,
    category TEXT,
    subcategory TEXT,
    metric TEXT,
    type TEXT,
    value TEXT
) AS $$
BEGIN
    RETURN QUERY
        SELECT stats.collected,
            stats.topic,
            stats.category,
            stats.subcategory,
            stats.metric,
            stats.type,
            stats.value
        FROM stats
        WHERE stats.collected BETWEEN start_time AND end_time;
END;
$$ LANGUAGE plpgsql;

/* Create a get_stat overload */
DROP FUNCTION IF EXISTS get_stat(TIMESTAMP, TIMESTAMP, TEXT);
CREATE FUNCTION get_stat (
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    vmatric TEXT
)
RETURNS TABLE(
    collected TIMESTAMP,
    topic TEXT,
    category TEXT,
    subcategory TEXT,
    metric TEXT,
    type TEXT,
    value TEXT
) AS $$
BEGIN
    RETURN QUERY
        SELECT stats.collected,
            stats.topic,
            stats.category,
            stats.subcategory,
            stats.metric,
            stats.type,
            stats.value
        FROM stats
        WHERE stats.collected BETWEEN start_time AND end_time
            AND stats.metric = vmetric;
END;
$$ LANGUAGE plpgsql;

/* Create a get_stat overload */
DROP FUNCTION IF EXISTS get_stat(TIMESTAMP, TIMESTAMP, TEXT, TEXT);
CREATE FUNCTION get_stat (
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    vmatric TEXT,
    vtype TEXT
)
RETURNS TABLE(
    collected TIMESTAMP,
    topic TEXT,
    category TEXT,
    subcategory TEXT,
    metric TEXT,
    type TEXT,
    value TEXT
) AS $$
BEGIN
    RETURN QUERY
        SELECT stats.collected,
            stats.topic,
            stats.category,
            stats.subcategory,
            stats.metric,
            stats.type,
            stats.value
        FROM stats
        WHERE stats.collected BETWEEN start_time AND end_time
            AND stats.metric = vmetric
            AND stats.type = vtype;
END;
$$ LANGUAGE plpgsql;

/* Create a get_stat overload */
DROP FUNCTION IF EXISTS get_stat(TIMESTAMP, TIMESTAMP, TEXT, TEXT, TEXT);
CREATE FUNCTION get_stat (
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    vtopic TEXT,
    vmatric TEXT,
    vtype TEXT
)
RETURNS TABLE(
    collected TIMESTAMP,
    topic TEXT,
    category TEXT,
    subcategory TEXT,
    metric TEXT,
    type TEXT,
    value TEXT
) AS $$
BEGIN
    RETURN QUERY
        SELECT stats.collected,
            stats.topic,
            stats.category,
            stats.subcategory,
            stats.metric,
            stats.type,
            stats.value
        FROM stats
        WHERE stats.collected BETWEEN start_time AND end_time
            AND stats.metric = vmetric
            AND stats.type = vtype
            AND stats.topic = vtopic;
END;
$$ LANGUAGE plpgsql;

/* Create a get_stat overload */
DROP FUNCTION IF EXISTS get_stat(TIMESTAMP, TIMESTAMP, TEXT, TEXT, TEXT, TEXT);
CREATE FUNCTION get_stat (
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    vtopic TEXT,
    vcategory TEXT,
    vmatric TEXT,
    vtype TEXT
)
RETURNS TABLE(
    collected TIMESTAMP,
    topic TEXT,
    category TEXT,
    subcategory TEXT,
    metric TEXT,
    type TEXT,
    value TEXT
) AS $$
BEGIN
    RETURN QUERY
        SELECT stats.collected,
            stats.topic,
            stats.category,
            stats.subcategory,
            stats.metric,
            stats.type,
            stats.value
        FROM stats
        WHERE stats.collected BETWEEN start_time AND end_time
            AND stats.metric = vmetric
            AND stats.type = vtype
            AND stats.topic = vtopic
            AND stats.category = vcategory;
END;
$$ LANGUAGE plpgsql;

/* Create a get_stat overload */
DROP FUNCTION IF EXISTS get_stat(TIMESTAMP, TIMESTAMP, TEXT, TEXT, TEXT, TEXT, TEXT);
CREATE FUNCTION get_stat (
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    vtopic TEXT,
    vcategory TEXT,
    vsubcategory TEXT,
    vmatric TEXT,
    vtype TEXT
)
RETURNS TABLE(
    collected TIMESTAMP,
    topic TEXT,
    category TEXT,
    subcategory TEXT,
    metric TEXT,
    type TEXT,
    value TEXT
) AS $$
BEGIN
    RETURN QUERY
        SELECT stats.collected,
            stats.topic,
            stats.category,
            stats.subcategory,
            stats.metric,
            stats.type,
            stats.value
        FROM stats
        WHERE stats.collected BETWEEN start_time AND end_time
            AND stats.metric = vmetric
            AND stats.type = vtype
            AND stats.topic = vtopic
            AND stats.category = vcategory
            AND stats.subcategory = vsubcategory;
END;
$$ LANGUAGE plpgsql;
