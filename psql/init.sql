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
    identity TEXT NULL,
    metric TEXT NOT NULL,
    type TEXT NOT NULL,
    value TEXT NOT NULL,
    CONSTRAINT stats_pk PRIMARY KEY (collected, metric, type)
);

DROP FUNCTION IF EXISTS add_stat(TIMESTAMP, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT);
CREATE FUNCTION add_stat (
    vcollected TIMESTAMP,
    vtopic TEXT,
    vcategory TEXT,
    vsubcategory TEXT,
    videntity TEXT,
    vmetric TEXT,
    vtype TEXT,
    vvalue TEXT
) RETURNS void AS $$
BEGIN
    INSERT INTO stats (collected, topic, category, subcategory, identity, metric, type, value)
    SELECT vcollected, vtopic, vcategory, vsubcategory, videntity, vmetric, vtype, vvalue
    WHERE NOT EXISTS (SELECT 1
        FROM stats
        WHERE stats.collected = vcollected
        AND stats.metric = vmetric
        AND stats.type = vtype
    );
END;
$$ LANGUAGE plpgsql;
