/*
    Create a table to hold all statsd stats

    This table is going to be written to with high frequency but since it needs
    to provide stats in real time we have to both write and read with high frequency.
    Therefore let's limit indexing to as much as possible.
*/
CREATE TABLE IF NOT EXISTS stats (
    collected TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    topic varchar NULL,
    category varchar NULL,
    subcategory varchar NULL,
    identity varchar NULL,
    metric varchar NOT NULL,
    type varchar NOT NULL,
    value TEXT NOT NULL
);
CREATE UNIQUE INDEX stats_uniq ON stats (collected, topic, category, subcategory, identity, metric, type);

DROP FUNCTION IF EXISTS add_stat(TIMESTAMP, varchar, varchar, varchar, varchar, varchar, varchar, TEXT);
CREATE FUNCTION add_stat (
    vcollected TIMESTAMP,
    vtopic varchar,
    vcategory varchar,
    vsubcategory varchar,
    videntity varchar,
    vmetric varchar,
    vtype varchar,
    vvalue TEXT
) RETURNS void AS $$
BEGIN
    INSERT INTO stats (collected, topic, category, subcategory, identity, metric, type, value)
    SELECT vcollected, vtopic, vcategory, vsubcategory, videntity, vmetric, vtype, vvalue
    WHERE NOT EXISTS (SELECT 1
        FROM stats
        WHERE stats.collected = vcollected
        AND stats.topic = vtopic
        AND stats.category = vcategory
        AND stats.subcategory = vsubcategory
        AND stats.identity = videntity
        AND stats.metric = vmetric
        AND stats.type = vtype
    );
END;
$$ LANGUAGE plpgsql;
