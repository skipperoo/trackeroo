CREATE SCHEMA IF NOT EXISTS trackeroo;

CREATE TABLE IF NOT EXISTS trackeroo.data (
    ts_unix BIGINT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    dev_id TEXT NOT NULL,
    insertion_time TIMESTAMPTZ DEFAULT NOW(),
    tag TEXT NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (ts, dev_id)
);

CREATE TABLE IF NOT EXISTS trackeroo.aggregated (
    ts_unix BIGINT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    dev_id TEXT NOT NULL,
    route_hash TEXT NOT NULL,
    insertion_time TIMESTAMPTZ DEFAULT NOW(),
    tag TEXT NOT NULL,
    payload JSONB NOT NULL,

    PRIMARY KEY (ts, route_hash, dev_id)
);

SELECT create_hypertable('trackeroo.data', 'ts', 'dev_id', 4);
SELECT create_hypertable('trackeroo.aggregated', 'ts', 'dev_id', 4);
SELECT set_chunk_time_interval('trackeroo.data', INTERVAL '12 hours');
SELECT set_chunk_time_interval('trackeroo.aggregated', INTERVAL '12 hours');
CREATE INDEX IF NOT EXISTS idx_trackeroo_data_dev_id ON trackeroo.data(dev_id);
CREATE INDEX IF NOT EXISTS idx_trackeroo_data_tag ON trackeroo.data(tag);
CREATE INDEX IF NOT EXISTS idx_trackeroo_data_ts ON trackeroo.data(ts);

-- Added to speedup the last position query
CREATE INDEX IF NOT EXISTS idx_trackeroo_data_dev_id_ts_desc ON trackeroo.data (dev_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_data_device_type ON trackeroo.data ((payload->>'device_type'));
ALTER TABLE trackeroo.data SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'dev_id,tag',
    timescaledb.compress_orderby = 'ts DESC'
);
SELECT add_compression_policy('trackeroo.data', INTERVAL '6 hours');

CREATE INDEX IF NOT EXISTS idx_trackeroo_aggregated_dev_id ON trackeroo.aggregated(dev_id);
CREATE INDEX IF NOT EXISTS idx_trackeroo_aggregated_route_hash ON trackeroo.aggregated(route_hash);
CREATE INDEX IF NOT EXISTS idx_trackeroo_aggregated_tag ON trackeroo.aggregated(tag);
CREATE INDEX IF NOT EXISTS idx_trackeroo_aggregated_ts ON trackeroo.aggregated(ts);
ALTER TABLE trackeroo.aggregated SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'dev_id,route_hash,tag',
    timescaledb.compress_orderby = 'ts DESC'
);

SELECT add_compression_policy('trackeroo.aggregated', INTERVAL '6 hours');
SELECT add_retention_policy('trackeroo.data', INTERVAL '3 days');
SELECT add_retention_policy('trackeroo.aggregated', INTERVAL '3 days');

-- Now I have to allow apps to read/write on all current tables
GRANT USAGE ON SCHEMA trackeroo TO apps;
--
-- Grant read/write on all current tables
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA trackeroo TO apps;

-- Grant read/write on all sequences (needed if you ever add SERIAL/IDENTITY columns)
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA trackeroo TO apps;

-- Ensure future tables/sequences also inherit these permissions
ALTER DEFAULT PRIVILEGES IN SCHEMA trackeroo
   GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO apps;

ALTER DEFAULT PRIVILEGES IN SCHEMA trackeroo
   GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO apps;

-- Last position materialized views
CREATE MATERIALIZED VIEW trackeroo.latest_positions AS
SELECT d.dev_id,
       last.ts,
       (last.payload->'position'->>'lat')::double precision AS lat,
       (last.payload->'position'->>'lon')::double precision AS lon,
       last.payload->>'device_name' AS device_name,
       last.payload->>'device_type' AS device_type
FROM (SELECT DISTINCT dev_id FROM trackeroo.data) d
CROSS JOIN LATERAL (
    SELECT ts, payload
    FROM trackeroo.data
    WHERE trackeroo.data.dev_id = d.dev_id
      AND (payload->'position'->>'lat') IS NOT NULL
      AND (payload->'position'->>'lon') IS NOT NULL
    ORDER BY ts DESC
    LIMIT 1
) last;

GRANT UPDATE, SELECT ON trackeroo.latest_positions TO apps;
CREATE UNIQUE INDEX ON trackeroo.latest_positions (dev_id);
