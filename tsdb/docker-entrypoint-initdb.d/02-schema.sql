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

-- Latest positions continuous aggregate (replaces materialized view)
CREATE MATERIALIZED VIEW trackeroo.latest_positions
WITH (timescaledb.continuous) AS
SELECT
    dev_id,
    time_bucket('30 seconds', ts) AS bucket,
    last(ts, ts) AS ts,
    last((payload->'position'->>'lat')::double precision, ts) AS lat,
    last((payload->'position'->>'lon')::double precision, ts) AS lon,
    last(payload->>'device_name', ts) AS device_name,
    last(payload->>'device_type', ts) AS device_type
FROM trackeroo.data
WHERE (payload->'position'->>'lat') IS NOT NULL
    AND (payload->'position'->>'lon') IS NOT NULL
GROUP BY dev_id, bucket
WITH NO DATA;

-- Add automatic refresh policy (refreshes every 2 minutes)
SELECT add_continuous_aggregate_policy('trackeroo.latest_positions',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '30 seconds',
    schedule_interval => INTERVAL '2 minutes');

-- Create unique index on the continuous aggregate
CREATE UNIQUE INDEX idx_latest_positions_dev_bucket ON trackeroo.latest_positions (dev_id, bucket);

-- Create a simple view to get the current position for each device
-- This view queries the continuous aggregate and returns only the latest position per device
CREATE VIEW trackeroo.current_positions AS
SELECT DISTINCT ON (dev_id)
    dev_id,
    ts,
    lat,
    lon,
    device_name,
    device_type
FROM trackeroo.latest_positions
ORDER BY dev_id, bucket DESC;

-- Grant permissions on the new views
GRANT SELECT ON trackeroo.latest_positions TO apps;
GRANT SELECT ON trackeroo.current_positions TO apps;
