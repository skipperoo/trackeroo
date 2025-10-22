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

CREATE INDEX IF NOT EXISTS idx_trackeroo_aggregated_dev_id ON trackeroo.aggregated(dev_id);
CREATE INDEX IF NOT EXISTS idx_trackeroo_aggregated_route_hash ON trackeroo.aggregated(route_hash);
CREATE INDEX IF NOT EXISTS idx_trackeroo_aggregated_tag ON trackeroo.aggregated(tag);
CREATE INDEX IF NOT EXISTS idx_trackeroo_aggregated_ts ON trackeroo.aggregated(ts);

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
