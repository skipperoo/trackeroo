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

SELECT create_hypertable('trackeroo.data', 'ts');
SELECT create_hypertable('trackeroo.aggregated', 'ts');

CREATE INDEX IF NOT EXISTS idx_trackeroo_data_dev_id ON trackeroo.data(dev_id);
CREATE INDEX IF NOT EXISTS idx_trackeroo_data_tag ON trackeroo.data(tag);
CREATE INDEX IF NOT EXISTS idx_trackeroo_data_ts ON trackeroo.data(ts);

CREATE INDEX IF NOT EXISTS idx_trackeroo_aggregated_dev_id ON trackeroo.aggregated(dev_id);
CREATE INDEX IF NOT EXISTS idx_trackeroo_aggregated_route_hash ON trackeroo.aggregated(route_hash);
CREATE INDEX IF NOT EXISTS idx_trackeroo_aggregated_tag ON trackeroo.aggregated(tag);
CREATE INDEX IF NOT EXISTS idx_trackeroo_aggregated_ts ON trackeroo.aggregated(ts);