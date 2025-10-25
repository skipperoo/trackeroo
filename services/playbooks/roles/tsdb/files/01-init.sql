CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;
CREATE EXTENSION IF NOT EXISTS pg_cron;
DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'admin') THEN
      CREATE ROLE admin
        WITH LOGIN SUPERUSER CREATEDB CREATEROLE REPLICATION BYPASSRLS
        PASSWORD 'administrator';
   END IF;
END
$$;

-- Application role
DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'apps') THEN
      CREATE ROLE apps
        WITH LOGIN PASSWORD 'apps';
   END IF;
END
$$;
GRANT ALL PRIVILEGES ON DATABASE tracker_db TO admin;

-- TimescaleDB specific permissions
GRANT USAGE ON SCHEMA _timescaledb_catalog TO apps;
GRANT USAGE ON SCHEMA _timescaledb_config TO apps;
GRANT USAGE ON SCHEMA _timescaledb_internal TO apps;
GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_catalog TO apps;
GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_config TO apps;
