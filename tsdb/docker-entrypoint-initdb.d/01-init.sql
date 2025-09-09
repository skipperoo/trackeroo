CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
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

-- Apps user gets full read/write access
GRANT ALL PRIVILEGES ON DATABASE tracker_db TO apps;
GRANT ALL ON SCHEMA public TO apps;
GRANT ALL ON ALL TABLES IN SCHEMA public TO apps;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO apps;
GRANT ALL ON ALL FUNCTIONS IN SCHEMA public TO apps;

-- Ensure apps user gets permissions on future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO apps;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO apps;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS TO apps;

-- TimescaleDB specific permissions
GRANT USAGE ON SCHEMA _timescaledb_catalog TO apps;
GRANT USAGE ON SCHEMA _timescaledb_config TO apps;
GRANT USAGE ON SCHEMA _timescaledb_internal TO apps;
GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_catalog TO apps;
GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_config TO apps;  