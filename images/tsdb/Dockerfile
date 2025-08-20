FROM postgres:17-bookworm

RUN apt-get update && apt-get install -y \
    gnupg wget ca-certificates lsb-release software-properties-common && \
    rm -rf /var/lib/apt/lists/*

# Add TimescaleDB repo key & source
RUN wget -qO- https://packagecloud.io/timescale/timescaledb/gpgkey | gpg --dearmor -o /usr/share/keyrings/timescale.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/timescale.gpg] https://packagecloud.io/timescale/timescaledb/debian $(lsb_release -c -s) main" \
    > /etc/apt/sources.list.d/timescaledb.list

# Add PostgreSQL repo key & source (contains PostGIS)
RUN wget -qO- https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgres.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/postgres.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -c -s)-pgdg main" \
    > /etc/apt/sources.list.d/pgdg.list

# Install TimescaleDB + PostGIS
RUN apt-get update && apt-get install -y \
    timescaledb-2-postgresql-17 \
    postgresql-17-postgis-3 \
    postgresql-17-postgis-3-scripts \
    && rm -rf /var/lib/apt/lists/*

# Enable TimescaleDB preload
RUN echo "shared_preload_libraries='timescaledb'" >> /usr/share/postgresql/postgresql.conf.sample
