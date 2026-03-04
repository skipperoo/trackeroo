# Trackeroo

Distributed IoT tracking platform for large-scale sensor data ingestion, real-time stream processing, and time-series storage.
The aim of the project is to create a platform capable of monitoring fleets of vehicles in real-time, providing insights and statistics on each route.

## Architecture

- **Ingestion**: IoT devices (simulated by `Tracky`) publish via MQTT to **RabbitMQ**.
- **Routing**: `Brokeroo` (Go) routes raw data to **TimescaleDB** and streams to **Kafka**.
- **Processing**: **Apache Flink** (Java) performs real-time aggregation and anomaly detection.
- **Storage**: **MongoDB** (metadata), **Redis** (caching), and **TimescaleDB** (telemetry).
- **Visualization**: **Grafana** dashboards for fleet monitoring and system health.

## Quick Start (Local)

Requires Docker and Docker Compose.

```bash
docker-compose up --build
```

### Access Points

- **Backend API**: `http://localhost:8080`
- **Grafana**: `http://localhost:3000` (admin/admin123)
- **Flink Dashboard**: `http://localhost:8081`
- **RabbitMQ**: `http://localhost:15672` (admin/admin123)

## Deployment

The project is designed for **Kubernetes (K3s)** using:

- **Ansible**: K3s installation and service orchestration.
- **KEDA**: Event-driven autoscaling for ingestion workers.

## Project Structure

- `/trackeroo-backend`: Go REST API for management and auth.
- `/brokeroo`: Go service bridging MQTT, Kafka, and Postgres.
- `/flink_job`: Java/Gradle stream processing logic.
- `/tracky`: Go IoT agent for realistic vehicle simulation.
- `/bootstrap`: IaC for virtualized cluster setup.
- `/services`: IaC for services deployment to Kubernetes.

## Development Notes

Key implementation details from the project development:

- **Backend**: Built with **Go 1.24**, using only the standard library for HTTP routing to ensure a minimal and robust service. It has become the library [routy](github.com/skipperoo/routy).
- **Data Reliability**: **RabbitMQ Quorum queues** ensure high availability and data durability across cluster nodes.
- **Geo-Spatial Time-Series**: **TimescaleDB** is enhanced with the **PostGIS** plugin to optimize complex geo-spatial queries and high-throughput ingestion.
- **Cluster Topology**: High-availability **K3s** deployment (3 server + 3 agent nodes) with **dual-network configuration** to segregate internal cluster traffic (virtual multi-Gbps network) from external access.
- **Realistic Simulation**: The **Tracky** agent uses **Overpass API** and **OSRM** to simulate realistic vehicle movement across real-world road networks, rather than generating random data.
