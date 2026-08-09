# Energy Lakehouse Platform

Open Source Lakehouse platform for integrating and analyzing public meteorological and energy data from Spain.

## 📖 Project Overview

This project is being developed as a Master's Thesis (MSc Big Data & Data Engineering).

Its objective is to design and implement a complete Open Source Lakehouse platform capable of integrating, processing, storing and visualizing public meteorological and energy data from Spain.

The project focuses on Data Engineering concepts using a real-world use case based on public meteorological and energy data.

The platform follows a Lakehouse architecture based entirely on Open Source technologies and is designed to be reproducible in a local environment using Docker Compose.

---

## 🏗️ Architecture

The platform follows a Medallion Architecture:

```text
Data Sources
     │
     ▼
  Ingestion
     │
     ▼
┌─────────┐
│ Bronze  │
└────┬────┘
     │
     ▼
┌─────────┐
│ Silver  │
└────┬────┘
     │
     ▼
┌─────────┐
│  Gold   │
└────┬────┘
     │
     ▼
Trino / SQL
     │
     ▼
  Superset
```

Main infrastructure components:

```text
                    Apache Airflow
                         │
                         ▼
                   Apache Spark
                         │
                         ▼
              Apache Iceberg Tables
                         │
                  ┌──────┴──────┐
                  ▼             ▼
                MinIO         Trino
                                │
                                ▼
                         Apache Superset

                    PostgreSQL
                Metadata / Catalog
```

A detailed architecture diagram is available in the project documentation.

---

## ⚙️ Technologies

### Data Engineering

- Python
- Apache Spark
- PySpark
- Apache Iceberg

### Storage

- MinIO
- PostgreSQL

### SQL Query Engine

- Trino

### Orchestration

- Apache Airflow

### Analytics & Visualization

- Apache Superset

### Infrastructure

- Docker
- Docker Compose
- Git
- GitHub

---

## 📊 Data Sources

The platform is designed to integrate public data from:

- AEMET — Spanish State Meteorological Agency
- Open-Meteo — Meteorological data
- REE / ESIOS — Spanish electricity system data

---

## 🥉🥈🥇 Medallion Architecture

### Bronze

Raw data obtained directly from the source APIs.

### Silver

Cleaned, validated and standardized datasets.

### Gold

Business-ready analytical datasets and aggregated KPIs.

Apache Iceberg is used as the Lakehouse table format.

---

## 🐳 Local Infrastructure

The complete development environment is orchestrated using Docker Compose.

Main services:

| Service | Purpose | Port |
|---|---|---:|
| PostgreSQL | Metadata / relational storage | 5432 |
| Spark Master | Distributed processing | 8080 |
| Spark Worker | Spark worker node | 8081 |
| Trino | Distributed SQL engine | 8082 |
| Airflow | Workflow orchestration | 8083 |
| Superset | Analytics and visualization | 8088 |
| MinIO API | S3-compatible storage | 9000 |
| MinIO Console | Storage administration | 9001 |

The infrastructure can be started with:

```bash
docker compose up -d
```

Deployment instructions are available in:

```text
docs/Deployment/01_local_deployment.md
```

---

## 📂 Project Structure

```text
energy-lakehouse-platform/
│
├── airflow/          # Airflow DAGs, configuration and logs
├── architecture/     # Architecture resources
├── config/           # Project configuration
├── dashboards/       # Superset dashboard resources
├── data/             # Bronze, Silver and Gold local data
├── docker/           # Dockerfiles and service configuration
├── docs/             # Technical documentation
├── ingestion/        # Data ingestion modules
├── notebooks/        # Development and analysis notebooks
├── postgres/         # PostgreSQL initialization
├── processing/       # Data transformation pipelines
├── scripts/          # Utility scripts
├── spark/            # Spark jobs and configuration
├── superset/         # Superset resources
├── tests/            # Automated tests
│
├── .env.example
├── .gitignore
├── docker-compose.yml
├── LICENSE
└── README.md
```

---

## 🚧 Project Status

| Phase | Status |
|---|---|
| Phase 0 – Project Organization | ✅ Completed |
| Phase 1 – Architecture Design | ✅ Completed |
| Phase 2 – Infrastructure | 🟡 Final validation pending |
| Phase 3 – Data Ingestion | ⏳ Pending |
| Phase 4 – Data Processing | ⏳ Pending |
| Phase 5 – Analytics & Visualization | ⏳ Pending |

Phase 2 infrastructure has been successfully deployed locally. A final clean-environment reproducibility test is pending.

---

## 📜 License

This project is licensed under the MIT License.