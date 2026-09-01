# Local Deployment Guide

## 1. Overview

This document describes how to deploy the Energy Lakehouse Platform in a local
environment using Docker Compose.

The platform is based entirely on Open Source technologies and includes the
infrastructure required for:

- API ingestion;
- S3-compatible object storage;
- distributed data processing;
- Apache Iceberg table management;
- workflow orchestration;
- distributed SQL querying;
- analytical visualization.

The main containerized services are:

```text
PostgreSQL
MinIO
Spark Master
Spark Worker
Trino
Airflow Webserver
Airflow Scheduler
Superset
```

Initialization containers are also used for Airflow and Superset.

Apache Iceberg is used as the table format for the structured Lakehouse layers
and does not run as an independent Docker service.

---

## 2. Prerequisites

The following software is required:

```text
Git
Docker Desktop
Docker Compose
```

Recommended local resources:

```text
16 GB RAM
30 GB or more free disk space
```

The actual disk requirement depends on the historical range loaded into Bronze,
Silver and Gold.

Verify the Docker installation:

```bash
docker --version
docker compose version
```

---

## 3. Clone the Repository

Clone the repository and enter the project directory:

```bash
git clone <REPOSITORY_URL>
cd energy-lakehouse-platform
```

The repository root contains the main Docker Compose definition:

```text
docker-compose.yml
```

---

## 4. Environment Configuration

Environment-specific configuration and credentials are externalized from source
code.

The repository provides:

```text
.env.example
```

Create the local `.env` file from the template.

### Windows PowerShell

```powershell
Copy-Item .env.example .env
```

### Linux / macOS

```bash
cp .env.example .env
```

Review and complete the required values before starting the platform.

The environment configuration includes values associated with:

```text
PostgreSQL
MinIO
Airflow
Superset
AEMET
ESIOS
Open-Meteo access where required by the configured service plan
```

Credentials, API keys and passwords must never be embedded directly in
committed source code or documentation.

The real:

```text
.env
```

must never be committed to Git.

---

## 5. Validate the Docker Compose Configuration

Before starting the environment, validate the Compose configuration:

```bash
docker compose config
```

This checks the Compose syntax and resolves the configured environment
variables.

Any configuration error should be corrected before building or starting the
services.

---

## 6. Build the Platform

The project uses custom Docker images where additional project-specific
dependencies are required.

Custom images are used for:

```text
Apache Spark
Apache Airflow
Apache Superset
```

Build the platform with:

```bash
docker compose build
```

A complete rebuild without Docker build cache can be requested with:

```bash
docker compose build --no-cache
```

This is normally unnecessary unless dependencies or image configuration have
changed.

The final Airflow image uses Python 3.10 to remain compatible with the Python
runtime used by the Spark worker during PySpark execution.

---

## 7. Start the Platform

Start the complete environment:

```bash
docker compose up -d
```

Inspect all service states:

```bash
docker compose ps -a
```

The long-running services should remain active.

Initialization services such as:

```text
airflow-init
superset-init
```

are expected to terminate after successful initialization.

A successful initialization state is:

```text
Exited (0)
```

---

## 8. Services and Local Ports

The main host-accessible services are:

| Component | Purpose | Local access |
|---|---|---|
| PostgreSQL | Platform and catalog metadata | `localhost:5432` |
| Spark Master | Spark cluster and monitoring | `http://localhost:8080` |
| Spark Worker | Spark worker monitoring | `http://localhost:8081` |
| Trino | Distributed SQL query engine | `http://localhost:8082` |
| Airflow | Workflow orchestration | `http://localhost:8083` |
| Superset | Analytics and visualization | `http://localhost:8088` |
| MinIO API | S3-compatible object storage | `http://localhost:9000` |
| MinIO Console | Object-storage administration | `http://localhost:9001` |

Spark cluster communication also uses:

```text
spark-master:7077
```

inside the Docker environment.

---

## 9. Docker Network

The platform services communicate through the shared Docker network:

```text
lakehouse-network
```

Containers communicate using Docker service names rather than dynamically
assigned container IP addresses.

Examples include:

```text
postgres:5432
minio:9000
spark-master:7077
```

Host port mappings are used only when a service needs to be accessed from the
local machine.

---

## 10. Local Platform Architecture

The deployed infrastructure supports the following logical data path:

```text
External APIs
      │
      ▼
Python ingestion
      │
      ▼
MinIO / Bronze
      │
      ▼
Apache Spark
      │
      ▼
Apache Iceberg / Silver
      │
      ▼
Apache Spark
      │
      ▼
Apache Iceberg / Gold
      │
      ▼
Trino
      │
      ▼
Apache Superset
```

Apache Airflow coordinates pipeline executions.

PostgreSQL provides service and Iceberg catalog metadata.

---

## 11. Storage Architecture

MinIO acts as the common physical object-storage platform.

However, Bronze and the structured Lakehouse layers use different logical
storage models.

### Bronze

Bronze stores source objects directly in MinIO.

For analytical time-series datasets, the physical temporal hierarchy is governed
by source observation time rather than by ingestion time.

The validated canonical paths include:

```text
Open-Meteo hourly
bronze/open_meteo/weather_hourly/
year=YYYY/month=MM/day=DD/
station_id=<station_id>.json

Open-Meteo 15-minute
bronze/open_meteo/weather_15min/
year=YYYY/month=MM/day=DD/
station_id=<station_id>.json

ESIOS hourly
bronze/esios/<dataset>/
year=YYYY/month=MM/day=DD/
data.json

ESIOS monthly
bronze/esios/<dataset>/
year=YYYY/month=MM/
data.json

AEMET stations
bronze/aemet/stations/stations.json

AEMET current observations
bronze/aemet/current_observations/
year=YYYY/month=MM/day=DD/
observations.json

CNIG provinces
bronze/cnig/provinces/provinces.csv

CNIG municipalities
bronze/cnig/municipalities/municipalities.csv
```

`ingestion_timestamp` remains audit metadata and does not determine the physical
business partition date.

Bronze is not implemented as Apache Iceberg tables.

### Silver

Silver is implemented as Apache Iceberg tables stored in MinIO.

The final physical Silver model contains exactly:

```text
9 tables
```

### Gold

Gold is also implemented as Apache Iceberg tables stored in MinIO.

The final physical Gold model contains exactly:

```text
4 tables
```

---

## 12. Apache Iceberg

Apache Iceberg is the table format used by the structured Silver and Gold
layers.

It does not run as a separate infrastructure container.

The architecture is:

```text
                  PostgreSQL
              Iceberg JDBC catalog
                      │
                      ▼
Apache Spark ──► Apache Iceberg ◄── Trino
                      │
                      ▼
                    MinIO
```

Spark primarily creates and writes tables.

Trino provides interactive SQL access to the persisted tables.

---

## 13. PostgreSQL

PostgreSQL provides relational metadata storage.

Its responsibilities include:

```text
Apache Airflow metadata
Apache Iceberg JDBC catalog metadata
other configured platform metadata
```

The principal analytical data does not reside in PostgreSQL.

It remains stored in MinIO.

---

## 14. Apache Spark

The local Spark deployment contains:

```text
Spark Master
Spark Worker
```

Spark executes the PySpark jobs responsible for:

```text
Bronze → Silver
Silver → Gold
```

The current executable jobs are located under:

```text
spark/jobs/
```

with dedicated Silver and Gold modules.

Spark accesses MinIO and the shared Iceberg catalog using its platform
configuration.

---

## 15. Manual Spark Execution

Spark jobs can be executed manually inside the Spark environment when required
for development or validation.

The Spark executable is available at:

```text
/opt/spark/bin/spark-submit
```

The current container configuration generates the effective Spark configuration
under:

```text
/tmp/spark-conf
```

When a Spark job is launched manually through `docker compose exec`, the
required environment can be provided explicitly.

For example:

```bash
docker compose exec -T spark-master sh -lc \
  'SPARK_CONF_DIR=/tmp/spark-conf PYTHONPATH=/opt/spark/jobs /opt/spark/bin/spark-submit <JOB>'
```

This ensures that the manual execution uses the same Spark configuration
required to access the Lakehouse.

No credentials should be written directly into the command.

---

## 16. Apache Airflow

The Docker environment includes:

```text
Airflow Init
Airflow Webserver
Airflow Scheduler
```

DAG definitions are maintained under:

```text
airflow/dags/
```

Airflow metadata is stored in PostgreSQL.

The final Airflow runtime contains exactly four DAGs:

```text
historical_reload
hourly_ingestion
monthly_ingestion
open_meteo_15min
```

Their validated roles are:

```text
historical_reload
→ historical Bronze → Silver → Gold

hourly_ingestion
→ recurrent hourly Bronze ingestion

monthly_ingestion
→ recurrent monthly Bronze ingestion

open_meteo_15min
→ manual historical Open-Meteo 15-minute Bronze utility
```

Final DAG import validation reported no import errors.

The complete Airflow-controlled historical:

```text
Bronze
→ Silver
→ Gold
```

execution has been validated successfully.

The `historical_reload` workflow exposes exactly:

```text
fecha_inicio
fecha_fin
sobreescribir_datos
eliminar_historial_completo
```

and supports the three validated persistence behaviours:

```text
PRESERVE
RANGE OVERWRITE
FULL DELETE
```

FULL DELETE has priority over RANGE OVERWRITE.

AEMET current observations are deliberately excluded from historical
reconstruction.

---

## 17. Trino

Trino provides SQL access to the Apache Iceberg datasets.

The current Iceberg catalog exposes the structured Lakehouse namespaces,
including:

```text
silver
gold
```

The final validated physical model contains:

```text
9 Silver tables
4 Gold tables
```

The current Gold tables are:

```text
gold_dim_geography
gold_dim_time
gold_fact_installed_capacity_monthly
gold_fact_province_hourly
```

These tables have been queried successfully through Trino.

---

## 18. Apache Superset

Apache Superset provides the visualization infrastructure.

The intended analytical connection is:

```text
Superset
   │
   ▼
 Trino
   │
   ▼
Apache Iceberg Gold
```

The Superset infrastructure service has been deployed as part of the Docker
environment.

Final analytical datasets, charts and dashboards in Superset remain part of the
visualization implementation stage.

The existence of the Superset service must therefore not be confused with
completion of the final dashboard layer.

---

## 19. Persistent Storage

Persistent storage allows the platform to survive normal container shutdowns
and recreation.

### MinIO

MinIO persistence contains:

```text
Bronze source data
Silver Iceberg files
Gold Iceberg files
Iceberg metadata files
```

### PostgreSQL

PostgreSQL persistence contains:

```text
Airflow metadata
Iceberg catalog metadata
platform metadata
```

Normal service shutdown therefore does not require deleting persistent volumes.

---

## 20. Stop the Platform

To stop the platform while preserving persistent Docker volumes:

```bash
docker compose down
```

Start it again with:

```bash
docker compose up -d
```

This is the recommended workflow for normal development.

---

## 21. Complete Environment Reset

A destructive environment reset can be performed with:

```bash
docker compose down -v
```

This command deletes persistent Docker volumes.

It can therefore remove:

```text
MinIO data
PostgreSQL metadata
Lakehouse state
service state
```

depending on the volume configuration.

It must only be used when a complete environment reset is explicitly intended.

After the reset, the infrastructure can be recreated with:

```bash
docker compose up -d
```

A complete clean-volume reproducibility test has **not yet been validated**.

Therefore, a successful destructive reconstruction from an empty persistent
environment must not be claimed until that test has actually been executed and
confirmed.

---

## 22. Logs

Inspect all containers:

```bash
docker compose ps -a
```

Inspect logs for a specific service:

```bash
docker compose logs <service-name>
```

For example:

```bash
docker compose logs trino --tail 100
```

or:

```bash
docker compose logs airflow-scheduler --tail 100
```

Follow logs continuously:

```bash
docker compose logs -f <service-name>
```

---

## 23. Restarting a Service

Restart a service without rebuilding it:

```bash
docker compose restart <service-name>
```

Inspect its state afterwards:

```bash
docker compose ps -a
```

---

## 24. Rebuilding a Service

If a Dockerfile or dependency definition changes, rebuild the corresponding
service:

```bash
docker compose build <service-name>
```

To force a clean rebuild:

```bash
docker compose build --no-cache <service-name>
```

Then recreate the service using the normal Docker Compose workflow.

---

## 25. Validated Infrastructure Status

The following infrastructure components have been successfully deployed and
validated locally:

```text
PostgreSQL
MinIO
Spark Master
Spark Worker
Trino
Airflow Webserver
Airflow Scheduler
Superset
```

Initialization services have also been used successfully as part of the
containerized environment.

The infrastructure has supported the real processing path:

```text
External APIs
      │
      ▼
MinIO / Bronze
      │
      ▼
Apache Spark
      │
      ▼
Silver / Apache Iceberg
      │
      ▼
Apache Spark
      │
      ▼
Gold / Apache Iceberg
      │
      ▼
Trino
```

The complete historical Bronze → Silver → Gold path has also been coordinated
successfully through Airflow.

The current Gold tables have been queried successfully through Trino.

The core Lakehouse infrastructure is therefore operational and validated for the
implemented project scope.

---

## 26. Validated Lakehouse and Airflow Execution

An independent real historical processing execution was completed for:

```text
2026-01-10 → 2026-01-15
```

That execution validated the underlying:

```text
MinIO
→ Spark
→ Iceberg
→ Trino
```

processing path using real data.

The final Silver implementation contained exactly:

```text
9 tables
```

and the final Gold implementation contained exactly:

```text
4 tables
```

Relevant validated Gold counts for that independent execution include:

```text
gold_dim_geography = 71
gold_dim_time = 158
gold_fact_installed_capacity_monthly = 19
gold_fact_province_hourly = 8147
```

The final historical Airflow orchestration was subsequently validated
separately.

Its three persistence behaviours were validated with real data:

```text
PRESERVE
RANGE OVERWRITE
FULL DELETE
```

Validation confirmed that:

- PRESERVE keeps existing active Silver/Gold files unchanged and inserts only
  missing natural keys;
- RANGE OVERWRITE rebuilds only the requested interval while preserving
  outside-range data and existing masters;
- FULL DELETE removes active Bronze, purges the current Silver and Gold tables,
  physically cleans the active Silver/Gold warehouse prefixes and rebuilds
  masters;
- no duplicate natural keys were produced;
- previous-run physical Silver/Gold objects after FULL DELETE were zero.

After the final orchestration changes, the regression suites passed:

```text
tests/ingestion = 84 passed
tests/silver    = 85 passed
tests/gold      = 72 passed
```

---

## 27. Current Deployment Validation Status

The current status is:

```text
Docker Compose configuration
= VALIDATED

Normal local deployment
= VALIDATED

Service startup
= VALIDATED

Normal shutdown and restart
= VALIDATED

MinIO persistence
= VALIDATED

PostgreSQL persistence
= VALIDATED

Spark processing
= VALIDATED

Iceberg Silver / Gold persistence
= VALIDATED

Trino access
= VALIDATED

Airflow infrastructure
= VALIDATED

Airflow DAG import validation
= VALIDATED

Historical Airflow E2E runtime
= VALIDATED

PRESERVE persistence policy
= VALIDATED

RANGE OVERWRITE persistence policy
= VALIDATED

FULL DELETE persistence policy
= VALIDATED

Superset infrastructure
= VALIDATED

Final Superset dashboards
= PENDING IMPLEMENTATION

Destructive clean-volume reconstruction
= NOT VALIDATED
```

The remaining pending deployment-related validation is the complete destructive
reconstruction from empty persistent volumes.

The Superset service is deployed, while the final dashboard layer remains a
separate downstream implementation task.

---

## 28. Shutdown

For normal development shutdown, use:

```bash
docker compose down
```

This preserves persistent Docker volumes and is the recommended way to stop the
local Energy Lakehouse Platform.