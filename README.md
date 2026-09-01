# Energy Lakehouse Platform

Open-source Lakehouse platform for integrating meteorological, geographical and
electricity-system data in Spain.

The project was developed as a Big Data & Data Engineering Master's Final
Project and implements an end-to-end architecture based on:

```text
Bronze → Silver → Gold → Trino → Superset
```

The platform combines data from:

- AEMET OpenData;
- Open-Meteo;
- REE / ESIOS;
- CNIG / IGN.

Its main analytical product integrates meteorological and electricity-generation
information at:

```text
Province × hour
```

A complementary Gold fact represents installed electricity-generation capacity
at:

```text
Autonomous Community × month
```

---

## 1. Architecture

The final platform uses:

```text
Python
Apache Spark 3.5 / PySpark
Apache Iceberg
MinIO
PostgreSQL 17
Trino 483
Apache Airflow
Apache Superset
Docker Compose
```

The implemented processing flow is:

```text
External sources
      │
      ▼
Python ingestion
      │
      ▼
MinIO / Bronze
      │
      ▼
Apache Spark / PySpark
      │
      ▼
Apache Iceberg / Silver
      │
      ▼
Apache Spark / PySpark
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

Apache Airflow coordinates historical and incremental ingestion workflows.

PostgreSQL provides persistent metadata required by platform services and the
Apache Iceberg JDBC catalog.

---

## 2. Data Sources

### AEMET OpenData

Final active scope:

```text
stations
current_observations
```

The validated AEMET station catalogue used by the platform contains:

```text
926 locations
```

AEMET current observations provide recent/current official meteorological data.

They are not used to reconstruct arbitrary historical periods.

---

### Open-Meteo

Final active scope:

```text
weather_hourly
weather_15min
```

Historical Open-Meteo acquisition uses the validated catalogue of:

```text
926 AEMET locations
```

A complete UTC day requires:

```text
weather_hourly
→ 24 timestamps
```

```text
weather_15min
→ 96 timestamps
```

Object existence alone is therefore not considered proof of historical
completeness.

---

### REE / ESIOS

Final active scope:

```text
11 hourly electricity-generation indicators
9 monthly installed-capacity indicators
```

The final analytical scope excludes:

```text
ESIOS 5-minute datasets
electricity demand
electricity market prices
national 5-minute Gold facts
national 15-minute Gold facts
```

The project preserves the strict distinction between:

```text
MW
→ power
```

and:

```text
MWh
→ energy
```

An ESIOS response containing:

```text
values = []
```

is treated as a valid:

```text
NO_DATA
```

response.

Missing observations are never fabricated and are not converted to zero.

---

### CNIG / IGN

CNIG / IGN is the canonical territorial master used by the platform.

The active Bronze masters are:

```text
provinces
municipalities
```

The final geographical model contains:

```text
PROVINCE                 = 52
AUTONOMOUS_COMMUNITY     = 19
COUNTRY                  = 1
PENINSULA                = 1
```

for a final structural total of:

```text
73 members
```

---

## 3. Bronze Layer

Bronze stores source-oriented objects in MinIO.

Temporal Bronze datasets are physically organized by:

```text
observation time
```

and not by:

```text
ingestion_timestamp
```

`ingestion_timestamp` is retained only as technical audit metadata.

Canonical paths include:

### Open-Meteo hourly

```text
bronze/open_meteo/weather_hourly/year=YYYY/month=MM/day=DD/station_id=<id>.json
```

### Open-Meteo 15-minute

```text
bronze/open_meteo/weather_15min/year=YYYY/month=MM/day=DD/station_id=<id>.json
```

### ESIOS hourly

```text
bronze/esios/<dataset>/year=YYYY/month=MM/day=DD/data.json
```

### ESIOS monthly

```text
bronze/esios/<dataset>/year=YYYY/month=MM/data.json
```

### AEMET stations

```text
bronze/aemet/stations/stations.json
```

### AEMET current observations

```text
bronze/aemet/current_observations/year=YYYY/month=MM/day=DD/observations.json
```

### CNIG provinces

```text
bronze/cnig/provinces/provinces.csv
```

### CNIG municipalities

```text
bronze/cnig/municipalities/municipalities.csv
```

Bronze is raw object storage and is not implemented as Apache Iceberg tables.

---

## 4. Silver Layer

The final physical Silver model contains exactly:

```text
9 Apache Iceberg tables
```

### AEMET

```text
silver_aemet_stations
silver_aemet_current_observations
```

### Open-Meteo

```text
silver_open_meteo_hourly
silver_open_meteo_15min
```

### CNIG / IGN

```text
silver_cnig_provinces
silver_cnig_autonomous_communities
silver_cnig_municipalities
```

### REE / ESIOS

```text
silver_esios_energy_hourly
silver_esios_installed_capacity_monthly
```

Silver is responsible for:

- source parsing;
- explicit typing;
- timestamp normalization;
- geographical normalization;
- natural-key deduplication;
- technical data-quality validation;
- Apache Iceberg persistence.

The latest validated complete Silver automated test suite finished with:

```text
85 passed
```

---

## 5. Gold Layer

The final Gold model contains exactly:

```text
4 Apache Iceberg tables
```

```text
gold_fact_province_hourly
gold_fact_installed_capacity_monthly
gold_dim_geography
gold_dim_time
```

### `gold_fact_province_hourly`

Grain:

```text
Province × hour
```

The principal integration rule is:

```text
Meteorology Province × hour
FULL OUTER JOIN
Energy Province × hour
```

using:

```text
(province_code, gold_timestamp)
```

Uniqueness is validated before the join.

Missing source information remains null:

```text
NULL != 0
```

The validated ESIOS temporal rule is:

```text
gold_timestamp =
observation_timestamp + 1 hour
```

configured through:

```text
esios_time_gap_hours = 1
```

---

### `gold_fact_installed_capacity_monthly`

Grain:

```text
Autonomous Community × month
```

Installed capacity remains a power metric and is not artificially distributed
to provinces.

---

### Peninsula scope

The validated Peninsular scope excludes:

```text
07  Illes Balears
35  Las Palmas
38  Santa Cruz de Tenerife
51  Ceuta
52  Melilla
```

There is no dedicated physical Peninsular Gold fact table.

---

## 6. Apache Airflow

The final orchestration layer contains exactly four DAGs:

```text
historical_reload
hourly_ingestion
monthly_ingestion
open_meteo_15min
```

Their roles are:

```text
historical_reload
= historical E2E Bronze → Silver → Gold
```

```text
hourly_ingestion
= incremental hourly Bronze ingestion
```

```text
monthly_ingestion
= incremental monthly Bronze ingestion
```

```text
open_meteo_15min
= manual/historical Open-Meteo 15-minute Bronze utility
```

The hourly and monthly incremental DAGs do not automatically execute Silver or
Gold.

`open_meteo_15min` is not scheduled as a recurrent 15-minute production
pipeline.

The final `historical_reload` parameters are:

```text
fecha_inicio
fecha_fin
sobreescribir_datos
eliminar_historial_completo
```

The validated historical policies are:

```text
PRESERVE
RANGE OVERWRITE
FULL DELETE
```

`FULL DELETE` has priority over `RANGE OVERWRITE`.

The final `historical_reload` workflow has been executed successfully end to
end.

The validated historical Silver/Gold write policy is:

```text
LAKEHOUSE_WRITE_POLICY=insert-only
```

under the historical reconstruction path.

---

## 7. Historical Validation Evidence

A complete real-data historical execution was previously performed for:

```text
2026-01-10 → 2026-01-15
```

That execution is retained as historical evidence.

It predates the final `historical_reload` policy because it included AEMET
`current_observations`, which the final historical workflow now excludes.

Historical row counts from that execution must therefore not be interpreted as
permanent table cardinalities.

The Open-Meteo coverage from that execution matched exactly:

```text
926 × 144
= 133344 hourly rows
```

and:

```text
926 × 576
= 533376 fifteen-minute rows
```

The final structural validation of `gold_dim_geography` was performed later and
established:

```text
52 provinces
19 autonomous communities
1 country
1 peninsula

TOTAL = 73
```

---

## 8. Automated Tests

The latest validated regression results are:

```text
Ingestion
= 84 passed

Silver
= 85 passed

Gold
= 72 passed
```

No failures remained in those validated regression executions.

---

## 9. Docker Infrastructure

Docker and Docker Compose provide the reproducible local environment used by the
platform.

The main infrastructure definition is:

```text
docker-compose.yml
```

The environment includes:

| Service | Purpose |
|---|---|
| PostgreSQL | Platform metadata and Apache Iceberg JDBC catalog |
| MinIO | S3-compatible object storage |
| Spark Master | Apache Spark cluster coordination |
| Spark Worker | Distributed Spark processing |
| Trino | SQL query layer over Apache Iceberg |
| Airflow Webserver | Airflow user interface |
| Airflow Scheduler | Workflow scheduling and execution |
| Superset | Analytical visualization infrastructure |

The main host ports are:

| Service | Port |
|---|---:|
| PostgreSQL | 5432 |
| Spark Master | 7077 |
| Spark Master UI | 8080 |
| Spark Worker UI | 8081 |
| Trino | 8082 |
| Airflow | 8083 |
| Superset | 8088 |
| MinIO API | 9000 |
| MinIO Console | 9001 |

---

## 10. Environment Configuration

Environment-specific configuration and credentials are externalized from source
code.

The repository contains:

```text
.env.example
```

A local deployment uses:

```text
.env
```

The real `.env` file must remain outside version control.

Real API keys, passwords, tokens and other secrets must never be committed to
the repository.

---

## 11. Starting the Platform

Validate the Docker Compose configuration:

```bash
docker compose config
```

Build the custom images:

```bash
docker compose build
```

Start the complete environment:

```bash
docker compose up -d
```

Inspect the containers:

```bash
docker compose ps -a
```

Stop the platform while preserving persistent volumes:

```bash
docker compose down
```

A complete Docker-volume reset can be performed with:

```bash
docker compose down -v
```

This removes persistent Docker volumes and must only be used when a complete
local infrastructure reset is intended.

It is different from the application-level `historical_reload` `FULL DELETE`
policy.

---

## 12. Query and Visualization Layer

The final Gold tables are persisted in Apache Iceberg and queryable through
Trino.

Superset infrastructure is available.

The final Superset datasets, charts, dashboards and visualization validation are
still separate project tasks and must not be considered complete solely because
the Superset service is running.

---

## 13. Repository Documentation

Detailed project documentation is available under:

```text
docs/
```

including:

```text
docs/architecture/
docs/Ingestion/
docs/Silver/
docs/Gold/
docs/Deployment/
```

The repository also contains component-specific documentation under the
corresponding project directories.

---

## 14. Project Status

The validated implementation state is:

```text
Phase 0 — Organization
= COMPLETED

Phase 1 — Architecture
= COMPLETED

Phase 2 — Infrastructure
= COMPLETED AND VALIDATED

Phase 3 — Ingestion
= COMPLETED AND VALIDATED

Phase 4 — Lakehouse Silver / Gold
= COMPLETED AND VALIDATED

Phase 5 — Historical Airflow orchestration
= IMPLEMENTED AND VALIDATED
```

Superset infrastructure is available, while final analytical datasets, charts,
dashboards and visualization validation remain pending.
