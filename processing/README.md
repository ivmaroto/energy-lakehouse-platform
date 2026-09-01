# Processing Layer

## 1. Project Objective

The objective of this project is to design and implement an Open Source
Lakehouse platform for the integration, processing, storage and analysis of
public meteorological, geographical and electricity-system data from Spain.

The platform integrates information from:

- AEMET OpenData;
- Open-Meteo;
- REE / ESIOS;
- CNIG / IGN geographical reference data.

The solution supports historical data acquisition and subsequent incremental
updates through an orchestration layer based on Apache Airflow.

The platform is deployed locally using Docker Compose and relies entirely on
Open Source technologies.

Python is used for source ingestion, Apache Spark and PySpark perform
distributed Lakehouse transformations, MinIO provides S3-compatible object
storage, Apache Iceberg manages the structured Silver and Gold tables,
PostgreSQL provides service and catalog metadata, Trino exposes the analytical
tables through SQL, and Apache Superset provides the visualization
infrastructure.

The principal analytical objective is to study relationships between
meteorological conditions and electricity generation at:

```text
Province × hour
```

A complementary analytical product provides installed electricity-generation
capacity at:

```text
Autonomous Community × month
```

The geographical level is therefore not artificially forced to a single grain.
Each analytical product preserves the level that can be supported by the
validated source data.

---

## 2. High-Level Architecture

The platform follows a Lakehouse architecture based on the Medallion pattern:

```text
External Sources
       │
       ▼
Python Ingestion
       │
       ▼
┌─────────────────┐
│     Bronze      │
│ Raw objects     │
│     MinIO       │
└────────┬────────┘
         │
         ▼
   Apache Spark
         │
         ▼
┌─────────────────┐
│     Silver      │
│ Apache Iceberg  │
│     MinIO       │
└────────┬────────┘
         │
         ▼
   Apache Spark
         │
         ▼
┌─────────────────┐
│      Gold       │
│ Apache Iceberg  │
│     MinIO       │
└────────┬────────┘
         │
         ▼
       Trino
         │
         ▼
 Apache Superset
```

The platform separates ingestion, storage, processing, querying and
visualization responsibilities.

### Bronze

Bronze preserves source acquisitions in MinIO with minimal modification and
technical ingestion metadata.

Temporal Bronze datasets are physically organized by observation time.

`ingestion_timestamp` is retained only as technical audit metadata.

### Silver

Silver is generated with Apache Spark and persisted as Apache Iceberg tables.

It provides typed, normalized, deduplicated and geographically harmonized
datasets.

### Gold

Gold is also generated with Apache Spark and persisted as Apache Iceberg
tables.

It contains the analytical products prepared for SQL consumption and
visualization.

### Analytical query layer

Trino provides distributed SQL access to the Apache Iceberg tables.

This separates interactive analytical querying from Spark processing workloads.

### Visualization

Apache Superset connects to Trino rather than querying Spark directly.

Superset infrastructure is available, while the final datasets, charts,
dashboards and visualization validation remain separate pending activities.

---

## 3. Data Sources

### AEMET OpenData

AEMET provides official meteorological information.

The current active scope includes:

```text
stations
current_observations
```

The validated station catalogue contains:

```text
926 locations
```

The station catalogue is also used as the point catalogue for Open-Meteo
acquisition.

AEMET current observations provide recent/current meteorological data and are
not used for arbitrary historical reconstruction.

The final `historical_reload` workflow excludes AEMET
`current_observations`.

### Open-Meteo

Open-Meteo provides reproducible meteorological information for historical and
high-frequency processing.

The current active datasets are:

```text
weather_hourly
weather_15min
```

Historical acquisition uses the validated AEMET catalogue of:

```text
926 locations
```

A complete UTC day requires:

```text
weather_hourly
→ 24 timestamps

weather_15min
→ 96 timestamps
```

The existence of a Bronze object alone is not considered proof of temporal
completeness.

### REE / ESIOS

REE / ESIOS provides the electricity-system information used by the analytical
model.

The final configured scope consists of:

```text
11 hourly electricity-generation indicators
9 monthly installed-capacity indicators
```

The final scope excludes:

```text
ESIOS 5-minute datasets
electricity demand
electricity market prices
national 5-minute Gold facts
national 15-minute Gold facts
```

An ESIOS response containing:

```text
values = []
```

is a valid:

```text
NO_DATA
```

response.

Missing observations are not fabricated and are not converted to zero.

### CNIG / IGN

CNIG / IGN provides the canonical geographical reference used for territorial
normalization.

The source masters include:

```text
provinces
municipalities
```

The Silver layer derives the corresponding autonomous-community master.

The final Gold geographical structure is:

```text
PROVINCE = 52
AUTONOMOUS_COMMUNITY = 19
COUNTRY = 1
PENINSULA = 1

TOTAL = 73
```

---

## 4. Main Components

The platform is composed of the following main components.

### Python

Python is responsible for:

- communication with external APIs;
- authentication and configuration;
- historical and incremental ingestion logic;
- technical validation;
- Bronze persistence;
- auxiliary orchestration functionality.

### Apache Spark / PySpark

Apache Spark is the distributed processing engine of the platform.

It is responsible for:

- reading Bronze source data;
- Bronze-to-Silver transformations;
- data typing and normalization;
- natural-key deduplication;
- geographical normalization;
- distributed aggregations;
- Silver-to-Gold transformations;
- meteorological and energy integration;
- writing Apache Iceberg tables.

Spark SQL can be used internally where appropriate as part of the Spark
processing layer.

### Apache Iceberg

Apache Iceberg provides the managed table format for the structured Lakehouse
layers.

The current implementation uses Iceberg for:

```text
Silver
Gold
```

Bronze remains a raw object-storage landing layer rather than an Iceberg table
layer.

Iceberg allows Spark and Trino to operate over the same managed datasets.

### MinIO

MinIO provides S3-compatible object storage.

It stores:

- raw Bronze objects;
- Apache Iceberg data files;
- Apache Iceberg metadata.

### PostgreSQL

PostgreSQL provides relational metadata storage required by platform services.

Its validated responsibilities include:

- Apache Airflow metadata;
- Apache Iceberg JDBC catalog metadata;
- metadata required by other platform services where configured.

PostgreSQL does not contain the main analytical datasets.

### Apache Airflow

Apache Airflow provides the workflow-orchestration layer.

The final Airflow model contains exactly four DAGs:

```text
historical_reload
hourly_ingestion
monthly_ingestion
open_meteo_15min
```

Their validated roles are:

```text
historical_reload
= historical E2E Bronze → Silver → Gold

hourly_ingestion
= incremental hourly Bronze ingestion

monthly_ingestion
= incremental monthly Bronze ingestion

open_meteo_15min
= manual/historical Open-Meteo 15-minute Bronze utility
```

The hourly and monthly incremental DAGs perform Bronze ingestion only.

They do not automatically execute Silver or Gold.

The `open_meteo_15min` DAG is not scheduled as a recurrent 15-minute
production pipeline.

The final `historical_reload` workflow has been executed successfully end to
end.

Its exact parameters are:

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

Historical Silver/Gold processing uses:

```text
LAKEHOUSE_WRITE_POLICY=insert-only
```

### Trino

Trino provides the interactive distributed SQL query layer.

Its responsibilities include:

- querying Apache Iceberg tables;
- exposing Silver and Gold datasets through SQL;
- validating persisted Lakehouse tables;
- serving as the analytical access layer for Apache Superset.

### Apache Superset

Apache Superset is the Business Intelligence and visualization component.

It consumes curated Gold datasets through Trino.

Its infrastructure is available.

Final Superset datasets, charts, dashboards and visualization validation remain
pending and are not considered complete in this document.

### Docker Compose

Docker Compose deploys and manages the local platform services as a
reproducible environment.

---

## 5. Physical Lakehouse Model

### 5.1 Bronze

Bronze persists raw acquisitions in MinIO.

Temporal Bronze datasets are physically organized by observation time rather
than by ingestion timestamp.

Canonical temporal paths include:

```text
bronze/open_meteo/weather_hourly/year=YYYY/month=MM/day=DD/station_id=<id>.json

bronze/open_meteo/weather_15min/year=YYYY/month=MM/day=DD/station_id=<id>.json

bronze/esios/<dataset>/year=YYYY/month=MM/day=DD/data.json

bronze/esios/<dataset>/year=YYYY/month=MM/data.json

bronze/aemet/current_observations/year=YYYY/month=MM/day=DD/observations.json
```

Master datasets use stable canonical paths:

```text
bronze/aemet/stations/stations.json

bronze/cnig/provinces/provinces.csv

bronze/cnig/municipalities/municipalities.csv
```

Bronze preserves source payloads and ingestion metadata.

---

### 5.2 Silver

The current physical Silver implementation contains exactly:

```text
9 Apache Iceberg tables
```

#### AEMET

```text
silver_aemet_stations
silver_aemet_current_observations
```

#### Open-Meteo

```text
silver_open_meteo_hourly
silver_open_meteo_15min
```

#### CNIG

```text
silver_cnig_provinces
silver_cnig_autonomous_communities
silver_cnig_municipalities
```

#### ESIOS

```text
silver_esios_energy_hourly
silver_esios_installed_capacity_monthly
```

Silver preserves source granularity while applying the normalization required
for downstream analysis.

The latest validated complete Silver regression suite finished with:

```text
85 passed
```

---

### 5.3 Gold

The current physical Gold implementation contains exactly:

```text
4 Apache Iceberg tables
```

```text
gold_fact_province_hourly
gold_fact_installed_capacity_monthly
gold_dim_geography
gold_dim_time
```

#### `gold_fact_province_hourly`

Main analytical grain:

```text
Province × hour
```

It integrates meteorological and electricity-generation information.

Its natural key is:

```text
province_code + gold_timestamp
```

Meteorological and energy blocks are integrated after validating uniqueness on
both sides.

The final integration rule is:

```text
Meteorology Province × hour
FULL OUTER JOIN
Energy Province × hour
```

using:

```text
(province_code, gold_timestamp)
```

A full outer integration is used so that valid observations from either domain
are retained when the corresponding observation from the other domain is not
available.

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

The +1 hour boundary can produce 47 rows belonging to the following day.

This is expected behaviour and not an error.

#### `gold_fact_installed_capacity_monthly`

Analytical grain:

```text
Autonomous Community × month
```

Installed capacity remains at autonomous-community level and is not
artificially disaggregated to provinces.

#### Dimensions

```text
gold_dim_geography
gold_dim_time
```

provide reusable geographical and temporal analytical dimensions.

The final structural validation of `gold_dim_geography` established:

```text
PROVINCE = 52
AUTONOMOUS_COMMUNITY = 19
COUNTRY = 1
PENINSULA = 1

TOTAL = 73
```

The validated Peninsular scope excludes:

```text
07  Illes Balears
35  Las Palmas
38  Santa Cruz de Tenerife
51  Ceuta
52  Melilla
```

There is no dedicated physical Peninsular Gold fact table.

The latest validated complete Gold regression suite finished with:

```text
72 passed
```

---

## 6. End-to-End Data Flow

The implemented data-processing path is:

```text
AEMET ────────────┐
Open-Meteo ───────┤
REE / ESIOS ──────┼──► Python ingestion
CNIG / IGN ───────┘
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

Apache Airflow is the orchestration layer responsible for coordinating the
historical E2E workflow and the final incremental Bronze-ingestion workflows.

---

## 7. Processing and Query Separation

A fundamental architectural decision is the separation between distributed data
processing and interactive analytical querying.

### Spark responsibilities

Apache Spark and PySpark perform:

- validation;
- transformation;
- normalization;
- deduplication;
- aggregation;
- geographical harmonization;
- source integration;
- Iceberg writes.

### Trino responsibilities

Trino performs:

- interactive SQL queries;
- analytical access to Iceberg;
- SQL-based inspection of persisted datasets;
- serving Gold data to downstream analytical consumers.

The architecture therefore follows:

```text
PROCESSING

Bronze
   │
   ▼
 Spark
   │
   ▼
Silver
   │
   ▼
 Spark
   │
   ▼
 Gold


ANALYTICAL CONSUMPTION

Gold
 │
 ▼
Trino
 │
 ▼
Superset
```

This prevents the visualization layer from depending directly on the Spark
processing cluster.

---

## 8. Validated End-to-End Implementation

The Lakehouse processing chain has been validated with real source data.

The validated path is:

```text
External APIs
     ↓
Bronze / MinIO
     ↓
Silver / Spark / Iceberg
     ↓
Gold / Spark / Iceberg
     ↓
Trino
```

An earlier complete historical execution for:

```text
2026-01-10 → 2026-01-15
```

successfully supplied the Lakehouse with real Open-Meteo and ESIOS historical
data together with reference data.

This execution is retained as historical evidence.

It predates the final `historical_reload` policy because it included AEMET
`current_observations`, which the final historical workflow now excludes.

The resulting Silver implementation contained exactly:

```text
9 tables
```

and that concrete historical execution included:

```text
silver_open_meteo_hourly = 133344 rows
silver_open_meteo_15min = 533376 rows
silver_esios_energy_hourly = 38443 rows
silver_esios_installed_capacity_monthly = 123 rows
```

These row counts are execution-specific historical evidence and are not
permanent table cardinalities.

The final Gold implementation contained exactly:

```text
4 tables
```

The earlier E2E execution produced:

```text
gold_fact_province_hourly = 8147 rows
gold_fact_installed_capacity_monthly = 19 rows
gold_dim_geography = 71 rows
gold_dim_time = 158 rows
```

These are also execution-specific historical counts.

They must not be interpreted as permanent Gold cardinalities.

The final structural validation of `gold_dim_geography` was performed later and
established:

```text
52 provinces
19 autonomous communities
1 country
1 peninsula

TOTAL = 73
```

For the principal Gold fact, the earlier historical execution produced:

```text
rows with meteorological information = 8100
rows with energy information = 6768
rows with both domains = 6721
duplicate Province × hour keys = 0
```

The installed-capacity fact was also validated with:

```text
duplicate Autonomous Community × month keys = 0
```

The final `historical_reload` Airflow workflow was subsequently executed
successfully end to end.

This demonstrates that the core Lakehouse architecture is operational from real
source ingestion through Spark/Iceberg processing and Trino SQL access.

---

## 9. Architectural Decisions

The current architecture is based on the following validated decisions:

- The platform uses an Open Source Lakehouse architecture.
- Docker Compose provides the reproducible local infrastructure.
- Python implements API ingestion.
- Bronze preserves raw source acquisitions in MinIO.
- Temporal Bronze datasets are physically organized by observation time.
- `ingestion_timestamp` is audit metadata only.
- Apache Spark / PySpark performs distributed Lakehouse processing.
- Apache Iceberg provides the structured Silver and Gold table format.
- MinIO provides S3-compatible object storage.
- PostgreSQL supports platform metadata and the Iceberg JDBC catalog.
- Apache Airflow provides workflow orchestration.
- Trino provides the interactive analytical SQL layer.
- Apache Superset provides the visualization infrastructure through Trino.
- Spark processing and interactive SQL querying are intentionally separated.
- CNIG / IGN is the canonical territorial reference.
- Province is the principal integration level for the hourly analytical fact.
- Autonomous Community is preserved for monthly installed-capacity analysis.
- Geographical detail is never artificially manufactured when the source does
  not support it.
- The principal integrated analytical grain is `Province × hour`.
- The physical Silver model contains exactly 9 Iceberg tables.
- The physical Gold model contains exactly 4 Iceberg tables.
- Meteorological and energy Province × hour blocks are integrated using a full
  outer strategy after uniqueness validation.
- Missing source observations are not converted to zero.
- ESIOS `values = []` is a valid `NO_DATA` response.
- The final ESIOS Gold temporal rule is
  `gold_timestamp = observation_timestamp + 1 hour`.
- The validated Peninsular scope excludes province codes `07`, `35`, `38`,
  `51` and `52`.
- There is no dedicated physical Peninsular Gold fact table.
- The final Airflow model contains exactly four DAGs.
- `hourly_ingestion` and `monthly_ingestion` perform Bronze ingestion only.
- `open_meteo_15min` is a manual/historical Bronze utility rather than an
  automatically scheduled 15-minute production pipeline.
- `historical_reload` implements the final historical E2E
  Bronze → Silver → Gold flow.
- Historical reload supports `PRESERVE`, `RANGE OVERWRITE` and `FULL DELETE`.
- `FULL DELETE` takes priority over `RANGE OVERWRITE`.
- Historical Silver/Gold processing uses
  `LAKEHOUSE_WRITE_POLICY=insert-only`.
- Source observations are not synthetically created to fill temporal or
  geographical gaps.
