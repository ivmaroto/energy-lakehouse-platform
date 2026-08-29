# Technical Decisions

## 1. Design Principles

The architecture of the Energy Lakehouse Platform is based on the following
principles.

### Open Source first

The platform is based entirely on Open Source technologies and avoids
dependency on proprietary cloud services or commercial analytical platforms.

### Reproducibility

The complete infrastructure is deployable locally using Docker Compose.

Source code, configuration templates, infrastructure definitions and technical
documentation are maintained in Git so that the environment can be reproduced
on another compatible machine.

### Modularity

Each platform component has a clearly defined responsibility.

The architecture separates:

```text
source ingestion
storage
distributed processing
table management
metadata
workflow orchestration
SQL querying
visualization
```

This reduces coupling between components and allows individual services to
evolve independently.

### Separation of responsibilities

The platform deliberately separates:

```text
Python        -> source ingestion
MinIO        -> object storage
Spark        -> distributed processing
Iceberg      -> managed analytical tables
PostgreSQL   -> platform/catalog metadata
Airflow      -> workflow orchestration
Trino        -> interactive SQL querying
Superset     -> visualization
```

### Source fidelity

The platform must not manufacture temporal or geographical detail that is not
provided by the source.

Examples:

```text
Autonomous Community data
must not be artificially expanded to provinces.

Missing observations
must not be replaced by synthetic values.

A source NULL
must not automatically be interpreted as zero.
```

### Progressive refinement

The Lakehouse follows the Medallion Architecture:

```text
Bronze
   ↓
Silver
   ↓
Gold
```

Each layer has a different responsibility.

### Maintainability

Configuration values that may change independently from source code are
externalized whenever appropriate.

Credentials remain outside Git.

Validated source mappings such as ESIOS indicators and geographical aliases are
stored in dedicated configuration files rather than being duplicated across
processing or orchestration code.

### Scalability

The current deployment is local, but the selected processing, table and object
storage technologies support future execution on larger or distributed
infrastructures without changing the logical Lakehouse architecture.

---

# 2. Python

Python is the primary programming language used for source acquisition and
supporting platform logic.

Within the project, Python is responsible for:

- communicating with external APIs;
- authentication and configuration;
- historical and incremental ingestion;
- temporal-range handling;
- HTTP retries and error management;
- technical source validation;
- Bronze persistence;
- auxiliary orchestration functionality.

The source connectors are implemented independently for:

```text
AEMET
Open-Meteo
REE / ESIOS
CNIG / IGN
```

Shared infrastructure functionality is centralized under the common ingestion
package.

Python also provides the integration point with Apache Spark through PySpark.

---

# 3. Apache Spark

Apache Spark has been selected as the distributed processing engine of the
Lakehouse.

The initial platform is deployed locally, and the data volume does not require a
large distributed cluster. Spark is nevertheless used intentionally because
the project implements a Data Engineering architecture based on distributed
processing concepts and because the same processing model can scale beyond the
local environment.

Within the platform, Apache Spark is responsible for:

- reading Bronze source data from MinIO;
- parsing source payloads;
- validating and typing data;
- temporal normalization;
- natural-key deduplication;
- geographical normalization;
- distributed aggregations;
- Bronze-to-Silver transformations;
- Silver-to-Gold transformations;
- meteorological and energy integration;
- creating and writing Apache Iceberg tables.

Spark is therefore used as a **processing engine**, not as the Business
Intelligence query backend.

---

# 4. PySpark and Spark SQL

PySpark provides the primary programming interface used by the Spark
transformation jobs.

The DataFrame API is used for transformation logic including:

- column normalization;
- joins;
- aggregations;
- deduplication;
- geographical mapping;
- analytical fact construction.

Spark SQL is available as part of the Spark processing layer when an SQL
representation is more appropriate.

The project deliberately does not use Spark SQL as the final interactive query
layer.

The separation is:

```text
Data processing:
PySpark / Spark SQL

Interactive analytics:
Trino
```

This prevents visualization workloads from depending directly on the Spark
processing cluster.

---

# 5. Apache Iceberg

Apache Iceberg has been selected as the table format for the structured
Lakehouse layers.

A critical implementation decision is that Iceberg is used for:

```text
Silver
Gold
```

but **not for the raw Bronze landing layer**.

Bronze data is preserved as source objects in MinIO.

The resulting architecture is:

```text
Raw source payloads
        │
        ▼
MinIO / Bronze objects
        │
        ▼
Apache Spark
        │
        ▼
Apache Iceberg Silver
        │
        ▼
Apache Spark
        │
        ▼
Apache Iceberg Gold
```

Iceberg provides the common managed table layer shared by Spark and Trino.

Relevant capabilities include:

- schema management;
- partition management;
- transactional table writes;
- metadata-based table management;
- interoperability between processing and query engines.

The current physical Lakehouse contains:

```text
9 Silver Iceberg tables
4 Gold Iceberg tables
```

---

# 6. MinIO

MinIO has been selected as the S3-compatible object-storage layer.

It allows the complete Lakehouse to operate locally without requiring
proprietary cloud object storage.

MinIO stores two different types of data.

### Bronze source objects

Raw acquisitions are persisted under the Bronze hierarchy with ingestion
metadata.

Conceptually:

```text
bronze/
└── <source>/
    └── <dataset>/
        └── year=YYYY/
            └── month=MM/
                └── day=DD/
```

### Apache Iceberg storage

MinIO also stores:

- Silver data files;
- Silver Iceberg metadata;
- Gold data files;
- Gold Iceberg metadata.

MinIO therefore provides the shared physical storage layer while Bronze and
Iceberg represent different logical storage models.

---

# 7. PostgreSQL

PostgreSQL has been selected as the relational metadata database used by
platform services.

It does not contain the main analytical datasets.

Its responsibilities include:

- Apache Airflow metadata;
- Apache Iceberg JDBC catalog metadata;
- relational metadata required by platform services where configured.

The analytical datasets remain in MinIO and are managed through Apache Iceberg.

This separation prevents PostgreSQL from becoming a duplicate analytical
storage system.

---

# 8. Apache Airflow

Apache Airflow has been selected as the workflow orchestration platform.

Airflow is responsible for coordinating execution, not implementing analytical
transformations.

Its responsibilities include:

- triggering ingestion processes;
- coordinating task dependencies;
- coordinating Bronze, Silver and Gold stages;
- handling task retries;
- recording workflow execution status;
- exposing operational logs;
- scheduling recurring workloads.

Spark transformations remain implemented in dedicated Spark jobs.

The architectural relationship is therefore:

```text
Airflow
   │
   ├──► Python ingestion
   │
   ├──► Spark Silver jobs
   │
   └──► Spark Gold jobs
```

This keeps orchestration logic separated from transformation logic.

---

# 9. Trino

Trino has been selected as the distributed SQL query engine for analytical
access to the Lakehouse.

Its incorporation establishes a dedicated query layer between Apache Iceberg
and downstream analytical consumers.

The design is:

```text
PROCESSING

Spark
  │
  ▼
Apache Iceberg


QUERYING

Apache Iceberg
      │
      ▼
    Trino
      │
      ▼
   Superset
```

Within the platform, Trino is responsible for:

- querying persisted Apache Iceberg tables;
- providing interactive SQL access;
- inspecting Silver and Gold datasets;
- validating persisted analytical results;
- exposing Gold datasets to Apache Superset.

This decouples Business Intelligence workloads from Spark.

The shared Iceberg catalog allows Spark and Trino to operate over the same
tables while retaining different operational responsibilities.

---

# 10. Apache Superset

Apache Superset has been selected as the Business Intelligence and visualization
platform.

Its role is to consume curated analytical information rather than implement
Lakehouse transformation logic.

Superset accesses analytical data through:

```text
Superset
   │
   ▼
 Trino
   │
   ▼
Apache Iceberg Gold
```

Superset is therefore not connected directly to the Spark processing engine.

The intended analytical functionality includes:

- interactive dashboards;
- time-series visualizations;
- geographical comparisons;
- meteorological and energy analysis;
- filtering and exploratory analysis;
- KPI presentation.

Transformation and business logic should remain in Gold rather than being
duplicated inside dashboard definitions whenever possible.

---

# 11. Docker Compose

Docker Compose has been selected as the local infrastructure deployment
mechanism.

The platform contains containerized services for:

```text
PostgreSQL
MinIO
Spark Master
Spark Worker
Trino
Airflow
Superset
```

Docker Compose provides:

- service definitions;
- service dependencies;
- shared networking;
- persistent volumes;
- environment-variable injection;
- host port mappings;
- custom image builds;
- reproducible local deployment.

Custom images are used where project-specific dependencies are required,
including Spark, Airflow and Superset.

The complete platform can be managed using:

```bash
docker compose up -d
docker compose ps -a
docker compose down
```

---

# 12. CNIG / IGN as Canonical Geography

CNIG / IGN has been selected as the canonical territorial reference for the
platform.

This decision prevents individual source systems from defining incompatible
geographical dimensions.

The geographical source masters contain:

```text
provinces
municipalities
```

The autonomous-community master is derived during Silver processing from the
canonical territorial information.

The validated geographical structure includes:

```text
52 province-level entities
19 autonomous communities
8132 municipalities
```

Official geographical codes are preserved as strings so that leading zeroes are
not lost.

Source geographical information is mapped to CNIG only when the source provides
sufficient information to do so.

Missing geography is never manufactured.

---

# 13. Analytical Geographical Grain

The platform does not enforce a universal geographical grain.

The final analytical grains are determined by the real source capabilities.

## Hourly integrated fact

The principal analytical product is:

```text
Province × hour
```

implemented as:

```text
gold_fact_province_hourly
```

Province is used because both the meteorological preparation and the selected
hourly ESIOS generation data can support that integration level.

Autonomous-community attributes are retained as hierarchical information.

## Installed capacity

Installed capacity remains at:

```text
Autonomous Community × month
```

implemented as:

```text
gold_fact_installed_capacity_monthly
```

Monthly installed capacity is not artificially distributed among provinces.

The general geographical rule is therefore:

```text
Use Province when the validated source supports Province.

Otherwise preserve the real higher-level geography.
```

---

# 14. ESIOS Indicator Selection

The ESIOS connector remains generic, while the selected analytical indicator
catalogue is externalized in:

```text
config/esios_indicators.json
```

The final active ESIOS scope contains:

```text
11 hourly generation indicators
9 monthly installed-capacity indicators
```

The previously evaluated 5-minute ESIOS analytical flow is not part of the
current physical Silver or Gold model.

This reduced scope keeps the implemented analytical model aligned with the
validated final use cases.

---

# 15. Open-Meteo Source Strategy

Open-Meteo is used as the reproducible meteorological source for historical
analysis.

The active physical datasets are:

```text
weather_hourly
weather_15min
```

Different Open-Meteo services are used according to the requested temporal
product.

### Historical hourly data

```text
Archive API
```

### Historical 15-minute data

```text
Historical Forecast API
```

### Current / incremental data

```text
Forecast API
```

This distinction was introduced because the standard Forecast API cannot be
used as a generic replacement for historical 15-minute acquisition.

The AEMET station catalogue supplies the point catalogue and coordinates used
for Open-Meteo acquisition.

The current validated catalogue contains:

```text
926 locations
```

---

# 16. AEMET Source Strategy

AEMET provides the official meteorological reference used by the platform.

The active source scope is:

```text
stations
current_observations
```

The station master acts as the official point catalogue.

Current observations provide recent conventional meteorological information.

They are not treated as an arbitrary historical source.

Historical meteorological coverage used by the main analytical flow is
therefore supplied by Open-Meteo.

---

# 17. Silver Physical Model

The current Silver layer contains exactly 9 physical Apache Iceberg tables.

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

### CNIG

```text
silver_cnig_provinces
silver_cnig_autonomous_communities
silver_cnig_municipalities
```

### ESIOS

```text
silver_esios_energy_hourly
silver_esios_installed_capacity_monthly
```

Silver is responsible for normalized reusable datasets and does not perform the
final cross-domain analytical integration.

---

# 18. Gold Physical Model

The final Gold physical model contains exactly 4 Apache Iceberg tables:

```text
gold_fact_province_hourly
gold_fact_installed_capacity_monthly
gold_dim_geography
gold_dim_time
```

No separate country-level 5-minute or 15-minute Gold facts are part of the
current physical implementation.

---

# 19. Meteorology and Energy Integration

The principal analytical integration occurs in:

```text
gold_fact_province_hourly
```

The meteorological preparation produces:

```text
Province × hour
```

and the selected hourly ESIOS preparation produces:

```text
Province × hour
```

Uniqueness is validated before integration.

The two blocks are joined using a:

```text
FULL OUTER JOIN
```

on:

```text
province_code
gold_timestamp
```

This decision preserves valid observations from either source.

Therefore:

```text
weather available + energy missing
→ keep row, energy metrics remain NULL

energy available + weather missing
→ keep row, weather metrics remain NULL

both available
→ integrate both domains in the same row
```

Missing source information is never converted into artificial zero values.

---

# 20. Processing and Query Separation

One of the most important architectural decisions is the explicit separation
between processing and interactive querying.

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


               QUERYING

 Gold
   │
   ▼
 Trino
   │
   ▼
Superset
```

Advantages of this decision include:

- BI workloads do not depend on Spark execution;
- Spark can remain focused on transformation jobs;
- Trino provides a standard SQL interface;
- Apache Iceberg provides the common table abstraction;
- additional SQL consumers can access the same curated data in the future.

---

# 21. Validated Architecture

The core data-processing architecture has been validated using real source
data through:

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
Spark / Silver
      │
      ▼
Apache Iceberg
      │
      ▼
Spark / Gold
      │
      ▼
Apache Iceberg
      │
      ▼
Trino
```

The validated current physical model contains:

```text
Silver tables = 9
Gold tables   = 4
```

The main Gold fact was validated with:

```text
8147 total Province × hour rows
8100 rows containing meteorological information
6768 rows containing energy information
6721 rows containing both domains
0 duplicate Province × hour keys
```

The installed-capacity fact was validated with:

```text
19 Autonomous Community × month rows
0 duplicate Autonomous Community × month keys
```

These results validate the core architectural decisions from source acquisition
through analytical SQL access.

---

# 22. Overall Architecture Rationale

The selected technology stack provides a complete Open Source Data Engineering
platform with clearly separated responsibilities.

```text
Python
→ API integration and ingestion

MinIO
→ raw and Lakehouse object storage

Apache Spark / PySpark
→ distributed transformations

Apache Iceberg
→ managed Silver and Gold tables

PostgreSQL
→ platform and catalog metadata

Apache Airflow
→ workflow orchestration

Trino
→ analytical SQL access

Apache Superset
→ dashboards and visualization

Docker Compose
→ reproducible local infrastructure
```

The architecture combines raw-data preservation, distributed transformation,
managed analytical tables and an independent SQL consumption layer while
remaining fully deployable on a local environment.

The main design decisions ensure that the platform does not invent missing
source detail, retains source traceability, separates transformation from
interactive querying and provides reusable analytical products at the
geographical and temporal grains actually supported by the validated data.