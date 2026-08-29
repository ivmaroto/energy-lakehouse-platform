# Bronze Storage

## 1. Overview

The Bronze layer is the raw persistence layer of the Energy Lakehouse Platform.

Its purpose is to preserve information acquired from external sources before
cleaning, normalization, deduplication, geographical harmonization and
analytical transformations are applied.

The current Bronze layer receives information from four source domains:

- AEMET OpenData;
- Open-Meteo;
- REE / ESIOS;
- CNIG / IGN.

Both historical and recent/current acquisition processes use the same logical
Bronze storage layer.

The production-like storage backend is MinIO, which provides an S3-compatible
object-storage interface.

Bronze is deliberately implemented as raw object storage rather than as Apache
Iceberg tables.

---

## 2. Objectives

The Bronze layer has the following objectives:

- preserve source information with minimal modification;
- separate providers and datasets;
- record ingestion metadata;
- preserve source traceability;
- support historical acquisition;
- support recent/current acquisition;
- provide reusable input for Silver processing;
- allow incomplete acquisitions to be recovered where supported;
- avoid applying analytical business logic during ingestion.

Bronze is not the analytical consumption layer of the platform.

---

## 3. Data Flow

The Bronze persistence flow is:

```text
AEMET ────────────┐
Open-Meteo ───────┤
REE / ESIOS ──────┼──► Python ingestion
CNIG / IGN ───────┘
                         │
                         ▼
                  Technical validation
                         │
                         ▼
                  Common storage layer
                         │
                         ▼
                    MinIO / Bronze
                         │
                         ▼
                    Apache Spark
                         │
                         ▼
                  Silver / Iceberg
```

The common persistence functionality is implemented in:

```text
ingestion/common/storage.py
```

This keeps MinIO persistence separate from source-specific API logic.

---

## 4. Current Bronze Source Scope

The current final source scope is:

```text
bronze/
├── aemet/
│   ├── stations/
│   └── current_observations/
│
├── open_meteo/
│   ├── weather_hourly/
│   └── weather_15min/
│
├── esios/
│   ├── <hourly-generation-dataset>/
│   └── <monthly-installed-capacity-dataset>/
│
└── cnig/
    ├── provinces/
    └── municipalities/
```

The exact object hierarchy below each dataset also includes ingestion-date
partitions.

Datasets evaluated during earlier implementation stages but not retained in the
final physical scope include:

```text
AEMET daily climatology
AEMET radiation
Open-Meteo historical forecast as a separate dataset
ESIOS 5-minute power
electricity demand
```

These datasets must not be treated as part of the final Bronze contract.

---

## 5. Temporal Partitioning

Bronze objects are organized using the ingestion-date hierarchy:

```text
bronze/
└── <source>/
    └── <dataset>/
        └── year=YYYY/
            └── month=MM/
                └── day=DD/
                    └── <object>
```

The:

```text
year
month
day
```

values represent the **ingestion date**.

They do not necessarily represent:

- the observation timestamp;
- the requested historical interval;
- the source publication date.

For example, historical January observations downloaded in August remain stored
under the August ingestion-date path.

The requested source interval is retained separately in Bronze metadata.

This distinction is important because physical storage location and analytical
observation time represent different concepts.

---

## 6. Bronze Object Metadata

JSON Bronze objects use a wrapper containing technical ingestion metadata and
the source payload.

Conceptually:

```json
{
  "metadata": {
    "source": "<source>",
    "dataset": "<dataset>",
    "ingestion_mode": "<mode>",
    "ingestion_timestamp": "<timestamp>",
    "requested_start_date": "<value or null>",
    "requested_end_date": "<value or null>"
  },
  "data": {}
}
```

Additional source-specific metadata can be included where required for
traceability.

Examples include:

```text
AEMET
→ station information

Open-Meteo
→ location_id
→ latitude
→ longitude

ESIOS
→ indicator_id
```

Reference/master datasets may not require the same temporal metadata as
observation datasets.

---

## 7. Source Preservation

Bronze follows a raw-data preservation principle.

The ingestion layer may perform technical operations required for reliable
storage, such as:

- request validation;
- response validation;
- serialization;
- addition of ingestion metadata;
- storage-path generation;
- source/dataset identification.

Bronze does not perform:

- cross-source joins;
- geographical harmonization;
- analytical temporal aggregation;
- business metric calculation;
- unit reinterpretation;
- source fallback;
- KPI calculation;
- Gold analytical integration.

Those operations belong to Silver and Gold.

---

## 8. AEMET Bronze Storage

The final active AEMET Bronze datasets are:

```text
stations
current_observations
```

### Stations

The station catalogue acts as meteorological point master.

The current validated catalogue contains:

```text
926 stations
```

These coordinates are also used by Open-Meteo.

### Current observations

`current_observations` contains recent/current official AEMET meteorological
observations.

These observations retain their actual source timestamps.

They are not rewritten to match an arbitrary historical execution interval.

---

## 9. Open-Meteo Bronze Storage

The final Open-Meteo Bronze datasets are:

```text
weather_hourly
weather_15min
```

Both datasets operate over the validated AEMET point catalogue:

```text
926 locations
```

The source API used depends on the requested temporal product.

### Historical hourly

```text
Open-Meteo Archive API
```

### Historical 15-minute

```text
Open-Meteo Historical Forecast API
```

### Current / recent

```text
Open-Meteo Forecast API
```

The resulting Bronze structure remains the same logical dataset regardless of
the source endpoint used to acquire the observations.

---

## 10. Open-Meteo Coverage State

Open-Meteo historical acquisition contains additional Bronze-state logic.

The implementation can inspect already persisted objects and determine whether
the requested temporal interval is complete for a given location.

The relevant implementation is:

```text
ingestion/open_meteo/bronze_state.py
```

A location is not considered complete only because an object exists.

Its temporal coverage must correspond to the requested interval.

This allows the batch process to distinguish between:

```text
complete
incomplete
missing
```

locations.

---

## 11. Open-Meteo Resumable Acquisition

Large Open-Meteo executions process hundreds of locations.

The batch implementation therefore supports resuming incomplete executions.

Conceptually:

```text
926 requested locations
         │
         ▼
Inspect Bronze state
         │
         ├── complete → skip
         │
         └── incomplete / missing → acquire
```

This prevents an interrupted historical load from unnecessarily repeating all
completed locations.

For this reason, Bronze persistence must not be described as a single universal
append-only rule independent of source behaviour.

The persistence and recovery semantics are source-aware.

---

## 12. Open-Meteo Temporal Completeness

For the validated historical interval:

```text
2026-01-10 → 2026-01-15
```

the expected observations per location were:

### Hourly

```text
6 days × 24
= 144 observations
```

### 15-minute

```text
6 days × 24 × 4
= 576 observations
```

The completed historical Bronze acquisition covered:

```text
926 / 926 hourly locations
926 / 926 15-minute locations
```

The downstream Silver counts confirmed that coverage:

```text
926 × 144
= 133344 hourly rows
```

```text
926 × 576
= 533376 fifteen-minute rows
```

---

## 13. ESIOS Bronze Storage

REE / ESIOS Bronze data is organized by configured dataset.

The final active configuration contains:

```text
11 hourly generation indicators
9 monthly installed-capacity indicators
```

The indicator catalogue is stored in:

```text
config/esios_indicators.json
```

Each acquisition retains its corresponding:

```text
indicator_id
dataset
requested temporal interval
ingestion metadata
source payload
```

The current physical Bronze scope does not contain an analytical 5-minute ESIOS
family.

---

## 14. ESIOS Empty-Data Protection

A successful ESIOS HTTP response is not sufficient for Bronze persistence to be
considered successful.

The ingestion implementation validates:

```text
indicator.values
```

before accepting the dataset.

If:

```text
indicator.values = []
```

the acquisition fails rather than persisting the empty payload as if valid
observations had been obtained.

This avoids confusing:

```text
HTTP success
```

with:

```text
source data available
```

The orchestration policy for legitimate recent-source publication delays is a
separate concern and is not implemented by the Bronze storage layer.

---

## 15. CNIG / IGN Bronze Storage

CNIG / IGN provides the geographical reference masters.

The current Bronze datasets are:

```text
provinces
municipalities
```

These datasets are reference information rather than analytical time series.

They subsequently feed the Silver geographical model:

```text
silver_cnig_provinces
silver_cnig_autonomous_communities
silver_cnig_municipalities
```

CNIG is therefore part of Bronze source storage even though its data lifecycle
differs from meteorological and electricity observations.

---

## 16. Re-execution Behaviour

Bronze preserves source acquisition traceability.

Repeated requests can therefore result in overlapping source observations.

However, re-execution behaviour is not identical for every source.

For example:

```text
Open-Meteo
→ can inspect existing temporal coverage
→ can skip complete locations
→ can resume missing locations
```

Other acquisitions may create new source objects on repeated execution.

Therefore, the architecture does not rely on physical Bronze uniqueness as the
final deduplication mechanism.

Business-level canonicalization occurs in Silver.

---

## 17. Duplicate Handling

Bronze can contain repeated business observations originating from overlapping
or repeated acquisitions.

This is acceptable because Bronze represents source acquisitions rather than the
canonical analytical dataset.

Silver applies source-specific natural keys to produce normalized records.

Conceptually:

```text
Bronze object A ──┐
                  │
Bronze object B ──┼──► Spark parsing
                  │
Bronze object C ──┘
                        │
                        ▼
                 Natural-key logic
                        │
                        ▼
                   Deduplication
                        │
                        ▼
                      Silver
```

This separation allows Bronze to retain acquisition evidence while Silver
maintains logical dataset consistency.

---

## 18. Error Safety

Technical validation is performed before successful Bronze persistence whenever
possible.

Handled failure categories include:

- invalid temporal ranges;
- connection failures;
- request timeouts;
- authentication errors;
- invalid JSON;
- malformed API responses;
- missing expected source structures;
- incomplete Open-Meteo temporal coverage;
- empty ESIOS values;
- MinIO persistence errors.

A failed request must not be represented as a successfully acquired dataset.

Existing valid Bronze data remains available for subsequent processing.

---

## 19. MinIO Storage

MinIO provides the production-like S3-compatible storage backend.

Bronze data is stored under the configured Lakehouse bucket using the:

```text
bronze/
```

prefix.

MinIO has been validated for:

- object writing;
- object enumeration;
- object reading;
- historical Bronze persistence;
- current-source Bronze persistence;
- downstream Spark access.

The Bronze layer therefore operates as real object storage rather than only as a
local filesystem development abstraction.

---

## 20. Historical Bronze Execution

A complete historical Bronze execution was performed using real source data for:

```text
2026-01-10 → 2026-01-15
```

The final execution reported:

```text
BRONZE HISTORICAL LOAD COMPLETED
```

with:

```text
AEMET stations
= 1 object

CNIG masters
= 2 objects

ESIOS hourly
= 11 files

ESIOS monthly
= 9 files

Open-Meteo locations
= 926

Open-Meteo hourly
= 926 files

Open-Meteo 15-minute
= 926 files

AEMET current observations
= 1 file
```

This is the current principal real-data Bronze validation.

---

## 21. Relationship with Silver

Bronze is the direct input to the Silver processing layer.

The transformation flow is:

```text
Bronze
   │
   ▼
Apache Spark
   │
   ├── source parsing
   ├── explicit typing
   ├── timestamp normalization
   ├── geographical normalization
   ├── natural-key deduplication
   └── data-quality validation
   │
   ▼
Apache Iceberg Silver
```

The current final Silver model contains exactly:

```text
9 tables
```

Bronze itself remains raw object storage.

---

## 22. Relationship with Apache Iceberg

Apache Iceberg is not used for Bronze.

The architecture is:

```text
Bronze
Raw objects in MinIO
        │
        ▼
      Spark
        │
        ▼
Silver
Apache Iceberg
        │
        ▼
      Spark
        │
        ▼
Gold
Apache Iceberg
```

This distinction is intentional.

Bronze optimizes for source preservation and reprocessing.

Silver and Gold optimize for structured analytical processing.

---

## 23. Relationship with Gold

Bronze does not directly construct the analytical Gold products.

The complete path is:

```text
Bronze
  │
  ▼
Silver
  │
  ▼
Gold
```

The final Gold model contains:

```text
gold_fact_province_hourly
gold_fact_installed_capacity_monthly
gold_dim_geography
gold_dim_time
```

The principal Gold fact integrates meteorological and electricity-generation
data at:

```text
Province × hour
```

The installed-capacity fact operates at:

```text
Autonomous Community × month
```

These analytical structures are deliberately absent from Bronze.

---

## 24. Relationship with Airflow

Apache Airflow coordinates ingestion executions but does not implement the
storage layer itself.

The relationship is:

```text
Airflow
   │
   ▼
Python ingestion
   │
   ▼
Common storage component
   │
   ▼
MinIO / Bronze
```

Airflow Bronze-ingestion capability has previously been validated.

The final complete Airflow-controlled:

```text
Bronze
→ Silver
→ Gold
```

runtime execution remains part of the orchestration closure.

---

## 25. Version Control

Generated Bronze data is not committed to Git.

Git contains:

- ingestion source code;
- Spark source code;
- Airflow DAG definitions;
- tests;
- configuration templates;
- documentation;
- infrastructure definitions.

Runtime Bronze objects remain in MinIO.

The local:

```text
.env
```

also remains outside source control.

Real credentials must never be committed.

---

## 26. Current Bronze-to-Silver Validation

The validated historical Bronze execution produced the following Silver counts:

```text
silver_aemet_stations
= 926

silver_aemet_current_observations
= 9786

silver_open_meteo_hourly
= 133344

silver_open_meteo_15min
= 533376

silver_cnig_provinces
= 52

silver_cnig_autonomous_communities
= 19

silver_cnig_municipalities
= 8132

silver_esios_energy_hourly
= 38443

silver_esios_installed_capacity_monthly
= 123
```

The exact Open-Meteo counts validate complete historical Bronze coverage for the
requested six-day interval.

---

## 27. End-to-End Validation

The Bronze data was successfully consumed through the complete processing chain:

```text
Real external sources
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

The resulting Gold tables contained:

```text
gold_dim_geography
= 71 rows

gold_dim_time
= 158 rows

gold_fact_installed_capacity_monthly
= 19 rows

gold_fact_province_hourly
= 8147 rows
```

The principal hourly fact contained:

```text
8100 rows with weather
6768 rows with energy
6721 rows with weather and energy
```

and:

```text
0 duplicate Province × hour keys
```

This confirms that Bronze objects persisted in MinIO are valid inputs to the
implemented Lakehouse processing chain.

---

## 28. Current Validation Status

The current Bronze storage status is:

```text
MinIO Bronze backend
= VALIDATED

Source-based organization
= VALIDATED

Dataset-based organization
= VALIDATED

Ingestion-date partitioning
= VALIDATED

Bronze metadata
= VALIDATED

AEMET Bronze persistence
= VALIDATED

Open-Meteo Bronze persistence
= VALIDATED

ESIOS Bronze persistence
= VALIDATED

CNIG Bronze persistence
= VALIDATED

Historical Bronze acquisition
= VALIDATED

Open-Meteo temporal coverage checks
= VALIDATED

Open-Meteo resumable acquisition
= VALIDATED

ESIOS empty-response protection
= VALIDATED

Bronze → Silver processing
= VALIDATED

Silver → Gold processing
= VALIDATED

Gold → Trino querying
= VALIDATED

Complete final Airflow E2E runtime
= PENDING ORCHESTRATION VALIDATION
```

The Bronze storage layer is therefore implemented and operational for the
current project scope.