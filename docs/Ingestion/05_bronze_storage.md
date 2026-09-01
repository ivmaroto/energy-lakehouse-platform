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

Temporal datasets use deterministic canonical paths organized by source
observation time. Master datasets use fixed canonical paths.

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

The final Bronze storage model organizes time-series datasets by source
**observation time**, not by ingestion time.

`ingestion_timestamp` is retained as technical audit metadata, but it does not
govern the physical temporal hierarchy.

The validated canonical paths are:

### Open-Meteo hourly

```text
bronze/open_meteo/weather_hourly/
year=YYYY/month=MM/day=DD/
station_id=<station_id>.json
```

### Open-Meteo 15-minute

```text
bronze/open_meteo/weather_15min/
year=YYYY/month=MM/day=DD/
station_id=<station_id>.json
```

### ESIOS hourly

```text
bronze/esios/<dataset>/
year=YYYY/month=MM/day=DD/
data.json
```

### ESIOS monthly

```text
bronze/esios/<dataset>/
year=YYYY/month=MM/
data.json
```

### AEMET current observations

```text
bronze/aemet/current_observations/
year=YYYY/month=MM/day=DD/
observations.json
```

For example, observations from January that are acquired in August remain under
the January observation-time path. The August ingestion moment is preserved only
in audit metadata.

Master datasets are not governed by historical observation-date partitions:

```text
bronze/aemet/stations/stations.json
bronze/cnig/provinces/provinces.csv
bronze/cnig/municipalities/municipalities.csv
```

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

`ingestion_timestamp` records when the object entered the platform. It must not
be interpreted as the business observation timestamp or as the physical
partitioning key for historical facts.

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
- source/dataset identification;
- source-specific merge or completeness checks required by canonical Bronze
  persistence.

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

The canonical master path is:

```text
bronze/aemet/stations/stations.json
```

### Current observations

`current_observations` contains recent/current official AEMET meteorological
observations.

These observations retain their actual source timestamps.

They are persisted under the observation-day path:

```text
bronze/aemet/current_observations/
year=YYYY/month=MM/day=DD/
observations.json
```

They are not rewritten to match an arbitrary historical execution interval.

AEMET current observations are deliberately excluded from the final
`historical_reload` workflow and are not used to reconstruct arbitrary
historical periods.

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

The canonical daily paths are:

```text
bronze/open_meteo/weather_hourly/
year=YYYY/month=MM/day=DD/
station_id=<station_id>.json
```

and:

```text
bronze/open_meteo/weather_15min/
year=YYYY/month=MM/day=DD/
station_id=<station_id>.json
```

---

## 10. Open-Meteo Coverage State

Open-Meteo historical acquisition contains additional Bronze-state logic.

The implementation can inspect already persisted canonical daily objects and
determine whether the requested temporal coverage is complete for a given
location and observation day.

The relevant implementation is:

```text
ingestion/open_meteo/bronze_state.py
```

A location/day is not considered complete only because an object exists.

Its temporal coverage must contain the expected timestamps.

This allows the batch process to distinguish between:

```text
complete
incomplete
missing
```

objects.

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

The persistence and recovery semantics are source-aware and operate over the
canonical observation-time objects.

---

## 12. Open-Meteo Temporal Completeness

The final daily completeness rules are:

### Hourly

A complete UTC day contains:

```text
24 timestamps
```

### 15-minute

A complete UTC day contains:

```text
96 timestamps
```

Therefore, object existence alone does not prove that a station/day is complete.

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

These counts are evidence from that specific historical execution and are not
permanent table cardinalities.

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

Hourly datasets use the canonical daily path:

```text
bronze/esios/<dataset>/
year=YYYY/month=MM/day=DD/
data.json
```

Monthly installed-capacity datasets use:

```text
bronze/esios/<dataset>/
year=YYYY/month=MM/
data.json
```

The current physical Bronze scope does not contain an analytical 5-minute ESIOS
family.

---

## 14. ESIOS Empty-Data Protection

A successful ESIOS HTTP response does not necessarily contain observations.

The ingestion implementation validates:

```text
indicator.values
```

A structurally valid response with:

```text
indicator.values = []
```

is treated as valid:

```text
NO_DATA
```

It is not converted into a technical failure and does not create fabricated
observations.

The final behaviour is:

```text
HTTP response
      │
      ▼
Indicator structure
      │
      ▼
Validate indicator.values
      │
      ├── non-empty → persist observations
      └── empty     → valid NO_DATA / no observations persisted
```

This preserves the distinction between:

```text
NO_DATA
```

and:

```text
zero-valued measurement
```

No missing source observation is manufactured as zero.

---

## 15. CNIG / IGN Bronze Storage

CNIG / IGN provides the geographical reference masters.

The current Bronze datasets are:

```text
provinces
municipalities
```

Their canonical paths are:

```text
bronze/cnig/provinces/provinces.csv
bronze/cnig/municipalities/municipalities.csv
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

The final Bronze implementation uses canonical observation-time objects for
active temporal data.

Repeated acquisition therefore does not rely on creating an unlimited sequence
of ingestion-time files for the same historical business period.

Re-execution behaviour remains source-aware.

For example:

```text
Open-Meteo
→ inspect existing canonical daily coverage
→ skip complete station/day objects
→ reacquire incomplete or missing station/day objects
```

Incremental canonical objects can also merge newly acquired observations with
already persisted observations according to the source-specific ingestion
logic.

Historical reconstruction behaviour is additionally governed by the Airflow
policies:

```text
PRESERVE
RANGE OVERWRITE
FULL DELETE
```

Business-level canonicalization and final deduplication remain responsibilities
of Silver.

---

## 17. Duplicate Handling

Bronze storage is designed to avoid uncontrolled physical duplication of the
same canonical observation period.

The same observation-time object path is reused for the corresponding source,
dataset and period.

Where an incremental source supports merging into an existing canonical object,
source-specific natural observation identity is used to avoid repeating the
same business observation inside that object.

Silver still applies its own source-specific natural keys and deduplication
rules to produce the canonical structured tables.

Conceptually:

```text
Canonical Bronze objects
        │
        ▼
    Spark parsing
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

This keeps Bronze source-oriented while preserving Silver as the layer that
guarantees logical dataset consistency.

---

## 18. Error Safety

Technical validation is performed before successful Bronze persistence whenever
possible.

Handled conditions include:

- invalid temporal ranges;
- connection failures;
- request timeouts;
- authentication errors;
- invalid JSON;
- malformed API responses;
- missing expected source structures;
- incomplete Open-Meteo temporal coverage;
- valid ESIOS `NO_DATA` responses;
- MinIO persistence errors.

A malformed or technically failed request must not be represented as a
successfully acquired dataset.

A structurally valid ESIOS response with no observations is not a technical
failure: it is represented as `NO_DATA` and does not create synthetic records.

Existing valid Bronze data remains available for subsequent processing when a
later acquisition fails.

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
- observation-time physical organization;
- controlled prefix deletion;
- downstream Spark access.

The Bronze layer therefore operates as real object storage rather than only as a
local filesystem development abstraction.

---

## 20. Historical Bronze Execution

An independent complete historical Bronze execution was performed using real
source data for:

```text
2026-01-10 → 2026-01-15
```

The execution reported:

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

This remains valid historical execution evidence.

However, that independent execution predates the final `historical_reload`
policy and included an AEMET current-observations acquisition.

AEMET current observations retained their real current timestamps and were not
rewritten as January historical observations.

The final `historical_reload` workflow deliberately excludes AEMET current
observations.

The execution-specific object counts above must therefore not be interpreted as
a permanent Bronze cardinality or as the final Airflow task policy.

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

The final Gold model contains exactly:

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

using a validated `FULL OUTER JOIN` between the meteorological and energy blocks
on:

```text
(province_code, gold_timestamp)
```

after validating uniqueness on both sides.

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
= historical E2E Bronze → Silver → Gold

hourly_ingestion
= recurrent hourly Bronze ingestion

monthly_ingestion
= recurrent monthly Bronze ingestion

open_meteo_15min
= manual/historical Open-Meteo 15-minute Bronze utility
```

The hourly and monthly DAGs do not execute Silver or Gold.

`open_meteo_15min` is not automatically scheduled as a recurrent 15-minute
production flow.

The final `historical_reload` workflow was executed end-to-end successfully
under Airflow control.

Its exact parameters are:

```text
fecha_inicio
fecha_fin
sobreescribir_datos
eliminar_historial_completo
```

and the validated historical policies are:

```text
PRESERVE
RANGE OVERWRITE
FULL DELETE
```

`FULL DELETE` has priority over `RANGE OVERWRITE`.

For AEMET and CNIG masters:

```text
PRESERVE / RANGE OVERWRITE
→ preserve existing masters

FULL DELETE
→ remove masters and rebuild them
```

The validated FULL DELETE flow removes active Bronze, purges the 9 Silver and 4
Gold tables, removes the physical `warehouse/silver` and `warehouse/gold`
content and reconstructs the requested scope.

Physical validation of the final FULL DELETE execution produced:

```text
OLD_PREVIOUS_RUN_OBJECTS = 0
```

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

The repository state must only be described as clean after validating it with
an actual `git status` execution.

---

## 26. Current Bronze-to-Silver Validation

The independent historical Bronze execution described above produced the
following Silver counts:

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

These counts belong to that specific independent validation execution and are
not permanent cardinalities of the Silver tables.

The final regression suites subsequently completed with:

```text
Ingestion
= 84 passed

Silver
= 85 passed

Gold
= 72 passed
```

with no failures in the latest validated regression execution.

---

## 27. End-to-End Validation

The independent Bronze data was successfully consumed through the complete
processing chain:

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

That historical execution produced:

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

These values are execution-specific historical evidence and are not permanent
Gold cardinalities.

In particular, the final `gold_dim_geography` structure was validated later with:

```text
PROVINCE = 52
AUTONOMOUS_COMMUNITY = 19
COUNTRY = 1
PENINSULA = 1
```

for a final structural total of:

```text
73 members
```

The earlier value of 71 rows therefore remains historical execution evidence and
must not be presented as the final structural cardinality.

The final Airflow-controlled historical Bronze → Silver → Gold runtime was also
executed successfully after the orchestration refactor.

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

Observation-time physical organization
= VALIDATED

Canonical Bronze paths
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

Open-Meteo daily temporal coverage checks
= VALIDATED

Open-Meteo resumable acquisition
= VALIDATED

ESIOS values=[] → NO_DATA handling
= VALIDATED

Bronze → Silver processing
= VALIDATED

Silver → Gold processing
= VALIDATED

Gold → Trino querying
= VALIDATED

Complete final Airflow E2E runtime
= VALIDATED

Ingestion regression suite
= 84 PASSED

Silver regression suite
= 85 PASSED

Gold regression suite
= 72 PASSED
```

The Bronze storage layer is therefore implemented, validated and aligned with
the final project scope.
