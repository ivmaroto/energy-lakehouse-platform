# Ingestion Architecture

## 1. Overview

The ingestion layer is responsible for acquiring meteorological, geographical
and electricity-system data from the external public sources used by the
Energy Lakehouse Platform.

The current platform integrates four source domains:

- AEMET OpenData;
- Open-Meteo;
- REE / ESIOS;
- CNIG / IGN.

The ingestion processes are implemented in Python.

Their purpose is to acquire source information reliably, perform technical
validation, register ingestion metadata and persist the result in the Bronze
layer before downstream Lakehouse transformations are applied.

The ingestion layer supports:

```text
historical acquisition
current / incremental acquisition
master-data acquisition
```

depending on the capabilities of each source.

Business transformations, geographical harmonization and cross-source
integration are not performed in this layer.

---

## 2. Ingestion Architecture

The general ingestion architecture is:

```text
AEMET OpenData ──────┐
Open-Meteo ──────────┤
REE / ESIOS ─────────┼──► Python ingestion
CNIG / IGN ──────────┘
                            │
                            ▼
                    Technical validation
                            │
                            ▼
                      MinIO / Bronze
                            │
                            ▼
                       Apache Spark
                            │
                            ▼
                    Apache Iceberg Silver
                            │
                            ▼
                    Apache Iceberg Gold
```

The ingestion layer is intentionally separated from Lakehouse processing.

Its responsibilities are limited to:

1. connecting to the external source;
2. constructing the required requests;
3. authenticating when required;
4. validating request parameters;
5. retrieving the source response;
6. performing technical response validation;
7. generating ingestion metadata;
8. persisting valid acquisitions in Bronze.

Subsequent operations such as:

```text
typing
normalization
deduplication
geographical harmonization
aggregation
cross-source integration
```

belong to the Silver and Gold processing layers.

---

## 3. Source Domains

### 3.1 AEMET OpenData

AEMET provides official meteorological information for Spain.

The final active AEMET ingestion scope is:

```text
stations
current_observations
```

Authentication requires:

```text
AEMET_API_KEY
```

The station catalogue acts as the official meteorological point catalogue used
by the platform.

The validated catalogue used by historical Open-Meteo acquisition contains:

```text
926 stations
```

These station coordinates are also used as the acquisition locations for
Open-Meteo.

`current_observations` provides recent meteorological observations.

It is not treated as a source capable of reconstructing arbitrary historical
periods.

The final `historical_reload` workflow therefore excludes AEMET current
observations from historical reconstruction.

---

### 3.2 Open-Meteo

Open-Meteo provides the historical and high-frequency meteorological
information required by the analytical pipeline.

The final active Open-Meteo datasets are:

```text
weather_hourly
weather_15min
```

The source endpoint depends on the requested temporal product.

#### Historical hourly data

```text
Archive API
```

#### Historical 15-minute data

```text
Historical Forecast API
```

#### Current / incremental data

```text
Forecast API
```

Historical acquisition operates over the validated AEMET point catalogue:

```text
926 locations
```

Runtime source-access configuration for the configured Open-Meteo service plan
is externalized from source code and credentials must not be committed to Git.

---

### 3.3 REE / ESIOS

REE / ESIOS provides the electricity-system information used by the final
analytical model.

Authentication requires:

```text
ESIOS_API_KEY
```

The final active configuration contains:

```text
11 hourly electricity-generation indicators
9 monthly installed-capacity indicators
```

The indicator catalogue is externalized in:

```text
config/esios_indicators.json
```

The connector implementation remains generic while the configuration determines
which validated indicators belong to the project scope.

The current scope does not include the previously evaluated 5-minute ESIOS
dataset family.

Electricity demand and electricity market prices are also outside the final
implemented analytical scope.

A structurally valid ESIOS response with:

```text
values = []
```

is treated as valid:

```text
NO_DATA
```

rather than as a failed request.

No synthetic observation is created from that response.

---

### 3.4 CNIG / IGN

CNIG / IGN provides the canonical territorial reference used by the platform.

The current geographical source masters are:

```text
provinces
municipalities
```

The information is subsequently normalized in Silver to produce the canonical
geographical dimensions used throughout the Lakehouse.

Validated downstream cardinalities are:

```text
52 province-level entities
19 autonomous communities
8132 municipalities
```

Autonomous communities are derived during Silver processing from the canonical
territorial information.

---

## 4. Ingestion Modes

The ingestion architecture supports different execution patterns according to
the characteristics of each source.

### 4.1 Historical ingestion

Historical ingestion receives an explicit temporal interval:

```text
start_date
end_date
```

at source-ingestion level.

The final Airflow historical interface exposes the runtime interval as:

```text
fecha_inicio
fecha_fin
```

The principal historical observation datasets are:

```text
Open-Meteo hourly
Open-Meteo 15-minute
ESIOS hourly
ESIOS monthly
```

AEMET current observations are deliberately excluded from arbitrary historical
reconstruction.

Large historical intervals can be split into smaller source-specific request
windows.

---

### 4.2 Current / incremental ingestion

Current or incremental ingestion retrieves newly available information without
requiring a complete historical reload.

The exact strategy differs between providers because publication frequency,
latency and API capabilities are source-specific.

The current Airflow runtime includes:

```text
hourly_ingestion
→ recurrent hourly Bronze ingestion

monthly_ingestion
→ recurrent monthly Bronze ingestion

open_meteo_15min
→ manual historical Open-Meteo 15-minute Bronze utility
```

A requested ending timestamp does not imply that every external provider
already has observations available up to that exact instant.

The ingestion layer therefore preserves real source availability rather than
creating synthetic observations.

---

### 4.3 Master-data ingestion

Some datasets are reference masters rather than time-series observations.

Examples include:

```text
AEMET stations
CNIG provinces
CNIG municipalities
```

Within the final historical reload logic, masters follow ensure-style
semantics:

```text
master exists
→ preserve it

master missing
→ ingest it
```

Therefore PRESERVE and RANGE OVERWRITE keep existing masters, while FULL DELETE
rebuilds them after the active Bronze layer is removed.

---

## 5. Project Structure

The ingestion implementation is separated into source-specific packages and
shared infrastructure.

Relevant project areas include:

```text
config/
└── esios_indicators.json

ingestion/
│
├── common/
│   ├── config.py
│   ├── date_utils.py
│   ├── esios_config.py
│   ├── exceptions.py
│   ├── http_client.py
│   ├── logger.py
│   └── storage.py
│
├── aemet/
│   ├── client.py
│   └── ingest.py
│
├── open_meteo/
│   ├── client.py
│   ├── batch.py
│   └── ingest.py
│
├── esios/
│   ├── client.py
│   └── ingest.py
│
├── orchestration/
│   └── historical_reload.py
│
└── run_ingestion.py
```

The structure keeps API-specific code isolated while allowing common
configuration, HTTP, temporal and storage functionality to be reused.

---

## 6. Common Components

### `config.py`

Provides shared configuration including:

- API URLs;
- HTTP timeouts;
- retry configuration;
- Open-Meteo pacing and backoff;
- historical chunk configuration;
- MinIO configuration;
- environment-variable references.

### `date_utils.py`

Provides shared temporal functionality including:

- date validation;
- temporal-range handling;
- source request-window preparation.

### `esios_config.py`

Loads the selected ESIOS indicator catalogue from:

```text
config/esios_indicators.json
```

This separates indicator selection from connector and DAG source code.

### `http_client.py`

Provides reusable HTTP functionality including:

- sessions;
- timeouts;
- retries;
- temporary HTTP-error handling;
- authentication-error handling;
- JSON response processing.

### `logger.py`

Provides common logging configuration for ingestion components.

### `exceptions.py`

Defines ingestion-specific exceptions for conditions such as:

- invalid configuration;
- invalid temporal ranges;
- authentication failure;
- request failure;
- invalid API response;
- storage failure.

### `storage.py`

Provides the Bronze persistence abstraction backed by:

```text
MinIO
```

The validated storage helper supports:

```text
save_bytes
save_json
object_exists
read_json
delete_prefix
delete_warehouse_layer
```

Deletion is guarded by validated prefixes.

`delete_warehouse_layer` is restricted to:

```text
warehouse/silver/
warehouse/gold/
```

and is used only by the FULL historical reset workflow.

Storage logic remains separated from source-specific connectors.

---

## 7. Open-Meteo Batch Architecture

Open-Meteo historical acquisition operates over the validated AEMET station
catalogue.

The historical catalogue used by the final implementation contains:

```text
926 locations
```

Requesting hundreds of locations requires additional operational controls.

The batch implementation therefore includes:

- source-specific retries;
- exponential backoff;
- configurable pacing;
- historical request splitting;
- daily temporal-completeness validation;
- resumable processing of incomplete daily objects.

The principal batch implementation is:

```text
ingestion/open_meteo/batch.py
```

Completeness is determined from the contents of each canonical daily Bronze
object rather than from object existence alone.

---

## 8. Open-Meteo Coverage Validation

Historical Open-Meteo data is not considered complete merely because a Bronze
object exists.

The final storage model uses canonical daily objects per station.

For a complete UTC day:

```text
hourly
→ 24 timestamps

15-minute
→ 96 timestamps
```

A partial object is therefore incomplete and must be reloaded or completed.

For the earlier independent six-day historical validation interval:

```text
2026-01-10 → 2026-01-15
```

the expected totals per location were:

```text
6 × 24 = 144 hourly observations

6 × 24 × 4 = 576 fifteen-minute observations
```

Those totals remain useful evidence from that historical execution, while the
current implementation validates completeness at canonical daily-object level.

---

## 9. ESIOS Indicator Configuration

The active ESIOS indicator catalogue is externalized in:

```text
config/esios_indicators.json
```

The final configuration is divided into two analytical families:

```text
hourly
monthly
```

### Hourly generation

The current configuration contains 11 indicators.

| Indicator ID | Dataset |
|---:|---|
| 1159 | `generacion_medida_eolica_terrestre` |
| 1161 | `generacion_medida_solar_fotovoltaica` |
| 1162 | `generacion_medida_solar_termica` |
| 10035 | `generacion_medida_hidraulica` |
| 1153 | `generacion_medida_nuclear` |
| 1156 | `generacion_medida_ciclo_combinado` |
| 1158 | `generacion_medida_gas_natural_turbina_vapor` |
| 1164 | `generacion_medida_gas_natural_cogeneracion` |
| 10036 | `generacion_medida_carbon` |
| 10041 | `generacion_medida_otras_renovables` |
| 10043 | `generacion_medida_total` |

### Monthly installed capacity

The current configuration contains 9 indicators.

| Indicator ID | Dataset |
|---:|---|
| 1475 | `potencia_instalada_hidraulica` |
| 1485 | `potencia_instalada_eolica` |
| 1486 | `potencia_instalada_solar_fotovoltaica` |
| 1487 | `potencia_instalada_solar_termica` |
| 10302 | `potencia_instalada_total_renovable` |
| 1477 | `potencia_instalada_nuclear` |
| 1478 | `potencia_instalada_carbon` |
| 1483 | `potencia_instalada_ciclo_combinado` |
| 1488 | `potencia_instalada_otras_renovables` |

No ESIOS 5-minute family belongs to the current final scope.

---

## 10. Bronze Layer

The output of ingestion is persisted in the Bronze layer in MinIO.

For analytical time-series datasets, the physical hierarchy is governed by
source observation time rather than by ingestion time.

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

`ingestion_timestamp` remains audit metadata.

It does not determine the physical business partition date.

Bronze stores data from:

```text
AEMET
Open-Meteo
ESIOS
CNIG
```

Bronze is not implemented as Apache Iceberg tables.

---

## 11. Bronze Metadata

Bronze objects contain technical ingestion metadata alongside the source
payload where applicable.

Typical metadata includes fields such as:

```text
source
dataset
ingestion_mode
ingestion_timestamp
requested_start_date
requested_end_date
```

depending on the source object type.

Source-specific traceability fields may also be included.

Examples include:

```text
AEMET station identifiers

Open-Meteo location identifiers and coordinates

ESIOS indicator identifiers
```

`ingestion_timestamp` is retained for audit and traceability.

Observation time remains the governing temporal value for analytical Bronze
partitioning.

---

## 12. Bronze Design Principles

Bronze follows the following principles.

### Source preservation

The original source representation is retained as closely as possible.

### Minimal transformation

Only technical modifications necessary for acquisition and persistence are
performed.

### Traceability

Each acquisition includes metadata that identifies its origin and execution
context where applicable.

### Reprocessability

Persisted Bronze data can be processed again if Silver or Gold transformation
logic changes.

### Observation-time storage

Analytical time-series objects are stored according to source observation time.

`ingestion_timestamp` remains audit metadata and does not govern the business
partition.

### Separation from analytical logic

No business-level joins or aggregations are performed during ingestion.

---

## 13. Configuration and Credentials

Credentials and environment-specific settings are loaded through environment
variables.

The repository provides:

```text
.env.example
```

The local environment uses:

```text
.env
```

which must remain outside version control.

Validated source credentials include:

```text
AEMET_API_KEY
ESIOS_API_KEY
```

Open-Meteo runtime access configuration for the configured service plan is also
externalized and must not be hardcoded or documented with real secret values.

Relevant MinIO configuration includes values for:

```text
MINIO_ENDPOINT
MINIO_ROOT_USER
MINIO_ROOT_PASSWORD
MINIO_BUCKET
MINIO_SECURE
```

No real credential should be hardcoded in source code or documentation.

---

## 14. Technical Validation

The ingestion layer performs technical validation before treating a source
request as successfully processed.

Validation includes, depending on the provider:

- HTTP status;
- authentication;
- valid JSON structure;
- expected dataset structure;
- requested-date validation;
- coordinate validation;
- expected temporal structure;
- storage success.

For Open-Meteo historical data, daily object completeness is validated against
the expected 24 hourly or 96 fifteen-minute timestamps.

For ESIOS, a structurally valid empty observation list is handled as valid
`NO_DATA`.

More advanced normalization and business validation belong to Silver and Gold.

---

## 15. ESIOS Empty-Data Validation

A successful HTTP response can legitimately contain no source observations for
the requested indicator and interval.

The final ingestion implementation distinguishes:

```text
valid response with values
→ persist/process available observations
```

from:

```text
valid response with values = []
→ NO_DATA
```

A valid empty response is not treated as an ingestion failure.

It also does not create:

```text
synthetic zero values
synthetic timestamps
synthetic source observations
```

This keeps source absence distinct from a published numerical zero.

---

## 16. Error Handling

The ingestion architecture handles conditions including:

- connection failures;
- request timeouts;
- temporary HTTP errors;
- authentication errors;
- malformed responses;
- invalid date ranges;
- incomplete Open-Meteo daily temporal coverage;
- storage failures.

A structurally valid ESIOS response with:

```text
values = []
```

is not an error and is handled as valid:

```text
NO_DATA
```

Temporary failures can be retried by the common HTTP layer.

Open-Meteo additionally implements batch-specific retry and backoff behaviour.

Apache Airflow provides a further task-level orchestration layer for recurring
and historical executions.

---

## 17. Separation of Responsibilities

The complete responsibility chain is:

```text
External Sources
       │
       ▼
Python ingestion
       │
       ├── request
       ├── authentication
       ├── technical validation
       └── Bronze persistence
       │
       ▼
MinIO / Bronze
       │
       ▼
Apache Spark / Silver
       │
       ├── typing
       ├── normalization
       ├── deduplication
       └── geographical harmonization
       │
       ▼
Apache Spark / Gold
       │
       ├── aggregation
       ├── source fallback
       └── analytical integration
       │
       ▼
Trino
```

This separation prevents connector code from becoming coupled to the analytical
model.

---

## 18. Relationship with Airflow

Apache Airflow coordinates ingestion executions but does not implement source
connector logic.

The current runtime contains exactly four DAGs:

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

The final `historical_reload` runtime parameters are:

```text
fecha_inicio
fecha_fin
sobreescribir_datos
eliminar_historial_completo
```

The validated persistence behaviours are:

```text
PRESERVE
RANGE OVERWRITE
FULL DELETE
```

FULL DELETE has priority over RANGE OVERWRITE.

The complete historical:

```text
Bronze
→ Silver
→ Gold
```

runtime has been executed successfully under direct Airflow control.

The historical Silver and Gold write stages use:

```text
LAKEHOUSE_WRITE_POLICY=insert-only
```

so PRESERVE can add missing natural keys without rewriting existing active
records.

The secondary hourly and monthly DAGs remain Bronze-only ingestion workflows.

---

## 19. Development and Testing

The Python ingestion components remain independently executable and testable
outside the complete Airflow workflow.

This allows:

- connector unit testing;
- source-specific integration tests;
- MinIO persistence testing;
- historical batch validation;
- source API validation;
- debugging without modifying DAG definitions.

The latest validated ingestion regression suite completed successfully with:

```text
84 passed
```

The complete regression status after the final orchestration and persistence
changes was:

```text
tests/ingestion = 84 passed
tests/silver    = 85 passed
tests/gold      = 72 passed
```

---

## 20. Historical Validation Evidence

An independent historical Bronze execution was validated using real source data
for:

```text
2026-01-10 → 2026-01-15
```

That earlier execution produced the complete source set used by the then-current
Silver and Gold validation.

It included:

```text
AEMET station master
CNIG masters
ESIOS hourly indicators
ESIOS monthly indicators
Open-Meteo hourly coverage
Open-Meteo 15-minute coverage
AEMET current observations
```

and reported:

```text
BRONZE HISTORICAL LOAD COMPLETED
```

This remains valid evidence of real API → Bronze acquisition.

However, it predates two final ingestion-policy changes:

```text
1. analytical Bronze time-series data is now stored in canonical
   observation-time partitions;

2. final historical_reload excludes AEMET current observations.
```

The same real-source data path was subsequently validated through:

```text
Bronze
→ Silver
→ Gold
→ Trino
```

and the final historical Bronze → Silver → Gold workflow was validated under
Airflow control.

---

## 21. Current Ingestion Scope

The final current source scope is:

```text
AEMET
├── stations
└── current_observations

Open-Meteo
├── weather_hourly
└── weather_15min

REE / ESIOS
├── 11 hourly generation indicators
└── 9 monthly installed-capacity indicators

CNIG / IGN
├── provinces
└── municipalities
```

Previously evaluated datasets such as:

```text
AEMET daily climatology
AEMET radiation ingestion
ESIOS 5-minute power
```

are not part of the final physical ingestion scope.

---

## 22. Current Status

The ingestion architecture is implemented and validated for the current project
scope.

Validated elements include:

```text
AEMET source acquisition
Open-Meteo source acquisition
ESIOS source acquisition
CNIG master acquisition

historical Bronze ingestion
current AEMET acquisition
MinIO persistence

observation-time Bronze partitioning
Open-Meteo hourly daily completeness validation
Open-Meteo 15-minute daily completeness validation
Open-Meteo resumable historical processing

ESIOS valid empty response
= NO_DATA

API → Bronze
Bronze → Silver
Silver → Gold
Gold → Trino

Airflow historical Bronze → Silver → Gold
PRESERVE
RANGE OVERWRITE
FULL DELETE
```

The latest validated automated suites are:

```text
ingestion = 84 passed
silver    = 85 passed
gold      = 72 passed
```

The ingestion architecture therefore no longer has a pending orchestration
closure for the final historical workflow.

Final Superset dashboard implementation remains outside the ingestion layer.
