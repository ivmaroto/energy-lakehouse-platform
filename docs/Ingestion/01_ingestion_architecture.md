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

The current validated catalogue contains:

```text
926 stations
```

These station coordinates are also used as the acquisition locations for
Open-Meteo.

`current_observations` provides recent meteorological observations.

It is not treated as a source capable of reconstructing arbitrary historical
periods.

---

### 3.2 Open-Meteo

Open-Meteo provides the historical and high-frequency meteorological
information required by the analytical pipeline.

No API key is required for the access pattern used by the project.

The final active Open-Meteo datasets are:

```text
weather_hourly
weather_15min
```

The source endpoint depends on the requested temporal product.

#### Historical hourly data

```text
https://archive-api.open-meteo.com/v1/archive
```

#### Historical 15-minute data

```text
https://historical-forecast-api.open-meteo.com/v1/forecast
```

#### Current / incremental data

```text
https://api.open-meteo.com/v1/forecast
```

Historical acquisition operates over the complete AEMET location catalogue:

```text
926 locations
```

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

and retrieves available source information for that period.

The principal historical observation datasets are:

```text
Open-Meteo hourly
Open-Meteo 15-minute
ESIOS hourly
ESIOS monthly
```

Large historical intervals can be split into smaller source-specific request
windows.

---

### 4.2 Current / incremental ingestion

Current or incremental ingestion retrieves newly available information without
requiring a complete historical reload.

The exact strategy differs between providers because publication frequency,
latency and API capabilities are source-specific.

The general model is:

```text
Requested temporal window
          │
          ▼
       Source API
          │
          ▼
    Available records
          │
          ▼
        Bronze
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

These datasets are used by downstream processing to normalize and enrich the
time-series sources.

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
│   ├── bronze_state.py
│   └── ingest.py
│
├── esios/
│   ├── client.py
│   └── ingest.py
│
├── orchestration/
│   └── ...
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

---

### `date_utils.py`

Provides shared temporal functionality including:

- date validation;
- temporal-range handling;
- source request-window preparation.

---

### `esios_config.py`

Loads the selected ESIOS indicator catalogue from:

```text
config/esios_indicators.json
```

This separates indicator selection from connector and DAG source code.

---

### `http_client.py`

Provides reusable HTTP functionality including:

- sessions;
- timeouts;
- retries;
- temporary HTTP-error handling;
- authentication-error handling;
- JSON response processing.

---

### `logger.py`

Provides common logging configuration for ingestion components.

---

### `exceptions.py`

Defines ingestion-specific exceptions for conditions such as:

- invalid configuration;
- invalid temporal ranges;
- authentication failure;
- request failure;
- invalid API response;
- storage failure.

---

### `storage.py`

Provides the Bronze persistence abstraction.

The production-like backend used by the current platform is:

```text
MinIO
```

Storage logic remains separated from source-specific connectors.

---

## 7. Open-Meteo Batch Architecture

Open-Meteo historical acquisition operates over the complete AEMET station
catalogue.

The validated catalogue contains:

```text
926 locations
```

Requesting hundreds of locations requires additional operational controls.

The batch implementation therefore includes:

- source-specific retries;
- exponential backoff;
- configurable pacing;
- historical request splitting;
- temporal coverage validation;
- detection of already completed locations;
- resumable processing of incomplete batches.

The principal batch implementation is:

```text
ingestion/open_meteo/batch.py
```

Bronze completeness inspection is implemented in:

```text
ingestion/open_meteo/bronze_state.py
```

---

## 8. Open-Meteo Coverage Validation

Historical Open-Meteo data is not considered complete merely because a Bronze
object exists.

The requested temporal coverage must also be present.

For example, for the six-day validated period:

```text
2026-01-10 → 2026-01-15
```

each hourly location must contain:

```text
6 × 24 = 144 observations
```

and each 15-minute location must contain:

```text
6 × 24 × 4 = 576 observations
```

This allows an interrupted historical acquisition to distinguish:

```text
complete location
incomplete location
missing location
```

and resume only the required work.

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

The general organization is:

```text
bronze/
└── <source>/
    └── <dataset>/
        └── year=YYYY/
            └── month=MM/
                └── day=DD/
                    └── <object>
```

The physical date hierarchy represents the ingestion date.

The requested observation interval is stored separately in metadata.

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
payload.

Typical metadata includes:

```text
source
dataset
ingestion_mode
ingestion_timestamp
requested_start_date
requested_end_date
```

Source-specific traceability fields may also be included.

Examples include:

```text
AEMET station identifiers

Open-Meteo location identifiers and coordinates

ESIOS indicator identifiers
```

This information allows downstream processing to understand the origin and
requested scope of each acquisition.

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
context.

### Reprocessability

Persisted Bronze data can be processed again if Silver or Gold transformation
logic changes.

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

Relevant source credentials include:

```text
AEMET_API_KEY
ESIOS_API_KEY
```

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
request as successful.

Validation includes, depending on the provider:

- HTTP status;
- authentication;
- valid JSON structure;
- expected dataset structure;
- requested-date validation;
- coordinate validation;
- expected source data presence;
- expected temporal coverage.

More advanced normalization and business validation belong to Silver and Gold.

---

## 15. ESIOS Empty-Data Validation

An ESIOS HTTP response is not considered a successful dataset acquisition
solely because the request returned successfully.

The current ingestion implementation validates:

```text
indicator.values
```

before successful Bronze persistence.

If:

```text
indicator.values = []
```

the corresponding ingestion attempt fails rather than persisting the empty
payload as a valid completed dataset.

This prevents HTTP-level success from being confused with actual data
availability.

The behaviour of recent-data orchestration when an upstream source legitimately
has no data available for a requested interval belongs to the orchestration
layer and must not be inferred from this ingestion-level validation.

---

## 16. Error Handling

The ingestion architecture handles conditions including:

- connection failures;
- request timeouts;
- temporary HTTP errors;
- authentication errors;
- malformed responses;
- invalid date ranges;
- incomplete temporal coverage;
- empty ESIOS indicator values;
- storage failures.

Temporary failures can be retried by the common HTTP layer.

Open-Meteo additionally implements batch-specific retry and backoff behaviour.

Apache Airflow provides a further task-level orchestration layer for scheduled
executions.

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

Conceptually:

```text
Airflow DAG
    │
    ▼
Python ingestion
    │
    ▼
Bronze
```

Airflow can subsequently coordinate the Spark Silver and Gold stages.

The project contains existing source-ingestion DAGs and an implemented
historical reload workflow.

The final complete Airflow-controlled:

```text
Bronze
→ Silver
→ Gold
```

runtime execution remains part of the orchestration closure and should not be
described as fully validated until that execution is completed.

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
68 passed
```

---

## 20. Real Historical Validation

A complete historical Bronze execution has been validated using real source
data for:

```text
2026-01-10 → 2026-01-15
```

The execution completed with:

```text
AEMET station master      = 1 file
CNIG masters              = 2 files

ESIOS hourly              = 11 files
ESIOS monthly             = 9 files

Open-Meteo locations      = 926
Open-Meteo hourly files   = 926
Open-Meteo 15-minute files = 926

AEMET current observations = 1 file
```

The result reported:

```text
BRONZE HISTORICAL LOAD COMPLETED
```

The same Bronze data subsequently produced valid Silver and Gold data through
Apache Spark.

This validates the ingestion layer as part of the real processing chain:

```text
External sources
      │
      ▼
Bronze / MinIO
      │
      ▼
Silver / Iceberg
      │
      ▼
Gold / Iceberg
      │
      ▼
Trino
```

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

Open-Meteo hourly historical acquisition
Open-Meteo 15-minute historical acquisition
Open-Meteo resumable batch processing

ESIOS non-empty response validation

API → Bronze
Bronze → Silver
Silver → Gold
Gold → Trino
```

The remaining orchestration work concerns final runtime coordination through
Apache Airflow rather than redesign of the ingestion architecture itself.