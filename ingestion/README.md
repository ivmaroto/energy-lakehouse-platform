# Ingestion Layer

## 1. Overview

The ingestion layer of the Energy Lakehouse Platform is responsible for
acquiring public meteorological, geographical and electricity-system data and
persisting the acquired source information in the Bronze layer.

The platform currently uses four source domains:

- AEMET OpenData.
- Open-Meteo.
- REE / ESIOS.
- CNIG / IGN geographical reference data.

The ingestion components are implemented in Python.

Their responsibility is limited to source acquisition, technical validation,
ingestion metadata generation and Bronze persistence.

Cleaning, typing, geographical normalization, deduplication, source integration
and analytical transformations are performed later by the Silver and Gold
processing layers.

---

## 2. Architecture

The ingestion flow follows this general architecture:

```text
AEMET OpenData ──────┐
Open-Meteo ──────────┤
REE / ESIOS ─────────┼──► Python ingestion
CNIG / IGN ──────────┘          │
                                ▼
                       Technical validation
                                │
                                ▼
                          MinIO / Bronze
                                │
                                ▼
                      Spark / Iceberg Silver
                                │
                                ▼
                      Spark / Iceberg Gold
```

The ingestion layer is intentionally independent from the analytical
transformation logic.

Bronze therefore preserves the source representation before downstream
Lakehouse processing is applied.

---

## 3. Current Source Scope

### AEMET

The active AEMET Bronze datasets are:

```text
stations
current_observations
```

`stations` provides the meteorological-station catalogue used by the platform.

The validated current station catalogue contains:

```text
926 stations
```

`current_observations` provides recent conventional meteorological
observations.

AEMET current observations are a recent/current source and are not used to
reconstruct arbitrary historical periods.

### Open-Meteo

The active Open-Meteo Bronze datasets are:

```text
weather_hourly
weather_15min
```

The AEMET station catalogue provides the point catalogue and coordinates used
for Open-Meteo acquisition.

The validated production location catalogue contains:

```text
926 locations
```

Historical hourly acquisition uses the Open-Meteo archive service.

Historical 15-minute acquisition uses the Open-Meteo Historical Forecast API.

The standard Forecast API remains available for current/incremental acquisition.

### REE / ESIOS

The active ESIOS configuration contains:

```text
11 hourly energy-generation indicators
9 monthly installed-capacity indicators
```

The selected indicators are externalized in:

```text
config/esios_indicators.json
```

The current ingestion scope does not include the previously evaluated ESIOS
5-minute datasets.

### CNIG / IGN

CNIG / IGN provides the canonical geographical source data used later for
territorial normalization.

The Bronze master datasets used by the current implementation are:

```text
provinces
municipalities
```

Autonomous communities are subsequently derived and normalized in the Silver
layer.

---

## 4. Project Structure

The ingestion package is organized by source and shared infrastructure.

The main structure includes:

```text
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

Source-specific API logic remains isolated while common configuration, HTTP,
logging, temporal and storage functionality is reused.

---

## 5. Common Components

### `config.py`

Contains shared ingestion configuration.

It includes configuration for:

- API base URLs.
- Open-Meteo archive and historical-forecast endpoints.
- HTTP timeout.
- HTTP retry behaviour.
- Open-Meteo retry and pacing configuration.
- Historical chunk sizes.
- MinIO connectivity.
- References to API credentials loaded from environment variables.

### `date_utils.py`

Provides common temporal validation and range-processing functionality.

### `esios_config.py`

Loads the selected ESIOS indicators from:

```text
config/esios_indicators.json
```

This avoids embedding the final indicator catalogue directly in ingestion or
Airflow source code.

### `exceptions.py`

Defines the ingestion-specific exception hierarchy.

Handled error categories include:

- configuration errors;
- connection failures;
- authentication errors;
- API request errors;
- invalid responses;
- invalid date ranges;
- storage errors.

### `http_client.py`

Provides common HTTP functionality including:

- reusable HTTP sessions;
- request timeouts;
- retry handling;
- exponential retry backoff;
- temporary HTTP-error handling;
- authentication-error detection;
- JSON deserialization.

### `logger.py`

Provides common logging configuration.

### `storage.py`

Provides the Bronze persistence abstraction used by the source connectors.

The production-like Bronze backend is MinIO.

Objects are organized logically by source, dataset and canonical observation
time where the dataset is temporal.

For temporal Bronze datasets:

```text
year
month
day
```

represent the source observation period, not the ingestion timestamp.

`ingestion_timestamp` is retained as technical audit metadata only.

The storage layer isolates object persistence from API-specific connector code.

---

## 6. AEMET Connector

The AEMET connector is implemented under:

```text
ingestion/aemet/
```

AEMET OpenData requires an API key.

The credential is supplied through:

```text
AEMET_API_KEY
```

### Active datasets

The final active AEMET scope is:

```text
stations
current_observations
```

The station catalogue acts as the official point catalogue used by the
meteorological processing flow.

Current observations provide recent conventional meteorological measurements.

AEMET OpenData uses a two-stage response mechanism for applicable endpoints:

```text
Request AEMET endpoint
        │
        ▼
AEMET metadata response
        │
        ▼
Returned dataset URL
        │
        ▼
Actual source dataset
```

The acquired source payload is technically validated before Bronze
persistence.

---

## 7. Open-Meteo Connector

The Open-Meteo connector is implemented under:

```text
ingestion/open_meteo/
```

No API key is required for the Open-Meteo access pattern used by this project.

### Active datasets

```text
weather_hourly
weather_15min
```

### Hourly historical acquisition

Historical hourly weather is obtained from:

```text
https://archive-api.open-meteo.com/v1/archive
```

### 15-minute historical acquisition

Historical 15-minute weather is obtained from:

```text
https://historical-forecast-api.open-meteo.com/v1/forecast
```

This distinction is important because the standard Forecast API is not used for
arbitrary historical 15-minute periods.

### Current/incremental acquisition

Current or incremental forecast access uses:

```text
https://api.open-meteo.com/v1/forecast
```

### Meteorological variables

The current analytical flow uses Open-Meteo information including variables
such as:

```text
temperature_2m
relative_humidity_2m
precipitation
shortwave_radiation
direct_normal_irradiance
wind_speed_80m
wind_direction_80m
wind_speed_120m
wind_direction_120m
```

Additional source variables can remain available upstream even when they are
not selected for the final Gold products.

---

## 8. Open-Meteo Batch Acquisition and Recovery

Open-Meteo acquisition operates over the complete AEMET station catalogue.

The current validated catalogue contains:

```text
926 locations
```

The implementation includes controls for large batch executions and external
API rate limitations.

These controls include:

- configurable retries;
- exponential backoff;
- configurable inter-batch delay;
- validation of expected temporal coverage;
- inspection of already persisted Bronze objects;
- resumable acquisition of missing locations.

The Bronze-state logic is implemented in:

```text
ingestion/open_meteo/bronze_state.py
```

Before resuming a historical batch, persisted objects can be checked against the
requested temporal window.

A location is considered complete only when the expected temporal coverage is
present.

For a complete UTC day, the validated requirements are:

```text
weather_hourly
→ 24 timestamps
```

and:

```text
weather_15min
→ 96 timestamps
```

For the validated six-day interval:

```text
2026-01-10 → 2026-01-15
```

the expected observations per location were:

```text
Hourly:
6 days × 24 = 144 observations

15 minutes:
6 days × 24 × 4 = 576 observations
```

The complete historical acquisition produced:

```text
weather_hourly = 926 / 926 locations
weather_15min  = 926 / 926 locations
```

This is execution-specific historical evidence.

---

## 9. REE / ESIOS Connector

The ESIOS connector is implemented under:

```text
ingestion/esios/
```

Authentication uses:

```text
ESIOS_API_KEY
```

The connector remains generic and accepts the selected indicator and temporal
range as parameters.

### Final hourly scope

The current configuration contains 11 hourly energy indicators:

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

### Final monthly scope

The current configuration contains 9 installed-capacity indicators:

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

The selected indicator IDs are maintained outside connector source code in:

```text
config/esios_indicators.json
```

---

## 10. ESIOS Response Validation

A successful HTTP request is not sufficient to assume that ESIOS observations
exist for the requested interval.

The ingestion implementation validates the expected indicator structure.

An ESIOS response containing:

```text
indicator.values = []
```

is a valid source response representing:

```text
NO_DATA
```

It must not be converted into fabricated observations or zero-valued
measurements.

Therefore:

```text
NO_DATA != zero-valued measurement
```

and:

```text
NULL != 0
```

Real API validation confirmed that the configured ESIOS indicators returned
actual observations for the historical intervals used in the validated E2E
executions.

That historical availability must not be generalized to every future requested
interval.

---

## 11. CNIG / IGN Geographical Data

CNIG / IGN source data provides the geographical reference used by the
Lakehouse.

The current Bronze geographical master includes:

```text
provinces
municipalities
```

Validated source cardinalities used downstream are:

```text
52 province-level entities
8132 municipalities
```

Autonomous communities are derived from the canonical territorial information
during Silver processing.

CNIG codes are preserved as strings so leading zeroes are not lost.

---

## 12. Configuration and Credentials

Real credentials must never be stored in the repository.

The ingestion code obtains credentials and environment-specific parameters from
environment variables.

Relevant values include:

```text
AEMET_API_KEY
ESIOS_API_KEY

MINIO_ENDPOINT
MINIO_ROOT_USER
MINIO_ROOT_PASSWORD
MINIO_BUCKET
MINIO_SECURE
```

The repository provides:

```text
.env.example
```

The real:

```text
.env
```

must remain outside version control.

Open-Meteo does not require an API key for the access pattern used by the
project.

---

## 13. Bronze Storage

The current production-like Bronze persistence backend is MinIO.

Temporal Bronze datasets are physically organized by:

```text
observation time
```

and not by:

```text
ingestion_timestamp
```

`ingestion_timestamp` is retained only as technical audit metadata.

The final canonical Bronze paths are:

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

The requested source interval and ingestion timestamp can additionally be
retained in Bronze metadata for traceability.

---

## 14. Bronze Object Structure

JSON Bronze objects contain two principal sections:

```json
{
  "metadata": {
    "source": "...",
    "dataset": "...",
    "ingestion_mode": "...",
    "ingestion_timestamp": "...",
    "requested_start_date": "...",
    "requested_end_date": "..."
  },
  "data": {}
}
```

Source-specific metadata may additionally be included when required for
traceability.

Examples include:

```text
AEMET:
station identifiers

Open-Meteo:
location_id
latitude
longitude

ESIOS:
indicator_id
```

Generated runtime Bronze data must not be committed to Git.

---

## 15. Historical Ingestion

Historical ingestion receives an explicit temporal interval.

Its purpose is to populate Bronze with source data available for that interval.

Large requested periods can be divided into smaller source-specific request
windows.

Historical acquisition includes:

```text
Open-Meteo hourly
Open-Meteo 15-minute
ESIOS hourly
ESIOS monthly
```

AEMET station and CNIG geographical masters are loaded independently from the
historical observation interval.

AEMET current observations remain a recent/current dataset and are explicitly
excluded from the final:

```text
historical_reload
```

workflow for arbitrary historical reconstruction.

The final historical orchestration parameters are:

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

---

## 16. Incremental Ingestion

Incremental ingestion is intended to retrieve newly available information after
the initial historical population.

Historical and incremental paths reuse the same source-specific connector
architecture wherever applicable:

```text
Historical ─────┐
                ├──► Source connector ──► Bronze
Incremental ────┘
```

The exact execution window depends on the source because publication latency and
data availability are not identical across providers.

The final Airflow orchestration model contains exactly four DAGs:

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

`open_meteo_15min` is not scheduled as a recurrent 15-minute production
pipeline.

---

## 17. Error Handling

The ingestion layer provides controlled handling for:

- connection failures;
- timeouts;
- HTTP errors;
- authentication errors;
- invalid JSON responses;
- malformed API structures;
- ESIOS `NO_DATA` responses and malformed ESIOS structures;
- invalid date ranges;
- incomplete Open-Meteo temporal coverage;
- storage failures.

Temporary HTTP failures are retried according to the configured retry policy.

Open-Meteo additionally uses source-specific retry, backoff and pacing controls
for large station batches.

Failures are raised before data is promoted downstream as a valid acquisition
whenever possible.

A valid ESIOS `NO_DATA` response is not treated as a fabricated successful
observation and does not produce zero-valued measurements.

---

## 18. Automated Tests

The ingestion implementation includes automated regression tests for:

- AEMET;
- Open-Meteo;
- ESIOS;
- shared date utilities;
- common storage;
- Open-Meteo Bronze-state validation;
- historical Open-Meteo batch behaviour;
- historical orchestration support.

The latest validated complete ingestion regression suite completed with:

```text
84 passed
```

No ingestion test failures remained in that validated execution.

---

## 19. Real Historical Validation

A complete real-source Bronze historical execution was validated for:

```text
2026-01-10 → 2026-01-15
```

This execution is retained as historical evidence.

It predates the final `historical_reload` policy because it still included
AEMET `current_observations`, which the final historical workflow now excludes.

The execution completed with:

```text
AEMET station master:
1 Bronze object
926 stations

CNIG masters:
2 Bronze objects

ESIOS hourly:
11 files

ESIOS monthly:
9 files

Open-Meteo:
926 locations
926 hourly files
926 15-minute files

AEMET current observations:
1 file
```

The execution finished with:

```text
BRONZE HISTORICAL LOAD COMPLETED
```

The Open-Meteo temporal coverage for that execution was subsequently confirmed
through the Silver row counts:

```text
926 × 144 hourly observations
= 133344 rows

926 × 576 fifteen-minute observations
= 533376 rows
```

ESIOS data was also successfully transformed downstream, producing in that
specific historical execution:

```text
38443 hourly Silver observations
123 monthly Silver observations
```

These are execution-specific historical row counts and are not permanent table
cardinalities.

This confirms that the historical ingestion supplied real data successfully to
the downstream Lakehouse processing chain.

---

## 20. End-to-End Integration

The ingestion layer has been validated as part of the complete data-processing
path:

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
PySpark / Silver
       │
       ▼
PySpark / Gold
       │
       ▼
Trino
```

Real data acquired through the ingestion layer was successfully transformed
into the final Silver and Gold Apache Iceberg tables and queried through Trino.

The final `historical_reload` Airflow workflow was also executed successfully
end to end.

This demonstrates that Bronze ingestion is operational as the source stage of
the Lakehouse rather than only as an isolated connector implementation.

---

## 21. Current Status

| Component | Status |
|---|---|
| Common configuration | Validated |
| Exception hierarchy | Validated |
| HTTP client | Validated |
| Logging | Validated |
| MinIO Bronze storage | Validated |
| AEMET station ingestion | Validated |
| AEMET current-observation ingestion | Validated |
| Open-Meteo hourly ingestion | Validated |
| Open-Meteo 15-minute ingestion | Validated |
| Open-Meteo historical 15-minute endpoint | Validated |
| Open-Meteo batch completeness validation | Validated |
| Open-Meteo resumable acquisition | Validated |
| ESIOS hourly ingestion | Validated |
| ESIOS monthly ingestion | Validated |
| ESIOS `NO_DATA` handling | Validated |
| CNIG master ingestion | Validated |
| Historical ingestion | Validated |
| MinIO integration | Validated |
| API → Bronze integration | Validated |
| Bronze → Silver → Gold → Trino E2E | Validated |
| `historical_reload` E2E runtime | Validated |
| Ingestion regression suite | 84 passed |

Detailed ingestion validation evidence is documented in:

```text
docs/Ingestion/06_validation_and_testing.md
```

The ingestion layer is technically implemented and operational for the current
project scope.

Cleaning, normalization, deduplication, geographical harmonization and
analytical integration are performed by the downstream Silver and Gold layers.
