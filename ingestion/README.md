# Ingestion Layer

## 1. Overview

The ingestion layer of the Energy Lakehouse Platform is responsible for
acquiring meteorological and energy data from external public APIs and
persisting the acquired information in the Bronze layer.

The platform integrates three data sources:

- AEMET OpenData.
- Open-Meteo.
- REE / ESIOS.

The ingestion components are implemented in Python and are designed to support
both historical and incremental data acquisition.

---

## 2. Architecture

The ingestion flow follows this architecture:

```text
+-------------------+
| AEMET OpenData    |
+---------+---------+
          |
          |
+---------v---------+
|                   |
| Python Ingestion  |
|                   |
+---------+---------+
          |
          v
+-------------------+
| Bronze Layer      |
+-------------------+
```

The same pattern is used for Open-Meteo and REE / ESIOS.

The ingestion layer is independent from subsequent Lakehouse transformations.

Cleaning, normalization, integration and analytical transformations are
performed in later processing stages.

---

## 3. Project Structure

```text
ingestion/
|
|-- __init__.py
|
|-- common/
|   |-- __init__.py
|   |-- config.py
|   |-- exceptions.py
|   |-- http_client.py
|   |-- logger.py
|   `-- storage.py
|
|-- aemet/
|   |-- __init__.py
|   |-- client.py
|   `-- ingest.py
|
|-- open_meteo/
|   |-- __init__.py
|   |-- client.py
|   `-- ingest.py
|
|-- esios/
|   |-- __init__.py
|   |-- client.py
|   `-- ingest.py
|
`-- run_ingestion.py
```

---

## 4. Common Components

### `config.py`

Contains common configuration used by the ingestion modules.

Configuration includes:

- API base URLs.
- HTTP timeout.
- Retry configuration.
- Bronze directory.
- References to API credentials obtained from environment variables.

### `exceptions.py`

Defines the custom exception hierarchy used by the ingestion layer.

Examples include:

- Configuration errors.
- Connection errors.
- Authentication errors.
- API request errors.
- Invalid API responses.
- Invalid date ranges.
- Storage errors.

### `http_client.py`

Provides reusable HTTP functionality for all external connectors.

It includes:

- HTTP sessions.
- Request timeout.
- Automatic retries.
- Handling of temporary HTTP errors.
- Authentication error detection.
- JSON deserialization.
- Empty-response detection.

### `logger.py`

Provides a common logging configuration for ingestion modules.

### `storage.py`

Provides Bronze persistence functionality.

The current implementation supports local JSON persistence and organizes
generated data by:

```text
source
dataset
year
month
day
```

The storage component isolates persistence logic from the API connectors.

---

## 5. AEMET Connector

The AEMET connector is implemented in:

```text
ingestion/aemet/
```

### Client

`client.py` handles communication with AEMET OpenData.

The client supports retrieval of daily climatological observations for a
specified meteorological station and temporal interval.

AEMET OpenData uses a two-step acquisition mechanism:

```text
Request dataset
      |
      v
AEMET metadata response
      |
      v
Dataset URL
      |
      v
Download actual data
```

### Ingestion

`ingest.py` coordinates:

```text
AEMET client
     |
     v
Data acquisition
     |
     v
Bronze persistence
```

Both historical and incremental temporal windows are supported.

---

## 6. Open-Meteo Connector

The Open-Meteo connector is implemented in:

```text
ingestion/open_meteo/
```

No API key is required for the Open-Meteo access pattern used by this project.

### Client

`client.py` supports:

- Historical weather acquisition.
- Current weather acquisition.
- Coordinate validation.
- Date-range validation.
- Selection of meteorological variables.

### Default meteorological variables

The ingestion implementation currently defines the following default hourly
variables:

```text
temperature_2m
relative_humidity_2m
precipitation
cloud_cover
wind_speed_10m
surface_pressure
```

The final selection will be confirmed during API and analytical validation.

### Ingestion

`ingest.py` coordinates Open-Meteo acquisition and Bronze persistence.

Historical requests accept an explicit date range.

Current acquisition is used as the initial implementation of the incremental
ingestion path.

---

## 7. REE / ESIOS Connector

The ESIOS connector is implemented in:

```text
ingestion/esios/
```

### Client

`client.py` provides access to ESIOS indicators.

It supports parameters including:

```text
indicator_id
start_date
end_date
time_trunc
time_agg
geo_ids
geo_trunc
geo_agg
```

This allows the connector to remain independent from specific energy
indicators.

### Ingestion

`ingest.py` supports both historical and incremental acquisition.

The dataset name and indicator identifier are supplied to the ingestion process
rather than being hardcoded in the connector.

This allows the same implementation to support different energy datasets such
as generation, demand and prices after the definitive indicators have been
validated.

---

## 8. Configuration

Real credentials must never be stored in the repository.

The ingestion code obtains credentials from environment variables.

Required variables:

```text
AEMET_API_KEY
ESIOS_API_KEY
```

Open-Meteo does not require an API key for the access pattern used by this
project.

The repository contains:

```text
.env.example
```

to document the required configuration.

The real:

```text
.env
```

must remain outside version control.

---

## 9. Bronze Storage

During local development, ingestion output can be persisted under:

```text
data/
`-- bronze/
```

The generated structure follows this pattern:

```text
data/
`-- bronze/
    `-- <source>/
        `-- <dataset>/
            `-- year=YYYY/
                `-- month=MM/
                    `-- day=DD/
                        `-- <generated-file>.json
```

For example:

```text
data/
`-- bronze/
    `-- open_meteo/
        `-- weather/
            `-- year=2026/
                `-- month=08/
                    `-- day=09/
                        `-- open_meteo_weather_<timestamp>.json
```

Generated Bronze files contain two main sections:

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

Generated runtime data must not be committed to Git.

---

## 10. Command-Line Interface

The common entry point is:

```text
ingestion/run_ingestion.py
```

The CLI supports the following sources:

```text
aemet
open_meteo
esios
```

and the following ingestion modes:

```text
historical
incremental
```

The module can be executed from the project root using:

```powershell
python -m ingestion.run_ingestion <source> [arguments]
```

---

## 11. Open-Meteo Example

Historical ingestion:

```powershell
python -m ingestion.run_ingestion open_meteo `
    --mode historical `
    --latitude 43.0 `
    --longitude -2.5 `
    --start-date 2025-01-01 `
    --end-date 2025-01-31
```

Incremental/current ingestion:

```powershell
python -m ingestion.run_ingestion open_meteo `
    --mode incremental `
    --latitude 43.0 `
    --longitude -2.5
```

The coordinates above are examples and do not define the definitive geographic
configuration of the project.

---

## 12. AEMET Example

Conceptual historical execution:

```powershell
python -m ingestion.run_ingestion aemet `
    --mode historical `
    --station-id <STATION_ID> `
    --start-date 2025-01-01 `
    --end-date 2025-01-31
```

Incremental execution uses the same interface:

```powershell
python -m ingestion.run_ingestion aemet `
    --mode incremental `
    --station-id <STATION_ID> `
    --start-date 2025-02-01 `
    --end-date 2025-02-02
```

The definitive AEMET stations will be selected and documented after technical
and geographic validation.

---

## 13. ESIOS Example

Conceptual historical execution:

```powershell
python -m ingestion.run_ingestion esios `
    --mode historical `
    --indicator-id <INDICATOR_ID> `
    --dataset <DATASET_NAME> `
    --start-date 2025-01-01 `
    --end-date 2025-01-31
```

Optional parameters include:

```text
--time-trunc
--time-agg
--geo-id
--geo-trunc
--geo-agg
```

The definitive indicator identifiers and geographic parameters will be selected
after validation against the real ESIOS API.

---

## 14. Historical Ingestion

Historical ingestion receives an explicit temporal interval:

```text
start_date
end_date
```

Its objective is to populate the initial Bronze datasets.

Large historical ranges may subsequently be divided into smaller request
windows depending on the limitations and behaviour of each external API.

---

## 15. Incremental Ingestion

Incremental ingestion is designed to retrieve newly available information after
the initial historical load.

Historical and incremental ingestion reuse the same API clients.

Conceptually:

```text
Historical --------\
                    \
                     > Source Client ---> Bronze
                    /
Incremental -------/
```

The definitive scheduling and automatic calculation of incremental windows will
be implemented as part of the orchestration workflow.

---

## 16. Error Handling

The ingestion layer provides controlled handling for:

- Connection failures.
- Timeouts.
- HTTP errors.
- Authentication errors.
- Invalid JSON responses.
- Empty responses.
- Invalid date ranges.
- Storage failures.

Temporary HTTP failures can be retried by the common HTTP client.

Orchestration-level retries will subsequently be managed by Apache Airflow.

---

## 17. Development Without Docker

The Python ingestion modules are intentionally separated from the containerized
platform infrastructure.

This allows development of:

- API clients.
- Ingestion logic.
- Validation logic.
- Logging.
- Local Bronze persistence.
- Command-line interfaces.

without requiring the complete Docker environment.

Final platform integration requires validation against the deployed
infrastructure.

---

## 18. Final Integration

In the complete Lakehouse environment, the ingestion layer will be integrated
with the platform storage and processing infrastructure.

The final flow is:

```text
AEMET -----------+
                 |
Open-Meteo ------+--> Python Ingestion --> Bronze --> Spark / Iceberg
                 |
REE / ESIOS -----+
```

MinIO provides the S3-compatible object-storage infrastructure of the platform.

Apache Spark and Apache Iceberg are used during subsequent Lakehouse processing.

Apache Airflow will orchestrate scheduled ingestion executions.

---

## 19. Current Status

The ingestion layer has been implemented and technically validated against the
three external data sources and the MinIO Bronze storage layer.

Validation includes real API requests, historical and incremental ingestion,
frequency-specific ingestion scenarios, Bronze persistence, error handling and
automated regression testing.

| Component | Status |
|---|---|
| Common configuration | Validated |
| Exception hierarchy | Validated |
| HTTP client | Validated |
| Logging | Validated |
| Local Bronze storage | Validated |
| Open-Meteo client | Validated |
| Open-Meteo ingestion | Validated |
| AEMET client | Validated |
| AEMET ingestion | Validated |
| ESIOS client | Validated |
| ESIOS ingestion | Validated |
| Common CLI | Validated |
| Historical ingestion | Validated |
| Incremental ingestion | Validated |
| MinIO integration | Validated |
| End-to-end API to Bronze integration | Validated |

The final ingestion regression suite completed successfully with:

```text
56 passed
```

Detailed validation evidence is documented in:

```text
docs/Ingestion/06_validation_and_testing.md
```

The ingestion layer is considered technically implemented and validated for
Phase 3.

Subsequent cleaning, normalization, business-level deduplication, source
integration and analytical transformations are handled in the Lakehouse
processing phase.
