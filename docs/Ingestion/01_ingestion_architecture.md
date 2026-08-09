# Ingestion Architecture

## 1. Overview

The ingestion layer is responsible for acquiring meteorological and energy data
from the external public data sources used by the Energy Lakehouse Platform.

The platform integrates three main data providers:

- AEMET OpenData.
- Open-Meteo.
- REE / ESIOS.

The ingestion processes are implemented in Python and are designed to support
both historical data loading and periodic incremental updates.

The main objective of this layer is to acquire the source data reliably while
preserving the original information before subsequent transformation and
normalization processes are applied.

---

## 2. Ingestion Flow

The general ingestion flow of the platform is:

```text
                    +-------------------+
                    |   AEMET OpenData  |
                    +---------+---------+
                              |
                              |
+-------------------+         |         +-------------------+
|    Open-Meteo     +---------+---------+    REE / ESIOS    |
+-------------------+                   +-------------------+
                              |
                              v
                    +-------------------+
                    | Python Ingestion  |
                    |      Layer        |
                    +---------+---------+
                              |
                              v
                    +-------------------+
                    | Initial Validation|
                    +---------+---------+
                              |
                              v
                    +-------------------+
                    |   Bronze Layer    |
                    |     Raw Data      |
                    +---------+---------+
                              |
                              v
                    +-------------------+
                    | Lakehouse         |
                    | Processing        |
                    | Spark / Iceberg   |
                    +-------------------+
```

The ingestion layer is intentionally separated from the transformation layer.

Its responsibility is limited to:

1. Connecting to external APIs.
2. Requesting the required datasets.
3. Performing basic technical validations.
4. Adding ingestion metadata when required.
5. Persisting the acquired information in the Bronze layer.

Business transformations, normalization and integration between data sources
are performed in later stages of the Lakehouse pipeline.

---

## 3. Data Sources

### 3.1 AEMET OpenData

AEMET provides official meteorological information for Spain.

The connector is responsible for retrieving the meteorological datasets required
by the platform using the AEMET OpenData API.

Authentication is performed using an API key.

---

### 3.2 Open-Meteo

Open-Meteo provides meteorological information through a public HTTP API.

The service does not require an API key for the access pattern used by this
project.

Open-Meteo complements the meteorological information available from AEMET
and provides access to historical and current weather data.

---

### 3.3 REE / ESIOS

REE / ESIOS provides information related to the Spanish electricity system.

The data used by the platform includes energy-related information required for
the analytical use case, such as electricity generation, demand and energy
prices where available through the selected API endpoints.

Authentication is performed using the access credentials provided for the API.

---

## 4. Ingestion Modes

The ingestion architecture supports two execution modes.

### 4.1 Historical ingestion

Historical ingestion is used to populate the platform with previously published
data.

A date range is supplied to the ingestion process and the connector retrieves
the available information for that period.

This process is primarily executed during the initial population of the
Lakehouse.

---

### 4.2 Incremental ingestion

Incremental ingestion is used to keep the platform updated after the initial
historical load.

Only data corresponding to the required new time window is requested from the
source systems.

This reduces unnecessary API calls and avoids repeatedly downloading the entire
historical dataset.

---

## 5. Project Structure

The ingestion code is organized by data source while sharing common
infrastructure components.

```text
ingestion/
|
|-- common/
|   |-- config.py
|   |-- exceptions.py
|   |-- http_client.py
|   |-- logger.py
|   `-- storage.py
|
|-- aemet/
|   |-- client.py
|   `-- ingest.py
|
|-- open_meteo/
|   |-- client.py
|   `-- ingest.py
|
|-- esios/
|   |-- client.py
|   `-- ingest.py
|
`-- run_ingestion.py
```

### Common components

The `common` package contains reusable functionality shared by the different
connectors.

- `config.py`: common configuration.
- `http_client.py`: common HTTP communication functionality.
- `logger.py`: logging configuration.
- `exceptions.py`: ingestion-specific exceptions.
- `storage.py`: abstraction for persistence of acquired data.

### Source connectors

Each external source has an independent package containing:

- `client.py`: communication with the external API.
- `ingest.py`: ingestion logic for the corresponding source.

This separation keeps API-specific logic isolated while allowing shared
functionality to be reused.

---

## 6. Bronze Layer

The output of the ingestion processes is stored in the Bronze layer.

The purpose of Bronze is to preserve source information with minimal
modification.

No analytical transformations or cross-source integrations are performed at
this stage.

A logical organization similar to the following is used:

```text
bronze/
|
|-- aemet/
|-- open_meteo/
`-- esios/
```

Additional partitioning by dataset and ingestion date can be applied depending
on the characteristics of each source.

The definitive persistence mechanism of the platform uses the object storage
layer defined by the Lakehouse architecture.

---

## 7. Configuration and Credentials

API credentials and environment-specific parameters must not be stored in the
source code committed to the repository.

The production project configuration uses environment variables loaded from the
local `.env` file.

The `.env` file is excluded from version control.

A `.env.example` file documents the variables required to execute the platform
without exposing real credentials.

Relevant variables include:

```text
AEMET_API_KEY
ESIOS_API_KEY
```

Open-Meteo does not require an API key for the access pattern used by this
project.

---

## 8. Error Handling

The ingestion layer is designed to handle common errors associated with external
APIs, including:

- Connection errors.
- HTTP errors.
- Request timeouts.
- Invalid responses.
- Empty responses.
- Authentication errors.
- Temporary service unavailability.

Common HTTP functionality is centralized so that timeout and retry policies can
be applied consistently across connectors.

Detailed orchestration-level retry policies will be handled by Apache Airflow
during the orchestration phase of the project.

---

## 9. Validation

Before data is persisted in Bronze, basic technical validation is performed.

Examples include:

- Successful HTTP response.
- Valid response format.
- Presence of expected data.
- Basic structural checks.
- Registration of the ingestion execution.

More advanced data-quality rules and business validations are applied in later
processing layers.

---

## 10. Separation of Responsibilities

The ingestion layer follows a clear separation of responsibilities:

```text
External APIs
      |
      v
Python ingestion
      |
      v
Technical validation
      |
      v
Bronze
      |
      v
PySpark transformations
      |
      v
Silver
      |
      v
Gold
```

This design ensures that acquisition logic remains independent from analytical
transformations and allows source data to be reprocessed without requesting it
again from the external APIs.

---

## 11. Development and Deployment

The ingestion components are developed as independent Python modules.

They can be developed and tested locally without requiring the complete
containerized platform to be running.

In the final deployment environment, the ingestion processes will operate as
part of the complete Lakehouse platform and will persist their output using the
storage infrastructure defined for the project.

This separation facilitates development, testing and reproducibility across
different environments.