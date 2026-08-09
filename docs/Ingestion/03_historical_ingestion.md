# Historical Ingestion

## 1. Overview

The historical ingestion process is responsible for acquiring previously
published meteorological and energy data from the external sources integrated
into the Energy Lakehouse Platform.

The historical load is primarily used to populate the Bronze layer during the
initial deployment of the platform.

The three supported sources are:

- AEMET OpenData.
- Open-Meteo.
- REE / ESIOS.

Historical ingestion is implemented independently for each source because the
available temporal ranges, API interfaces and request limitations may differ.

---

## 2. Objectives

The historical ingestion process has the following objectives:

- Retrieve historical information from each external source.
- Support configurable start and end dates.
- Preserve the original information returned by the source.
- Perform basic technical validation of API responses.
- Store the acquired data in the Bronze layer.
- Allow failed periods to be executed again without requiring a complete reload.
- Provide a reproducible mechanism for rebuilding the initial dataset.

No business transformations or cross-source integrations are performed during
historical ingestion.

---

## 3. General Process

The historical ingestion workflow follows this general pattern:

```text
Start date + End date
          |
          v
+---------------------+
| Historical Ingestion|
+----------+----------+
           |
           v
+---------------------+
| Split requested     |
| period if required  |
+----------+----------+
           |
           v
+---------------------+
| External API        |
+----------+----------+
           |
           v
+---------------------+
| Technical validation|
+----------+----------+
           |
           v
+---------------------+
| Bronze persistence  |
+---------------------+
```

The requested period may be divided into smaller intervals when required by the
characteristics or limitations of an external API.

---

## 4. Date Range

Historical ingestion accepts a temporal range defined by:

```text
start_date
end_date
```

The exact historical range used for the final project will be determined
according to:

- Availability of each source.
- API limitations.
- Data volume.
- Execution time.
- Analytical requirements of the project.

The selected final range will be documented after validation against the real
APIs.

---

## 5. AEMET Historical Ingestion

The AEMET historical ingestion process retrieves historical meteorological data
using the AEMET OpenData API.

The process will:

1. Receive the requested date range.
2. Build the corresponding authenticated API request.
3. Request the available meteorological information.
4. Validate the API response.
5. Retrieve the dataset referenced by the service when applicable.
6. Persist the acquired source information in Bronze.

The exact AEMET datasets and endpoint parameters used by the final
implementation will be documented after technical validation.

---

## 6. Open-Meteo Historical Ingestion

Open-Meteo historical information is obtained using its historical archive
service.

The process will:

1. Receive the requested date range.
2. Receive or determine the geographical coordinates required by the request.
3. Select the required meteorological variables.
4. Request the historical information.
5. Validate the returned response.
6. Persist the acquired information in Bronze.

Open-Meteo does not require an API credential for the access pattern used by
this project.

---

## 7. REE / ESIOS Historical Ingestion

The REE / ESIOS historical ingestion process retrieves historical information
related to the Spanish electricity system.

The process will:

1. Receive the requested date range.
2. Select the required energy dataset or indicator.
3. Build the authenticated request.
4. Retrieve the available information.
5. Validate the returned response.
6. Persist the acquired information in Bronze.

The final indicator identifiers and API parameters will be documented after
technical validation against the service.

---

## 8. Request Chunking

A complete historical period should not necessarily be retrieved using a single
API request.

The ingestion architecture allows large date ranges to be divided into smaller
time windows.

Conceptually:

```text
Requested period
2025-01-01 -------------------------- 2025-12-31

        |
        v

+-------------+-------------+-------------+-----+
| Window 1    | Window 2    | Window 3    | ... |
+-------------+-------------+-------------+-----+
        |
        v
Individual API requests
```

The appropriate window size can be configured independently for each source.

This approach provides several advantages:

- Reduced impact of request failures.
- Easier retries.
- Lower memory requirements.
- Better control of API limits.
- Improved execution traceability.

The definitive chunk sizes will be established after testing the real APIs.

---

## 9. Technical Validation

Each historical request must pass basic technical validation before its data is
considered successfully acquired.

Validation includes, where applicable:

- Successful HTTP status.
- Valid response format.
- Expected response structure.
- Presence of data.
- Valid requested date interval.
- Detection of malformed or incomplete responses.

Advanced data-quality rules are outside the responsibility of historical
ingestion and will be implemented in later processing stages.

---

## 10. Error Handling

A failure affecting one historical interval should not require restarting the
complete historical load.

The ingestion process is designed so that individual periods can be executed
again.

Potential failures include:

- Network errors.
- Request timeouts.
- Authentication failures.
- API service unavailability.
- Invalid responses.
- Empty datasets.
- API rate or request limitations.

Connector-level errors are recorded through the common logging mechanism.

Orchestration-level retry policies will subsequently be managed by Apache
Airflow.

---

## 11. Bronze Output

Historical data is persisted in the Bronze layer while preserving the source
representation as much as possible.

A conceptual organization is:

```text
bronze/
|
|-- aemet/
|   `-- <dataset>/
|
|-- open_meteo/
|   `-- <dataset>/
|
`-- esios/
    `-- <dataset>/
```

Additional temporal partitioning may be introduced according to the
characteristics of each dataset.

The ingestion process must not apply Silver or Gold transformations before
persistence in Bronze.

---

## 12. Reproducibility

Historical ingestion is designed to be reproducible.

Given the same:

- Data source.
- Dataset.
- Start date.
- End date.
- Configuration.

the platform must be able to request the corresponding historical information
again.

This capability is important for rebuilding the Bronze layer or recovering
specific periods when required.

---

## 13. Execution

The final ingestion interface will allow the historical mode to be selected
explicitly.

Conceptually:

```text
run_ingestion
    |
    |-- source
    |-- mode = historical
    |-- start_date
    `-- end_date
```

The exact command-line interface will be documented once
`run_ingestion.py` has been implemented and validated.

---

## 14. Validation Status

The historical ingestion architecture and implementation are developed
independently from the containerized infrastructure.

Final end-to-end validation will verify:

- Authentication against the required APIs.
- Historical data retrieval.
- Handling of configured date ranges.
- Request chunking.
- Bronze persistence.
- Integration with the platform storage layer.
- Compatibility with subsequent Lakehouse processing.

The results of these tests will be recorded in the ingestion validation
documentation.