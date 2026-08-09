# Incremental Ingestion

## 1. Overview

After the initial historical load, the Energy Lakehouse Platform uses
incremental ingestion processes to keep meteorological and energy datasets
updated.

Incremental ingestion retrieves only the new data required since the previous
successful execution instead of downloading the complete historical dataset
again.

The process applies independently to:

- AEMET OpenData.
- Open-Meteo.
- REE / ESIOS.

---

## 2. Objectives

The incremental ingestion process has the following objectives:

- Retrieve newly available data from each external source.
- Minimize unnecessary API requests.
- Avoid repeatedly downloading complete historical datasets.
- Preserve source information in the Bronze layer.
- Support repeated executions.
- Detect and handle failed requests.
- Provide the data required for subsequent Lakehouse processing.

---

## 3. General Process

The incremental ingestion workflow follows this general pattern:

```text
Previous ingestion state
          |
          v
+----------------------+
| Determine required   |
| temporal window      |
+----------+-----------+
           |
           v
+----------------------+
| External API request |
+----------+-----------+
           |
           v
+----------------------+
| Technical validation |
+----------+-----------+
           |
           v
+----------------------+
| Bronze persistence   |
+----------+-----------+
           |
           v
+----------------------+
| Register execution   |
+----------------------+
```

Each source is processed independently because publication frequency and data
availability may differ between providers.

---

## 4. Incremental Window

An incremental execution requires determining the temporal interval that must
be requested.

Conceptually:

```text
Existing Bronze data               New data

|-------------------------|--------------------|
                          ^
                          |
                  Last processed point
```

The next execution requests the required period after the previously processed
data.

The definitive mechanism used to determine the last processed point will be
validated during implementation.

---

## 5. AEMET Incremental Ingestion

The AEMET connector will periodically request newly available meteorological
information.

The process will:

1. Determine the required temporal interval.
2. Build an authenticated AEMET request.
3. Retrieve the available data.
4. Validate the API response.
5. Persist the source information in Bronze.
6. Record the result of the execution.

The exact update frequency will depend on the selected AEMET datasets and their
publication frequency.

---

## 6. Open-Meteo Incremental Ingestion

The Open-Meteo connector will retrieve the meteorological information required
for the new temporal window.

The process will:

1. Determine the requested time interval.
2. Select the required geographical coordinates.
3. Select the required meteorological variables.
4. Request the information from Open-Meteo.
5. Validate the response.
6. Persist the acquired information in Bronze.
7. Record the result of the execution.

No authentication credential is required for the Open-Meteo access pattern used
by this project.

---

## 7. REE / ESIOS Incremental Ingestion

The REE / ESIOS connector will retrieve newly available energy information for
the selected indicators.

The process will:

1. Determine the required temporal interval.
2. Select the energy dataset or indicator.
3. Build the authenticated API request.
4. Retrieve the available information.
5. Validate the response.
6. Persist the acquired information in Bronze.
7. Record the result of the execution.

The execution frequency will be adapted to the publication characteristics of
the selected energy datasets.

---

## 8. Idempotency

Incremental ingestion should be designed to tolerate repeated executions of the
same temporal period.

For example:

```text
Execution 1
2026-08-01 -> 2026-08-02

Execution 2
2026-08-02 -> 2026-08-03

Retry
2026-08-02 -> 2026-08-03
```

A retry should not corrupt the Bronze dataset.

Duplicate detection and definitive deduplication rules will be applied where
appropriate during subsequent Lakehouse processing.

The Bronze layer prioritizes preservation and traceability of source data.

---

## 9. Late or Updated Source Data

External APIs may publish data after its corresponding observation period or
may subsequently revise previously published information.

For this reason, the incremental architecture allows a configurable overlap
between consecutive ingestion windows when required.

Conceptually:

```text
Previous execution
|----------------------|

Next execution
                  |----------------------|
                  <---- overlap ---->
```

This strategy allows recently modified source information to be acquired again.

The appropriate overlap will be determined independently for each source after
the real API behaviour has been validated.

---

## 10. Error Handling

A failed incremental execution must not invalidate previously acquired data.

Potential failures include:

- Network connectivity errors.
- HTTP errors.
- Authentication failures.
- Request timeouts.
- Temporary API unavailability.
- Invalid responses.
- Empty responses.

Failures are recorded through the common logging mechanism.

A failed temporal window can subsequently be executed again.

Automatic orchestration retries will be configured during the Apache Airflow
phase.

---

## 11. Technical Validation

Before incremental data is persisted, the connector performs basic technical
validation.

This includes, where applicable:

- Successful HTTP response.
- Valid response format.
- Expected response structure.
- Valid requested dates.
- Presence of the expected dataset.
- Detection of malformed responses.

Business-level quality validation is performed during later Lakehouse
processing.

---

## 12. Bronze Persistence

Incrementally acquired data is stored using the same Bronze organization as the
historical ingestion process.

Conceptually:

```text
bronze/
|
|-- aemet/
|-- open_meteo/
`-- esios/
```

Historical and incremental ingestion therefore feed the same logical Bronze
layer.

This allows downstream processing to operate independently from the mechanism
used to acquire each dataset.

---

## 13. Historical and Incremental Relationship

Historical and incremental ingestion use the same source connectors.

```text
                  +------------------+
Historical ------>|                  |
                  | Source Connector |------> Bronze
Incremental ----->|                  |
                  +------------------+
```

The main difference is the temporal range supplied to each execution.

This avoids maintaining separate API implementations for historical and
incremental data.

---

## 14. Future Orchestration

Incremental ingestion will ultimately be executed automatically by Apache
Airflow.

Airflow will be responsible for:

- Scheduling ingestion jobs.
- Managing dependencies.
- Retrying failed executions.
- Recording execution status.
- Monitoring the ingestion workflow.

The ingestion modules themselves remain independent Python components so they
can also be executed and tested outside Airflow.

---

## 15. Validation Status

The incremental ingestion design can be implemented independently from the
containerized platform.

Final validation will verify:

- Incremental window calculation.
- Successful API acquisition.
- Re-execution behaviour.
- Handling of overlapping periods.
- Bronze persistence.
- Integration with the final storage infrastructure.
- Compatibility with subsequent processing.

The results will be recorded in the ingestion validation documentation.