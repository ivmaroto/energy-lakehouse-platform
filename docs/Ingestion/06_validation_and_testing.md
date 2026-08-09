# Ingestion Validation and Testing

## 1. Overview

This document defines the validation and testing strategy for the ingestion
layer of the Energy Lakehouse Platform.

The objective is to verify that data can be reliably acquired from the three
external sources and persisted in the Bronze layer before subsequent Lakehouse
processing.

The sources covered by the validation process are:

- AEMET OpenData.
- Open-Meteo.
- REE / ESIOS.

Testing covers both historical and incremental ingestion.

---

## 2. Validation Scope

The ingestion validation process covers:

- Configuration loading.
- API authentication.
- HTTP connectivity.
- API response validation.
- Historical ingestion.
- Incremental ingestion.
- Error handling.
- Local Bronze persistence.
- Final Bronze persistence.
- Integration with the platform storage infrastructure.
- Compatibility with subsequent processing.

The objective is to validate both individual components and the complete
ingestion flow.

---

## 3. Validation Levels

Testing is divided into several levels.

### 3.1 Configuration validation

Verify that the application correctly obtains the required configuration.

Relevant environment variables include:

```text
AEMET_API_KEY
ESIOS_API_KEY
```

Credentials must be obtained from the execution environment and must not be
stored directly in source code.

---

### 3.2 Connector validation

Each source connector is tested independently.

The objective is to verify:

- Request construction.
- Authentication where required.
- Parameter handling.
- HTTP communication.
- Response parsing.
- Error detection.

---

### 3.3 Ingestion validation

Historical and incremental ingestion logic is validated independently from the
external API communication where possible.

The objective is to verify:

- Date-range handling.
- Historical execution.
- Incremental execution.
- Request-window generation.
- Re-execution behaviour.
- Data persistence.

---

### 3.4 Integration validation

The final validation verifies the complete ingestion path:

```text
External API
     |
     v
Source connector
     |
     v
Ingestion logic
     |
     v
Technical validation
     |
     v
Bronze persistence
```

This test confirms that the individual components operate correctly as a single
pipeline.

---

## 4. AEMET Validation

The AEMET connector must be validated against the real AEMET OpenData service.

The following tests are required:

| Test | Expected result | Status |
|---|---|---|
| API authentication | Valid credentials accepted | Pending |
| HTTP connectivity | Successful connection | Pending |
| Valid request | Valid API response | Pending |
| Invalid authentication | Controlled error | Pending |
| Historical request | Historical data retrieved | Pending |
| Incremental request | New data retrieved | Pending |
| Bronze persistence | Data stored correctly | Pending |

The exact datasets and endpoints used in the final implementation will be
recorded after technical validation.

---

## 5. Open-Meteo Validation

The Open-Meteo connector must be validated against the real service.

The following tests are required:

| Test | Expected result | Status |
|---|---|---|
| HTTP connectivity | Successful connection | Pending |
| Current/recent request | Valid weather response | Pending |
| Historical request | Historical weather data retrieved | Pending |
| Date parameters | Requested interval respected | Pending |
| Geographic parameters | Requested coordinates processed | Pending |
| Invalid request | Controlled error | Pending |
| Bronze persistence | Data stored correctly | Pending |

No API credential is required for the Open-Meteo access pattern used by this
project.

---

## 6. REE / ESIOS Validation

The REE / ESIOS connector must be validated using the credentials provided for
the API.

The following tests are required:

| Test | Expected result | Status |
|---|---|---|
| API authentication | Valid credentials accepted | Pending |
| HTTP connectivity | Successful connection | Pending |
| Indicator request | Valid energy response | Pending |
| Historical request | Historical data retrieved | Pending |
| Incremental request | New data retrieved | Pending |
| Invalid authentication | Controlled error | Pending |
| Bronze persistence | Data stored correctly | Pending |

The definitive indicators and parameters used by the platform will be recorded
after testing the real API.

---

## 7. Historical Ingestion Tests

Historical ingestion must be validated using controlled date ranges before
executing the complete initial load.

The test sequence is:

```text
Small historical interval
          |
          v
Validate response
          |
          v
Validate Bronze output
          |
          v
Larger historical interval
          |
          v
Complete historical load
```

This progressive approach reduces the impact of implementation errors during
large historical executions.

Tests must verify:

- Correct start date.
- Correct end date.
- Request chunking where required.
- Successful acquisition of each interval.
- Correct Bronze persistence.
- Controlled recovery from failed intervals.

---

## 8. Incremental Ingestion Tests

Incremental ingestion testing must verify that only the required temporal
windows are requested.

Tests must include:

- First incremental execution.
- Consecutive incremental execution.
- Re-execution of the same period.
- Overlapping temporal windows where configured.
- Execution when no new data is available.
- Recovery after a failed execution.

The process must not require a complete historical reload after an incremental
failure.

---

## 9. Storage Tests

### 9.1 Local storage

The local persistence implementation is used to validate ingestion components
independently from the complete platform infrastructure.

Tests include:

- Directory creation.
- Source separation.
- Dataset separation.
- File creation.
- Temporal organization.
- Metadata persistence.
- Re-execution behaviour.

### 9.2 Platform storage

Final integration testing validates persistence using the storage infrastructure
of the complete platform.

Tests include:

- Storage connectivity.
- Object creation.
- Correct Bronze paths.
- Historical persistence.
- Incremental persistence.
- Access from the processing environment.

---

## 10. Error Handling Tests

The ingestion layer must handle expected failures in a controlled way.

Relevant scenarios include:

```text
Connection failure
Timeout
HTTP error
Invalid authentication
Malformed response
Empty response
Invalid date range
Storage failure
```

The expected behaviour is:

1. Detect the failure.
2. Prevent invalid data from being silently accepted.
3. Record useful diagnostic information.
4. Return a controlled application error.
5. Allow the failed execution to be retried.

---

## 11. Credential Security

No real API credential must be committed to the Git repository.

The following files may be versioned:

```text
.env.example
Python source code
Documentation
Tests
```

The following file must remain outside version control:

```text
.env
```

Source code must reference configuration variables rather than containing real
credentials.

Before each relevant Git commit, the repository should be checked to ensure that
no API credentials have accidentally been introduced.

---

## 12. Test Execution Environments

The ingestion architecture supports testing at different levels depending on the
available development environment.

### Python development environment

The following elements can be developed and validated independently:

- Python module structure.
- Configuration handling.
- Connector implementation.
- Request construction.
- Date-window logic.
- Response validation logic.
- Local persistence.
- Unit tests.

### Complete platform environment

The following tests require the complete platform infrastructure:

- Final object-storage integration.
- MinIO persistence.
- Container-to-container connectivity.
- Reading Bronze data from Spark.
- Complete platform integration.
- End-to-end execution in the deployment environment.

This separation allows ingestion development to progress independently from
infrastructure availability.

---

## 13. End-to-End Validation

The final ingestion validation will execute the complete flow for each source.

```text
AEMET -----------+
                 |
Open-Meteo ------+--> Ingestion --> Bronze --> Processing validation
                 |
REE / ESIOS -----+
```

The end-to-end test will verify:

1. Configuration loading.
2. Authentication.
3. External API connectivity.
4. Data acquisition.
5. Technical validation.
6. Bronze persistence.
7. Availability of the data to the processing layer.

---

## 14. Validation Evidence

Evidence from the final tests should be recorded for inclusion in the technical
documentation and TFM report.

Relevant evidence may include:

- Execution logs.
- Number of acquired records.
- Requested temporal ranges.
- Execution duration.
- Generated Bronze objects.
- Storage screenshots.
- Successful processing reads.
- Error-handling examples.

This evidence will also support the final evaluation metrics of the project.

---

## 15. Current Validation Status

The ingestion architecture and testing strategy have been defined.

The following items remain subject to execution and technical validation:

| Component | Status |
|---|---|
| Ingestion architecture | Defined |
| Data-source architecture | Defined |
| Historical ingestion design | Defined |
| Incremental ingestion design | Defined |
| Bronze storage design | Defined |
| Python connectors | Pending implementation/validation |
| AEMET real API validation | Pending |
| Open-Meteo real API validation | Pending |
| REE / ESIOS real API validation | Pending |
| Historical load validation | Pending |
| Incremental load validation | Pending |
| Local Bronze validation | Pending |
| MinIO integration | Pending |
| Spark Bronze read validation | Pending |
| End-to-end validation | Pending |

This table will be updated as implementation and integration tests are
completed.