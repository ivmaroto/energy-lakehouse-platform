# Ingestion Validation and Testing

## 1. Overview

This document describes the validation and testing performed for the ingestion
layer of the Energy Lakehouse Platform.

The objective is to verify that data can be reliably acquired from the three
external sources and persisted in the Bronze layer before subsequent Lakehouse
processing.

The validated sources are:

- AEMET OpenData.
- Open-Meteo.
- REE / ESIOS.

Both historical and incremental ingestion have been validated using real API
requests.

---

## 2. Validation Scope

The ingestion validation process covers:

- Configuration loading.
- API authentication.
- HTTP connectivity.
- API response validation.
- Historical ingestion.
- Incremental ingestion.
- Historical request chunking.
- Error handling.
- Local Bronze persistence.
- MinIO Bronze persistence.
- Integration with the platform storage infrastructure.
- Unit testing.

The validation was performed progressively, starting with isolated components
and finishing with real end-to-end ingestion into MinIO.

---

## 3. Validation Levels

### 3.1 Configuration validation

The application loads configuration from environment variables defined in the
local `.env` file.

Relevant variables include:

```text
AEMET_API_KEY
ESIOS_API_KEY
MINIO_ENDPOINT
MINIO_ROOT_USER
MINIO_ROOT_PASSWORD
MINIO_BUCKET
MINIO_SECURE
```

The `.env` file is excluded from version control.

The `.env.example` file documents the required variables without containing
real credentials.

Configuration loading from `.env` was successfully validated using
`python-dotenv`.

**Status: PASSED**

---

### 3.2 Connector validation

Each source connector was tested independently against its real external API.

Validation covered:

- Request construction.
- Authentication where required.
- Parameter handling.
- HTTP communication.
- Response parsing.
- Real API connectivity.

**Status: PASSED**

---

### 3.3 Ingestion validation

Historical and incremental ingestion were validated independently for all three
data sources.

Validation covered:

- Date-range handling.
- Historical execution.
- Incremental execution.
- Historical request chunking.
- Bronze metadata generation.
- Bronze persistence.

**Status: PASSED**

---

### 3.4 Integration validation

The complete ingestion path was validated:

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
MinIO Bronze layer
```

Real data from all three sources was successfully persisted in MinIO.

**Status: PASSED**

---

## 4. AEMET Validation

AEMET OpenData was validated using real API credentials.

The station inventory endpoint was successfully queried and returned 921
stations.

The station selected for integration validation was:

```text
Station ID: 1037Y
Province: GIPUZKOA
Station: ZUMARRAGA
```

Historical validation period:

```text
2026-08-01 -> 2026-08-03
```

Incremental validation period:

```text
2026-08-10 -> 2026-08-12
```

Validation results:

| Test | Expected result | Status |
|---|---|---|
| API authentication | Valid credentials accepted | PASSED |
| HTTP connectivity | Successful connection | PASSED |
| Station inventory | Valid station catalogue retrieved | PASSED |
| Station selection | Station 1037Y retrieved | PASSED |
| Historical request | Historical data retrieved | PASSED |
| Incremental request | Incremental data retrieved | PASSED |
| Local Bronze persistence | Data stored correctly | PASSED |
| MinIO historical persistence | Bronze object created | PASSED |
| MinIO incremental persistence | Bronze object created | PASSED |

Example MinIO path:

```text
bronze/aemet/daily_climatological_values/
year=2026/month=08/day=14/
```

**AEMET validation status: PASSED**

---

## 5. Open-Meteo Validation

Open-Meteo was validated against the real service.

The coordinates used for integration testing were:

```text
Latitude: 43.0
Longitude: -2.5
```

Historical validation period:

```text
2026-08-01 -> 2026-08-03
```

The current weather endpoint was used for incremental ingestion validation.

Validation results:

| Test | Expected result | Status |
|---|---|---|
| HTTP connectivity | Successful connection | PASSED |
| Current request | Valid weather response | PASSED |
| Historical request | Historical weather data retrieved | PASSED |
| Date parameters | Requested interval respected | PASSED |
| Geographic parameters | Coordinates processed | PASSED |
| Local Bronze persistence | Data stored correctly | PASSED |
| MinIO historical persistence | Bronze object created | PASSED |
| MinIO incremental persistence | Bronze object created | PASSED |

No API credential is required for the Open-Meteo access pattern used by this
project.

Example MinIO path:

```text
bronze/open_meteo/weather/
year=2026/month=08/day=14/
```

**Open-Meteo validation status: PASSED**

---

## 6. REE / ESIOS Validation

REE / ESIOS was validated using real API credentials.

The indicator catalogue endpoint was successfully queried.

The indicator selected for integration validation was:

```text
Indicator ID: 14
Name: Generación programada PBF Solar fotovoltaica
Dataset: solar_photovoltaic_generation
```

Historical validation period:

```text
2026-08-01 -> 2026-08-03
```

Incremental validation period:

```text
2026-08-10 -> 2026-08-12
```

Validation results:

| Test | Expected result | Status |
|---|---|---|
| API authentication | Valid credentials accepted | PASSED |
| HTTP connectivity | Successful connection | PASSED |
| Indicator catalogue | Valid catalogue retrieved | PASSED |
| Indicator request | Indicator 14 response retrieved | PASSED |
| Historical request | Historical data retrieved | PASSED |
| Incremental request | Incremental data retrieved | PASSED |
| Local Bronze persistence | Data stored correctly | PASSED |
| MinIO historical persistence | Bronze object created | PASSED |
| MinIO incremental persistence | Bronze object created | PASSED |

Example MinIO path:

```text
bronze/esios/solar_photovoltaic_generation/
year=2026/month=08/day=14/
```

**REE / ESIOS validation status: PASSED**

---

## 7. Historical Ingestion Validation

Historical ingestion was successfully validated for all three data sources.

The historical ingestion implementation divides large temporal ranges into
smaller chunks before requesting data from the external APIs.

Configured chunk sizes:

```text
AEMET:       31 days
Open-Meteo:  31 days
ESIOS:       31 days
```

The validation confirmed:

- Correct start-date handling.
- Correct end-date handling.
- Date-range splitting.
- Independent processing of chunks.
- Independent Bronze persistence for every successful chunk.
- Historical metadata generation.
- Real API acquisition.

**Status: PASSED**

---

## 8. Incremental Ingestion Validation

Incremental ingestion was validated against the three real external services.

The validation confirmed that incremental temporal windows can be requested
without executing a complete historical reload.

Validated flows:

```text
Open-Meteo -> Current weather -> MinIO Bronze
AEMET      -> Temporal window -> MinIO Bronze
ESIOS      -> Temporal window -> MinIO Bronze
```

Each execution generated an independent Bronze object.

**Status: PASSED**

---

## 9. Storage Validation

### 9.1 Local Bronze storage

`LocalBronzeStorage` was validated before integration with the complete
platform.

Unit tests confirmed:

- Directory creation.
- Expected directory structure.
- JSON file creation.
- Metadata persistence.
- Unique file generation.
- Valid JSON output.

**Status: PASSED**

### 9.2 MinIO Bronze storage

`MinIOBronzeStorage` was implemented and validated against the MinIO instance
running in the Docker platform.

Bucket:

```text
energy-lakehouse
```

Bronze organization:

```text
energy-lakehouse/
└── bronze/
    ├── aemet/
    ├── open_meteo/
    └── esios/
```

A dedicated storage validation object was successfully created before
performing real API ingestion.

The final validation confirmed successful historical and incremental
persistence for AEMET, Open-Meteo and ESIOS.

**Status: PASSED**

---

## 10. Unit Tests

The ingestion layer includes automated tests implemented with `pytest`.

Test modules:

```text
tests/ingestion/test_aemet.py
tests/ingestion/test_date_utils.py
tests/ingestion/test_esios.py
tests/ingestion/test_open_meteo.py
tests/ingestion/test_storage.py
```

Final execution result:

```text
31 tests collected
31 tests passed
0 tests failed
```

Coverage by test group:

| Test group | Passed |
|---|---:|
| AEMET client | 5 |
| Date utilities | 7 |
| ESIOS client | 7 |
| Open-Meteo client | 7 |
| Bronze storage | 5 |
| **Total** | **31** |

The final test suite was executed after MinIO integration to verify that the
new storage implementation did not break the previously validated ingestion
components.

**Status: PASSED**

---

## 11. Error Handling

The ingestion layer includes controlled handling for expected failures,
including:

```text
Connection failure
Timeout
HTTP error
Invalid authentication
Malformed response
Invalid date range
Storage failure
```

Custom ingestion exceptions prevent failures from being silently accepted and
provide diagnostic information through the logging layer.

Invalid date ranges and malformed API responses are additionally covered by
automated tests.

---

## 12. Credential Security

No real API credentials are stored in the Git repository.

Versioned configuration:

```text
.env.example
Python source code
Documentation
Tests
```

Excluded configuration:

```text
.env
```

Real credentials are loaded at execution time using environment variables and
`python-dotenv`.

The following credentials were successfully validated without being embedded
in source code:

```text
AEMET_API_KEY
ESIOS_API_KEY
MINIO_ROOT_USER
MINIO_ROOT_PASSWORD
```

---

## 13. Validated End-to-End Architecture

The final validated ingestion architecture is:

```text
AEMET -----------+
                 |
Open-Meteo ------+--> Python ingestion
                 |         |
REE / ESIOS -----+         v
                       Validation
                           |
                           v
                    MinIO Object Storage
                           |
                           v
                        Bronze
```

The resulting Bronze organization is:

```text
energy-lakehouse/
└── bronze/
    ├── aemet/
    │   └── daily_climatological_values/
    ├── open_meteo/
    │   └── weather/
    └── esios/
        └── solar_photovoltaic_generation/
```

Historical and incremental executions were successfully validated for every
source.

---

## 14. Validation Evidence

Evidence generated during the validation process includes:

- Real API execution logs.
- AEMET station catalogue retrieval.
- ESIOS indicator catalogue retrieval.
- Historical execution logs.
- Incremental execution logs.
- Generated local Bronze JSON files.
- Generated MinIO Bronze objects.
- MinIO bucket structure.
- Successful unit-test execution.

These results provide technical evidence that the ingestion layer is ready to
supply data to the subsequent Lakehouse processing stages.

---

## 15. Final Validation Status

| Component | Status |
|---|---|
| Ingestion architecture | PASSED |
| Data-source architecture | PASSED |
| Python connectors | PASSED |
| AEMET real API validation | PASSED |
| Open-Meteo real API validation | PASSED |
| REE / ESIOS real API validation | PASSED |
| Historical ingestion | PASSED |
| Historical chunking | PASSED |
| Incremental ingestion | PASSED |
| Local Bronze persistence | PASSED |
| MinIO connectivity | PASSED |
| MinIO Bronze persistence | PASSED |
| Unit tests | PASSED — 31/31 |
| API credentials externalized | PASSED |
| End-to-end API -> Bronze validation | PASSED |

The ingestion layer is considered technically validated and ready for the
subsequent Lakehouse processing phase.

**Phase 3 - Data Ingestion: COMPLETED**