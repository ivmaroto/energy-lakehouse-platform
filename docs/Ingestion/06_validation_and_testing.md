# Ingestion Validation and Testing

## 1. Overview

This document records the validation and testing performed for the ingestion
layer of the Energy Lakehouse Platform.

The objective is to verify that data can be reliably acquired from the three
external sources, orchestrated where applicable through Apache Airflow and
persisted in the Bronze layer in MinIO before subsequent Lakehouse processing.

The validated sources are:

- AEMET OpenData.
- Open-Meteo.
- REE / ESIOS.

Historical, incremental and frequency-specific ingestion scenarios have been
tested using both automated tests and real API requests.

---

## 2. Validation Scope

Phase 3 validation covers:

- Configuration loading.
- Credential externalization.
- API authentication.
- HTTP connectivity.
- API response validation.
- Historical ingestion.
- Incremental ingestion.
- Exact datetime windows.
- Multiple ingestion frequencies.
- Historical request chunking.
- Error handling.
- Local Bronze persistence.
- MinIO Bronze persistence.
- Airflow ingestion execution.
- Re-execution behaviour.
- Duplicate preservation in Bronze.
- Unit testing.
- Real API integration testing.

Validation was performed progressively, from isolated components to real
end-to-end executions.

---

## 3. Configuration and Security Validation

Application configuration is loaded from environment variables.

Relevant configuration includes:

```text
AEMET_API_KEY
ESIOS_API_KEY
MINIO_ENDPOINT
MINIO_ROOT_USER
MINIO_ROOT_PASSWORD
MINIO_BUCKET
MINIO_SECURE
```

The local `.env` file is excluded from version control.

`.env.example` documents the required configuration without exposing real
credentials.

Runtime configuration used by Airflow is also externalized where appropriate.

For example, the configured AEMET daily stations are stored using the Airflow
variable:

```text
AEMET_DAILY_STATIONS
```

This prevents the definitive station configuration from being embedded
directly in the DAG source code.

**Status: PASSED**

---

## 4. Connector Validation

Each source connector was validated independently.

Validation covered:

- Request construction.
- Authentication where required.
- Parameter handling.
- Date and datetime handling.
- UTC normalization.
- HTTP communication.
- Response parsing.
- Error handling.
- Real API connectivity.

**Status: PASSED**

---

## 5. AEMET Validation

AEMET OpenData was validated using real API credentials.

Validated acquisition types include:

```text
Station catalogue
Daily climatological values
Conventional observations
Radiation data
```

Real conventional observations were successfully retrieved and persisted.

One validated Bronze object contained:

```text
9760 records
```

A sample observation included source fields for:

```text
station identifier
coordinates
temperature
relative humidity
precipitation
wind speed
wind direction
atmospheric pressure
```

Daily climatological ingestion was also validated using station:

```text
B013X
```

A real historical request for this station successfully returned data and was
persisted in MinIO.

Radiation ingestion was validated independently and successfully persisted the
source dataset as raw CSV.

Validated Bronze paths include:

```text
bronze/aemet/current_observations/
bronze/aemet/daily_climatological_values/
bronze/aemet/radiation/
bronze/aemet/stations/
```

AEMET ingestion was also executed successfully from inside the Airflow
scheduler container.

**AEMET validation status: PASSED**

---

## 6. Open-Meteo Validation

Open-Meteo was validated against the real external service.

Validated acquisition modes include:

```text
Current weather
Historical weather
Historical forecast data
Hourly weather
15-minute weather
```

The 15-minute ingestion implementation supports exact datetime windows.

A complete daily validation for:

```text
2026-08-13
```

returned:

```text
96 observations
```

which corresponds to:

```text
24 hours * 4 observations/hour
```

The validated payload contained variables including:

```text
temperature_2m
relative_humidity_2m
dew_point_2m
precipitation
cloud_cover
pressure_msl
surface_pressure
wind_speed_10m
wind_direction_10m
wind_gusts_10m
wind_speed_80m
wind_direction_80m
wind_speed_120m
wind_direction_120m
shortwave_radiation
direct_radiation
diffuse_radiation
direct_normal_irradiance
sunshine_duration
```

Validated Bronze paths include:

```text
bronze/open_meteo/weather/
bronze/open_meteo/weather_hourly/
bronze/open_meteo/weather_15min/
bronze/open_meteo/weather_historical_forecast/
```

No API credential is required for the Open-Meteo access pattern used by this
project.

**Open-Meteo validation status: PASSED**

---

## 7. REE / ESIOS Validation

REE / ESIOS was validated using real API credentials.

The complete indicator catalogue was retrieved during source analysis.

The ingestion implementation supports:

- Date-based requests.
- Exact datetime windows.
- Optional temporal aggregation parameters.
- Optional geographical parameters.
- Multiple datasets and indicators.

A high-frequency validation was performed using:

```text
Indicator ID: 1293
Dataset: demand_real_5min
```

A complete day for:

```text
2025-08-13
```

returned:

```text
288 values
```

corresponding to:

```text
24 hours * 12 observations/hour
```

The first validated value contained:

```text
value
datetime
datetime_utc
tz_time
geo_id
geo_name
```

with geography:

```text
Península
```

Additional ESIOS datasets successfully persisted in Bronze include:

```text
demanda_real
generacion_medida_eolica_terrestre
potencia_instalada_eolica
solar_photovoltaic_generation
```

**REE / ESIOS validation status: PASSED**

---

## 8. Historical Ingestion Validation

Historical ingestion was validated for the three source connectors.

The historical ingestion architecture supports splitting large temporal ranges
into smaller request windows.

Validation confirmed:

- Start-date handling.
- End-date handling.
- Date-range validation.
- Historical request execution.
- Chunk-based processing.
- Independent Bronze persistence.
- Historical metadata generation.
- Real API acquisition.

**Status: PASSED**

---

## 9. Incremental Ingestion Validation

Incremental ingestion was validated using real APIs and automated tests.

The implementation supports different temporal granularities depending on the
dataset.

Validated execution frequencies include:

```text
5 minutes
15 minutes
Hourly
Daily
Monthly
```

Frequency-specific Airflow workloads include:

```text
open_meteo_15min
hourly_ingestion
daily_ingestion
monthly_ingestion
```

Exact datetime windows were validated for high-frequency ESIOS and Open-Meteo
ingestion.

Daily AEMET workloads and monthly ESIOS installed-capacity workloads were also
validated through Airflow task execution.

**Status: PASSED**

---

## 10. Airflow Validation

Apache Airflow was integrated with the ingestion layer.

The scheduler and webserver were confirmed running in the Docker environment.

Airflow successfully discovered ingestion DAGs and their tasks.

Examples of validated task execution include:

```text
daily_ingestion.aemet_radiation
daily_ingestion.climatology_B013X
monthly_ingestion.esios_1485
```

The AEMET climatological task was validated using a historical execution date
for which source data was available.

The complete validated execution path is:

```text
Airflow scheduler
       |
       v
Python ingestion module
       |
       v
External API
       |
       v
Common storage component
       |
       v
MinIO
       |
       v
Bronze object
```

A direct ingestion execution from inside the Airflow scheduler container also
successfully created a real AEMET Bronze object.

**Status: PASSED**

---

## 11. Local Bronze Storage Validation

Local Bronze persistence was validated through automated tests.

Validation covers:

- Directory creation.
- Expected hierarchy.
- JSON persistence.
- Metadata persistence.
- Unique file generation.
- Valid serialized output.

The storage abstraction remains independent from the source connectors.

**Status: PASSED**

---

## 12. MinIO Bronze Storage Validation

MinIO is the S3-compatible object storage backend used by the platform.

Configured bucket:

```text
energy-lakehouse
```

Bronze objects are stored below:

```text
energy-lakehouse/
`-- bronze/
```

Direct MinIO validation using the Python client successfully:

- Connected using configured credentials.
- Enumerated Bronze objects.
- Read persisted objects.
- Deserialized JSON payloads.
- Inspected ingestion metadata.
- Inspected source records.

During validation, 30 Bronze objects were enumerated at the inspected point in
time.

This number represents the runtime state during that validation and is not a
fixed platform requirement.

**Status: PASSED**

---

## 13. Bronze Metadata Validation

Real persisted JSON objects were inspected directly.

The validated metadata structure contains:

```text
source
dataset
ingestion_mode
ingestion_timestamp
requested_start_date
requested_end_date
```

Example behaviour:

- AEMET current observations may contain `null` requested date boundaries.
- ESIOS historical/incremental requests preserve their requested temporal
  window.
- Open-Meteo 15-minute ingestion preserves exact datetime boundaries.

The physical `year/month/day` object partition represents the ingestion date,
while the requested source-data period is retained in metadata.

**Status: PASSED**

---

## 14. Re-execution and Idempotency Validation

Re-execution behaviour was explicitly tested using ESIOS.

The same request was executed twice using:

```text
Indicator: 1293
Dataset: validation_idempotency
Start: 2025-08-13 00:00 UTC
End:   2025-08-13 00:05 UTC
```

Both executions created separate timestamped Bronze objects.

Comparison of the persisted results confirmed:

```text
SAME WINDOW: True
SAME DATA:   True
```

This validates the intended Bronze append-only strategy.

Bronze therefore preserves both acquisitions instead of overwriting the first
execution.

Physical idempotency is intentionally not enforced at Bronze level.

Business-level deduplication will be performed during Silver processing.

**Status: PASSED**

---

## 15. Duplicate Handling Validation

Repeated ingestion of the same temporal window can create duplicated business
observations across different Bronze objects.

This is expected behaviour.

The validation confirmed that repeated acquisitions:

- Do not overwrite existing objects.
- Do not corrupt previous acquisitions.
- Preserve the requested temporal window.
- Preserve equivalent source data.
- Remain independently traceable through ingestion timestamps.

Definitive duplicate removal is delegated to Silver.

**Status: PASSED**

---

## 16. Error Handling Validation

Controlled error handling was validated.

An invalid ESIOS temporal window was explicitly executed with:

```text
start_datetime:
2025-08-13 00:05 UTC

end_datetime:
2025-08-13 00:00 UTC
```

The ingestion correctly raised:

```text
InvalidDateRangeError
```

No successful Bronze persistence occurred for the invalid request.

Automated tests additionally cover invalid parameters and malformed API
responses.

Expected error categories handled by the ingestion architecture include:

```text
Connection failure
Timeout
HTTP error
Invalid authentication
Malformed response
Invalid date range
Storage failure
```

**Status: PASSED**

---

## 17. Unit Tests

The ingestion layer includes automated tests implemented with `pytest`.

Test modules include:

```text
tests/ingestion/test_aemet.py
tests/ingestion/test_date_utils.py
tests/ingestion/test_esios.py
tests/ingestion/test_open_meteo.py
tests/ingestion/test_storage.py
```

The expanded source-specific test suites were executed independently.

Validated results:

```text
ESIOS:
11 passed

AEMET:
11 passed

Open-Meteo:
12 passed
```

These tests include coverage for newly implemented functionality such as:

- Exact datetime windows.
- UTC normalization.
- 15-minute Open-Meteo requests.
- ESIOS high-frequency ingestion.
- AEMET conventional observations.
- AEMET radiation retrieval.
- AEMET radiation parsing.
- Raw radiation persistence.
- Invalid temporal ranges.

A final complete regression execution of the entire test suite is performed as
part of the final Phase 3 validation procedure.

---

## 18. Docker Integration Validation

The containerized platform was inspected during Phase 3 validation.

Running services included:

```text
Airflow scheduler
Airflow webserver
MinIO
PostgreSQL
Spark master
Spark worker
Superset
Trino
```

The ingestion components successfully communicated with MinIO from the
containerized Airflow environment.

Trino health belongs to the Lakehouse/query-engine infrastructure validation
and is handled separately from the ingestion-layer acceptance criteria.

**Status: PASSED for ingestion integration**

---

## 19. End-to-End Validation

The final validated ingestion path is:

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

For orchestrated workloads:

```text
Airflow
   |
   v
Python ingestion
   |
   v
External source
   |
   v
MinIO Bronze
```

Real source data from all three providers has successfully completed this
pipeline.

**Status: PASSED**

---

## 20. Validation Evidence

Technical evidence generated during Phase 3 includes:

- Automated pytest results.
- Real API execution logs.
- AEMET real observation retrieval.
- AEMET daily climatological retrieval.
- AEMET radiation retrieval.
- Open-Meteo 15-minute retrieval.
- ESIOS 5-minute retrieval.
- Airflow task execution logs.
- MinIO object enumeration.
- Direct inspection of persisted Bronze JSON.
- Bronze metadata inspection.
- Re-execution comparison.
- Invalid temporal-window exception validation.
- Docker service validation.

These results provide technical evidence that the ingestion layer can supply
real data to subsequent Lakehouse processing stages.

---

## 21. Validation Status

| Component | Status |
|---|---|
| Configuration loading | PASSED |
| Credential externalization | PASSED |
| Python connectors | PASSED |
| AEMET real API | PASSED |
| Open-Meteo real API | PASSED |
| REE / ESIOS real API | PASSED |
| Historical ingestion | PASSED |
| Incremental ingestion | PASSED |
| Exact datetime windows | PASSED |
| 5-minute strategy | PASSED |
| 15-minute strategy | PASSED |
| Hourly strategy | PASSED |
| Daily strategy | PASSED |
| Monthly strategy | PASSED |
| Local Bronze persistence | PASSED |
| MinIO connectivity | PASSED |
| MinIO Bronze persistence | PASSED |
| Airflow integration | PASSED |
| Append-only re-execution | PASSED |
| Duplicate preservation | PASSED |
| Invalid-range handling | PASSED |
| Expanded source tests | PASSED |
| API -> Bronze integration | PASSED |

Phase 3 ingestion functionality is technically implemented and validated.

The final complete regression execution and repository verification are
performed before formally closing the phase.