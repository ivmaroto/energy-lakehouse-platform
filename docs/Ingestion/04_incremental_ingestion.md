# Incremental Ingestion

## 1. Overview

After the initial historical load, the Energy Lakehouse Platform uses
incremental ingestion processes to keep meteorological and energy datasets
updated.

Incremental ingestion retrieves the temporal window required by each dataset
according to its publication frequency instead of repeatedly downloading the
complete historical dataset.

The process is implemented independently for:

- AEMET OpenData.
- Open-Meteo.
- REE / ESIOS.

Incremental ingestion is orchestrated through Apache Airflow and persists the
acquired source data in the Bronze layer hosted in MinIO.

---

## 2. Objectives

The incremental ingestion process has the following objectives:

- Retrieve newly available data from each external source.
- Minimize unnecessary API requests.
- Avoid repeatedly downloading complete historical datasets.
- Preserve source information in the Bronze layer.
- Support controlled re-execution of temporal windows.
- Detect and handle invalid requests and API failures.
- Support different ingestion frequencies according to dataset characteristics.
- Provide traceable source data for subsequent Silver processing.

---

## 3. General Process

The implemented incremental ingestion workflow follows this general pattern:

```text
Airflow schedule
      |
      v
+----------------------+
| Determine execution  |
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
|      in MinIO        |
+----------+-----------+
           |
           v
+----------------------+
| Airflow task status  |
+----------------------+
```

Each dataset is processed independently because publication frequency,
temporal granularity and data availability differ between providers.

---

## 4. Incremental Frequencies

The implemented ingestion strategy supports several execution frequencies:

| Frequency | Main use |
|---|---|
| 5 minutes | High-frequency ESIOS energy indicators |
| 15 minutes | Open-Meteo meteorological observations |
| Hourly | AEMET observations and selected ESIOS indicators |
| Daily | AEMET daily datasets and radiation data |
| Monthly | ESIOS installed-capacity datasets |

The Airflow orchestration layer separates these workloads according to their
required execution frequency.

The main implemented DAGs are:

```text
open_meteo_15min
hourly_ingestion
daily_ingestion
monthly_ingestion
```

This separation prevents datasets with different publication characteristics
from being unnecessarily requested at the same frequency.

---

## 5. Temporal Windows

Incremental ingestion uses explicit temporal windows.

For high-frequency datasets, the ingestion layer supports exact UTC
`datetime` boundaries rather than only calendar dates.

Example:

```text
ESIOS 5-minute window

2025-08-13 00:00 UTC
        |
        +--------------------+
                             |
                     2025-08-13 00:05 UTC
```

Open-Meteo 15-minute ingestion similarly supports exact temporal boundaries.

Example:

```text
2026-08-13 10:00 UTC
        |
        +--------------------+
                             |
                     2026-08-13 10:15 UTC
```

Datetime values supplied with another timezone are normalized to UTC before
building the corresponding API request where required.

Daily and monthly datasets use larger temporal windows appropriate to their
publication frequency.

---

## 6. AEMET Incremental Ingestion

AEMET incremental ingestion currently covers several types of meteorological
information.

### Conventional observations

Current conventional observations are retrieved periodically and persisted in:

```text
bronze/aemet/current_observations/
```

This dataset contains meteorological variables such as:

- Temperature.
- Relative humidity.
- Precipitation.
- Wind speed and direction.
- Atmospheric pressure.

### Daily climatological values

Daily climatological values are retrieved for configured AEMET stations.

The list of stations used by the Airflow DAG is externalized through the
Airflow variable:

```text
AEMET_DAILY_STATIONS
```

This avoids embedding the definitive station configuration directly in the
DAG source code.

The resulting data is persisted in:

```text
bronze/aemet/daily_climatological_values/
```

### Radiation

AEMET radiation data is acquired as source text/CSV information and preserved
in Bronze without applying analytical transformations.

The resulting files are persisted in:

```text
bronze/aemet/radiation/
```

The radiation parser is tested independently and will be used by subsequent
processing stages.

---

## 7. Open-Meteo Incremental Ingestion

Open-Meteo provides the high-frequency meteorological dataset used by the
platform.

The implemented 15-minute ingestion supports exact datetime windows and
retrieves the meteorological variables required by the analytical use case.

The validated dataset includes variables such as:

- Temperature.
- Relative humidity.
- Dew point.
- Precipitation.
- Cloud cover.
- Atmospheric pressure.
- Wind speed and direction at 10 m.
- Wind gusts.
- Wind speed and direction at 80 m.
- Wind speed and direction at 120 m.
- Shortwave radiation.
- Direct radiation.
- Diffuse radiation.
- Direct normal irradiance.
- Sunshine duration.

The resulting dataset is persisted in:

```text
bronze/open_meteo/weather_15min/
```

A full-day validation returned 96 observations, corresponding to one record
every 15 minutes.

Open-Meteo access does not require an API credential for the access pattern
used by this project.

---

## 8. REE / ESIOS Incremental Ingestion

REE / ESIOS incremental ingestion retrieves energy information using selected
indicator identifiers.

The ingestion layer supports both calendar-date windows and exact UTC datetime
windows.

This allows the same connector to support datasets with different temporal
granularities.

A validated example is indicator `1293`, used during the 5-minute incremental
validation.

A complete day returned 288 observations:

```text
24 hours * 12 observations/hour = 288 observations
```

The same ingestion implementation is also used for hourly and monthly ESIOS
datasets by supplying the corresponding temporal window and dataset
configuration.

---

## 9. Airflow Orchestration

Incremental ingestion is integrated with Apache Airflow.

Airflow is responsible for:

- Scheduling ingestion tasks.
- Separating workloads by execution frequency.
- Executing ingestion code inside the containerized environment.
- Recording task execution status.
- Supporting retries and operational monitoring.

The ingestion modules remain independent Python components and can therefore
also be executed and tested outside Airflow.

The integration has been validated from inside the Airflow scheduler container,
including a complete execution path:

```text
Airflow container
      |
      v
Python ingestion module
      |
      v
External API
      |
      v
MinIO
      |
      v
Bronze object
```

---

## 10. Bronze Append-Only Strategy

The Bronze layer uses an append-only strategy.

Each successful ingestion execution creates a new object containing:

- Source data.
- Source identifier.
- Dataset identifier.
- Ingestion mode.
- Ingestion timestamp.
- Requested temporal boundaries when applicable.

Object names contain the ingestion timestamp, preserving the traceability of
each acquisition.

For example:

```text
bronze/
`-- esios/
    `-- demand_real_5min/
        `-- year=2026/
            `-- month=08/
                `-- day=15/
                    `-- esios_demand_real_5min_<timestamp>.json
```

The partition path represents the ingestion date.

The requested source-data period is preserved separately in the Bronze
metadata.

---

## 11. Re-execution and Idempotency

Bronze is intentionally not physically idempotent.

If exactly the same temporal window is executed twice, both acquisitions are
preserved as separate Bronze objects because each execution has its own
ingestion timestamp.

This behaviour was explicitly validated using the same ESIOS indicator,
dataset and 5-minute temporal window in two consecutive executions.

The validation confirmed:

```text
Same requested window: True
Same source data:      True
Different Bronze objects
```

This behaviour is consistent with the Bronze layer objective of preserving raw
source acquisitions and maintaining ingestion traceability.

A retry therefore does not overwrite or corrupt an existing Bronze object.

---

## 12. Duplicate Handling

Because Bronze is append-only, repeated executions may produce duplicated
business observations across different Bronze objects.

These duplicates are intentionally preserved in Bronze.

Definitive duplicate detection and deduplication will be performed during
Silver processing using appropriate business and temporal keys for each
dataset.

Conceptually:

```text
Bronze
  |
  |-- acquisition A ----\
  |                      +----> Silver transformation
  |-- acquisition B ----/            |
                                    deduplication
                                        |
                                        v
                               canonical records
```

This separates source traceability from analytical data quality.

---

## 13. Error Handling

Invalid temporal ranges are rejected before Bronze persistence.

For example, an ESIOS request where:

```text
start_datetime > end_datetime
```

raises:

```text
InvalidDateRangeError
```

This behaviour was validated during Phase 3.

Other failures handled by the common ingestion architecture include:

- Network connectivity errors.
- HTTP errors.
- Authentication failures.
- Request timeouts.
- Temporary API unavailability.
- Invalid API responses.
- Missing expected response information.

Failures are recorded through the common logging mechanism.

A failed execution does not modify previously persisted Bronze data.

---

## 14. Technical Validation

The connectors perform technical validation before or during acquisition.

This includes, where applicable:

- Valid temporal ranges.
- Valid coordinates.
- Valid indicator identifiers.
- Successful HTTP responses.
- Expected API response structures.
- Presence of the required AEMET dataset URL.
- Valid source payloads.

Business-level quality rules and cross-source consistency checks belong to
later Lakehouse processing stages.

---

## 15. Bronze Persistence

Incremental data from all three sources is persisted in MinIO under the common
Bronze hierarchy:

```text
bronze/
|
|-- aemet/
|   |-- current_observations/
|   |-- daily_climatological_values/
|   `-- radiation/
|
|-- open_meteo/
|   `-- weather_15min/
|
`-- esios/
    |-- demand_real_5min/
    |-- generacion_medida_eolica_terrestre/
    |-- potencia_instalada_eolica/
    `-- ...
```

JSON datasets are wrapped with ingestion metadata and source data.

AEMET radiation source text is persisted as CSV/raw text in order to preserve
the original acquired representation.

---

## 16. Historical and Incremental Relationship

Historical and incremental ingestion reuse the same source-specific connector
architecture.

```text
                  +------------------+
Historical ------>|                  |
                  | Source Connector |------> Bronze / MinIO
Incremental ----->|                  |
                  +------------------+
```

The principal difference is the temporal range and execution strategy supplied
to each acquisition.

This avoids maintaining duplicated API implementations for historical and
incremental processing.

---

## 17. Validation Results

Phase 3 validation confirmed the following:

- Exact ESIOS datetime windows can be requested successfully.
- Open-Meteo 15-minute windows can be requested successfully.
- Datetimes are normalized to UTC where required.
- AEMET conventional observations can be ingested successfully.
- AEMET daily climatological data can be ingested successfully.
- AEMET radiation data can be persisted in raw CSV form.
- Incremental workloads execute successfully through Airflow.
- Airflow containerized ingestion can persist directly to MinIO.
- Bronze objects contain ingestion metadata and source payloads.
- Re-execution preserves both acquisitions in Bronze.
- Duplicate business observations are deferred to Silver deduplication.
- Invalid temporal ranges are rejected before persistence.

The incremental ingestion layer is therefore considered technically validated
for Phase 3.