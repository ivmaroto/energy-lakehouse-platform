# Incremental Ingestion

## 1. Overview

After the initial historical population, the Energy Lakehouse Platform uses
source-specific recurrent ingestion processes to capture newly available data.

Incremental ingestion does not rebuild the complete historical Lakehouse.

Its current responsibility is to acquire recent source data and persist it in
the Bronze layer hosted in MinIO.

The final Airflow runtime separates historical end-to-end processing from
recurrent Bronze acquisition.

The relevant DAG roles are:

```text
historical_reload
→ historical Bronze → Silver → Gold

hourly_ingestion
→ recurrent hourly Bronze ingestion

monthly_ingestion
→ recurrent monthly Bronze ingestion

open_meteo_15min
→ manual historical Open-Meteo 15-minute Bronze utility
```

Therefore, the recurrent hourly and monthly DAGs do **not** independently
execute Silver or Gold promotion.

---

## 2. Objectives

The incremental ingestion processes have the following objectives:

- retrieve newly available source data;
- avoid repeatedly downloading the complete historical dataset;
- preserve source availability exactly as published;
- persist recent acquisitions in canonical Bronze objects;
- merge repeated acquisitions into the appropriate observation-time object;
- avoid logical duplication inside canonical Bronze objects;
- preserve audit metadata;
- provide reusable Bronze input for later Silver processing;
- separate source acquisition from downstream analytical transformation.

Incremental ingestion does not perform:

- Silver normalization;
- Gold aggregation;
- cross-source joins;
- analytical source fallback;
- KPI calculation.

---

## 3. Current Airflow Workloads

The current runtime contains exactly four DAGs:

```text
historical_reload
hourly_ingestion
monthly_ingestion
open_meteo_15min
```

Only three are relevant to recurrent or auxiliary source acquisition.

### `hourly_ingestion`

Validated role:

```text
recurrent hourly Bronze ingestion
```

Configured schedule:

```text
0 * * * *
```

Its current Bronze responsibilities include:

```text
AEMET current observations
ESIOS hourly generation indicators
Open-Meteo hourly observations
```

### `monthly_ingestion`

Validated role:

```text
recurrent monthly Bronze ingestion
```

Configured schedule:

```text
@monthly
```

Its current Bronze responsibility is:

```text
ESIOS monthly installed-capacity indicators
```

### `open_meteo_15min`

Validated role:

```text
manual historical Open-Meteo 15-minute Bronze utility
```

Configured schedule:

```text
None
```

It is not an automatically scheduled 15-minute incremental pipeline.

The final Airflow runtime therefore does **not** contain active:

```text
esios_5min
daily_ingestion
```

workloads as part of the current project scope.

---

## 4. Current Incremental Frequencies

The final recurrent acquisition strategy is:

| Frequency | DAG | Current Bronze role |
|---|---|---|
| Hourly | `hourly_ingestion` | AEMET current, ESIOS hourly, Open-Meteo hourly |
| Monthly | `monthly_ingestion` | ESIOS installed capacity |
| Manual | `open_meteo_15min` | Historical Open-Meteo 15-minute acquisition |

The previous development scope involving:

```text
5-minute ESIOS
scheduled 15-minute Open-Meteo
daily AEMET climatology
AEMET radiation
```

is not part of the final incremental Airflow design.

---

## 5. Observation-Time Bronze Storage

The final Bronze model does not organize analytical time-series data by
ingestion date.

Physical storage is governed by source observation time.

`ingestion_timestamp` remains audit metadata.

The canonical paths are:

### AEMET current observations

```text
bronze/aemet/current_observations/
year=YYYY/month=MM/day=DD/
observations.json
```

### Open-Meteo hourly

```text
bronze/open_meteo/weather_hourly/
year=YYYY/month=MM/day=DD/
station_id=<station_id>.json
```

### Open-Meteo 15-minute

```text
bronze/open_meteo/weather_15min/
year=YYYY/month=MM/day=DD/
station_id=<station_id>.json
```

### ESIOS hourly

```text
bronze/esios/<dataset>/
year=YYYY/month=MM/day=DD/
data.json
```

### ESIOS monthly

```text
bronze/esios/<dataset>/
year=YYYY/month=MM/
data.json
```

This model keeps physical Bronze organization aligned with the business time of
the source observations.

---

## 6. AEMET Incremental Ingestion

The final incremental AEMET scope contains:

```text
current_observations
```

The station catalogue remains a reference master rather than an hourly
observation dataset.

AEMET current observations provide recent official meteorological measurements.

They are persisted by UTC observation day under:

```text
bronze/aemet/current_observations/
year=YYYY/month=MM/day=DD/
observations.json
```

When the canonical daily object already exists, newly acquired observations are
merged with the existing object.

Logical deduplication uses:

```text
idema
fint
```

as the observation identity.

Therefore, repeated acquisition of the same AEMET observation does not require
creating a second business observation inside the canonical daily object.

AEMET current observations are not used to reconstruct arbitrary historical
periods.

---

## 7. Open-Meteo Hourly Incremental Ingestion

The recurrent Open-Meteo workload uses the hourly dataset:

```text
weather_hourly
```

for the validated AEMET point catalogue.

The current point catalogue contains:

```text
926 locations
```

Incremental hourly acquisition uses the Open-Meteo current/forecast service
appropriate to recent data acquisition.

Runtime access configuration for the configured Open-Meteo service plan is
externalized from source code.

No real credential belongs in this document.

### Canonical daily object

Each station/day is persisted as:

```text
bronze/open_meteo/weather_hourly/
year=YYYY/month=MM/day=DD/
station_id=<station_id>.json
```

When additional observations for the same station/day are acquired, the daily
object is merged by observation time.

The final object therefore represents the canonical source state for that
station and UTC day rather than a sequence of timestamp-named ingestion files.

---

## 8. Open-Meteo Daily Completeness

Historical and incremental Open-Meteo logic must not assume that an existing
object is complete.

For a complete UTC day:

```text
hourly
→ 24 timestamps
```

For the historical 15-minute dataset:

```text
15-minute
→ 96 timestamps
```

Object existence alone is not sufficient.

A partial daily object remains incomplete and can be reloaded or completed.

This daily completeness rule replaces the earlier design that inferred
completion from a separate Bronze-state concept.

---

## 9. Open-Meteo 15-Minute Role

The dataset:

```text
weather_15min
```

remains part of the final Bronze and Silver models because Gold uses it for
elevated-wind analysis.

However, the final Airflow DAG:

```text
open_meteo_15min
```

is:

```text
manual
historical
Bronze-only
```

and has:

```text
schedule = None
```

It must not be described as an automatically scheduled 15-minute incremental
pipeline.

Historical 15-minute data is persisted as one canonical daily object per
station and requires:

```text
96 timestamps
```

for a complete UTC day.

---

## 10. ESIOS Hourly Incremental Ingestion

The recurrent hourly ESIOS workload uses the final configured catalogue of:

```text
11 hourly generation indicators
```

from:

```text
config/esios_indicators.json
```

The resulting source data is persisted by observation day:

```text
bronze/esios/<dataset>/
year=YYYY/month=MM/day=DD/
data.json
```

When the daily object already exists, new source observations are merged into
the canonical object.

The validated logical identity for ESIOS observations is based on:

```text
geo_id
datetime_utc
```

within the corresponding configured dataset.

Repeated retrieval of the same source observation therefore does not require a
second logical record inside the canonical daily Bronze object.

---

## 11. ESIOS Monthly Incremental Ingestion

Monthly installed-capacity acquisition is coordinated by:

```text
monthly_ingestion
```

using the final configured catalogue of:

```text
9 monthly installed-capacity indicators
```

The canonical Bronze path is:

```text
bronze/esios/<dataset>/
year=YYYY/month=MM/
data.json
```

Installed-capacity observations remain monthly source data.

They are not converted to hourly records during ingestion.

Their downstream Gold grain remains:

```text
Autonomous Community × month
```

---

## 12. ESIOS `NO_DATA` Semantics

A successful ESIOS HTTP response can legitimately contain:

```text
values = []
```

The final implementation handles this as:

```text
NO_DATA
```

rather than as an ingestion failure.

A valid empty response does not create:

```text
synthetic zero values
synthetic timestamps
synthetic source observations
```

This preserves the distinction between:

```text
no published observation
```

and:

```text
published value = 0
```

Source availability is therefore preserved exactly rather than manufactured by
the ingestion layer.

---

## 13. Re-execution and Canonical Merge Behaviour

The final Bronze model is not based on a universal append-only rule.

For active time-series datasets, re-execution can update the canonical object
for the corresponding observation period.

Examples:

```text
AEMET current
→ merge daily observations
→ deduplicate by (idema, fint)
```

```text
Open-Meteo hourly
→ merge the station/day object by observation time
```

```text
ESIOS hourly
→ merge the dataset/day object
→ deduplicate by (geo_id, datetime_utc)
```

This behaviour differs from the earlier implementation in which every
ingestion execution created a separate timestamp-named Bronze object.

The final design prioritizes canonical observation-time storage while retaining
ingestion metadata for audit.

---

## 14. Incremental Error Handling

Invalid requests and technical failures are rejected before successful
persistence whenever possible.

Handled conditions include:

- invalid temporal ranges;
- network failures;
- HTTP failures;
- authentication failures;
- request timeouts;
- malformed API responses;
- invalid coordinates;
- incomplete Open-Meteo daily coverage;
- storage failures.

A structurally valid ESIOS:

```text
values = []
```

response is not an error.

It is valid `NO_DATA`.

Existing valid Bronze objects remain available if a later acquisition fails.

---

## 15. Relationship with Historical Reload

Historical and recurrent ingestion reuse the same source-specific connector
architecture, but their orchestration responsibilities differ.

```text
historical_reload
→ explicit historical interval
→ Bronze acquisition
→ Silver
→ Gold
```

```text
hourly_ingestion
→ recurrent recent acquisition
→ Bronze only
```

```text
monthly_ingestion
→ recurrent monthly acquisition
→ Bronze only
```

```text
open_meteo_15min
→ manual historical acquisition
→ Bronze only
```

The final historical Airflow interface exposes:

```text
fecha_inicio
fecha_fin
sobreescribir_datos
eliminar_historial_completo
```

and supports the validated:

```text
PRESERVE
RANGE OVERWRITE
FULL DELETE
```

policies.

These historical persistence policies belong to `historical_reload`, not to the
secondary recurrent Bronze-only DAGs.

---

## 16. Downstream Processing Boundary

Incremental ingestion stops at Bronze.

The recurrent DAGs do not independently execute:

```text
Bronze → Silver
Silver → Gold
```

The downstream analytical path remains:

```text
Bronze
  │
  ▼
Silver
  │
  ▼
Gold
```

with Spark responsible for structured Lakehouse transformation.

This separation prevents the source-ingestion DAGs from duplicating processing
logic.

---

## 17. Validation Status

The final ingestion implementation has been regression-tested after the
observation-time storage and historical-orchestration refactor.

The latest validated ingestion suite is:

```text
84 passed
```

The complete final regression status is:

```text
tests/ingestion = 84 passed
tests/silver    = 85 passed
tests/gold      = 72 passed
```

Validated current behaviours include:

```text
AEMET current daily merge
Open-Meteo hourly canonical daily merge
Open-Meteo hourly 24-point completeness
Open-Meteo 15-minute 96-point completeness
ESIOS hourly canonical daily merge
ESIOS values=[] → NO_DATA
observation-time Bronze storage
Airflow recurrent Bronze-only DAG structure
```

The final historical Bronze → Silver → Gold runtime has also been validated
under Airflow control.

---

## 18. Current Incremental Status

The final incremental ingestion design is:

```text
hourly_ingestion
= RECURRENT BRONZE INGESTION

monthly_ingestion
= RECURRENT BRONZE INGESTION

open_meteo_15min
= MANUAL HISTORICAL BRONZE UTILITY

esios_5min
= NOT IN FINAL RUNTIME

daily_ingestion
= NOT IN FINAL RUNTIME

AEMET daily climatology
= NOT IN FINAL PHYSICAL SCOPE

AEMET radiation ingestion
= NOT IN FINAL PHYSICAL SCOPE

ESIOS 5-minute analytical flow
= NOT IN FINAL PHYSICAL SCOPE

Bronze partitioning
= OBSERVATION TIME

ESIOS valid empty response
= NO_DATA

Recurrent Silver promotion
= NOT IMPLEMENTED BY SECONDARY DAGS

Recurrent Gold promotion
= NOT IMPLEMENTED BY SECONDARY DAGS

Ingestion regression suite
= 84 PASSED
```

The recurrent ingestion layer is therefore aligned with the final implemented
project scope and remains deliberately separated from the historical end-to-end
Airflow workflow.
