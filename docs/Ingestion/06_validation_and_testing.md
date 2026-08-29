# Ingestion Validation and Testing

## 1. Overview

This document records the validation and testing performed for the ingestion
layer of the Energy Lakehouse Platform.

The objective is to verify that real meteorological, geographical and
electricity-system data can be acquired reliably, technically validated and
persisted in the Bronze layer in MinIO before downstream Lakehouse processing.

The current validated source domains are:

- AEMET OpenData;
- Open-Meteo;
- REE / ESIOS;
- CNIG / IGN.

Validation has been performed using:

- automated `pytest` tests;
- real external API requests;
- direct MinIO inspection;
- containerized execution;
- historical batch execution;
- downstream Silver and Gold processing;
- SQL validation through Trino.

The final core processing path validated with real data is:

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
Apache Spark / Silver
      │
      ▼
Apache Iceberg
      │
      ▼
Apache Spark / Gold
      │
      ▼
Apache Iceberg
      │
      ▼
Trino
```

Final execution of this complete chain directly orchestrated by Airflow remains
part of the orchestration closure.

---

## 2. Final Validation Scope

The current ingestion validation covers:

- configuration loading;
- credential externalization;
- API authentication;
- HTTP connectivity;
- HTTP retry behaviour;
- API-response validation;
- historical acquisition;
- current/recent acquisition;
- temporal-range validation;
- Open-Meteo historical endpoint selection;
- large Open-Meteo station batches;
- Open-Meteo temporal coverage validation;
- resumable Open-Meteo acquisition;
- ESIOS indicator configuration;
- ESIOS empty-data rejection;
- MinIO Bronze persistence;
- Bronze metadata;
- source-specific error handling;
- automated regression testing;
- real historical ingestion;
- compatibility with Silver processing;
- compatibility with Gold processing;
- Trino queryability of the final Lakehouse output.

The following earlier experimental dataset families are no longer part of the
final physical scope:

```text
AEMET daily climatology
AEMET radiation
ESIOS 5-minute power
electricity demand
electricity market prices
```

Validation evidence for those earlier experiments does not define the current
final ingestion model.

---

## 3. Final Source Scope

The validated final ingestion scope is:

```text
AEMET
├── stations
└── current_observations

Open-Meteo
├── weather_hourly
└── weather_15min

REE / ESIOS
├── 11 hourly generation indicators
└── 9 monthly installed-capacity indicators

CNIG / IGN
├── provinces
└── municipalities
```

These source datasets feed the final:

```text
9 Silver tables
4 Gold tables
```

implemented by the project.

---

## 4. Configuration and Security Validation

Runtime configuration is externalized from source code.

Relevant environment values include:

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

while the real:

```text
.env
```

is excluded from version control.

The final ESIOS indicator catalogue is also externalized in:

```text
config/esios_indicators.json
```

This prevents the validated indicator IDs and dataset mappings from being
duplicated across ingestion code and orchestration definitions.

**Status: VALIDATED**

---

## 5. Common HTTP Validation

The shared HTTP layer was tested for functionality including:

- request execution;
- timeout handling;
- temporary HTTP failures;
- retry behaviour;
- authentication failures;
- JSON-response handling;
- malformed-response handling.

Open-Meteo additionally uses source-specific:

```text
retry
backoff
pacing
```

behaviour for large historical batches.

**Status: VALIDATED**

---

## 6. AEMET Validation

The final active AEMET scope contains:

```text
stations
current_observations
```

AEMET authentication was validated using a real API credential supplied through:

```text
AEMET_API_KEY
```

### Station catalogue

The current validated station catalogue contains:

```text
926 stations
```

This catalogue is subsequently used as the meteorological point master for
Open-Meteo acquisition.

### Current observations

Real AEMET current observations were successfully retrieved and persisted.

The current-observation dataset contains source meteorological information such
as:

```text
station identifier
coordinates
temperature
relative humidity
precipitation
wind
pressure
```

AEMET current observations retain their real source timestamps.

They are not considered a generic source for reconstructing arbitrary
historical periods.

Historical meteorological reconstruction for the analytical model is therefore
provided by Open-Meteo.

**AEMET validation status: VALIDATED**

---

## 7. Open-Meteo Endpoint Validation

Open-Meteo was validated using real external requests.

The final implementation distinguishes between three service types.

### Current / recent data

```text
https://api.open-meteo.com/v1/forecast
```

### Historical hourly data

```text
https://archive-api.open-meteo.com/v1/archive
```

### Historical 15-minute data

```text
https://historical-forecast-api.open-meteo.com/v1/forecast
```

The historical 15-minute endpoint was explicitly validated against the real
service.

A real request for station:

```text
0002I
```

and interval:

```text
2026-01-10 → 2026-01-15
```

returned:

```text
POINTS = 576
FIRST  = 2026-01-10T00:00
LAST   = 2026-01-15T23:45
```

which corresponds exactly to:

```text
6 days × 24 hours × 4 observations/hour
= 576 observations
```

**Open-Meteo endpoint status: VALIDATED**

---

## 8. Open-Meteo Variable Validation

The current analytical flow uses Open-Meteo information including variables
such as:

```text
temperature_2m
relative_humidity_2m
precipitation

wind_speed_80m
wind_direction_80m

wind_speed_120m
wind_direction_120m

shortwave_radiation
direct_normal_irradiance
```

The source payload is persisted in Bronze before downstream normalization and
analytical naming.

No API credential is required for the Open-Meteo access pattern used by the
project.

**Status: VALIDATED**

---

## 9. Open-Meteo Batch Validation

Historical Open-Meteo acquisition operates over the complete AEMET station
catalogue:

```text
926 locations
```

The batch implementation includes:

- retry handling;
- exponential backoff;
- request pacing;
- progress by location;
- existing-Bronze inspection;
- temporal completeness validation;
- resumable acquisition.

Relevant implementation includes:

```text
ingestion/open_meteo/batch.py
ingestion/open_meteo/bronze_state.py
```

The historical execution successfully completed:

```text
926 / 926 hourly locations
926 / 926 15-minute locations
```

**Status: VALIDATED**

---

## 10. Open-Meteo Temporal Completeness Validation

Temporal completeness was validated for:

```text
2026-01-10 → 2026-01-15
```

The interval contains six complete days.

### Hourly expected coverage

```text
6 × 24
= 144 observations per location
```

Across 926 locations:

```text
926 × 144
= 133344 observations
```

The resulting Silver table contained:

```text
silver_open_meteo_hourly
= 133344 rows
```

### 15-minute expected coverage

```text
6 × 24 × 4
= 576 observations per location
```

Across 926 locations:

```text
926 × 576
= 533376 observations
```

The resulting Silver table contained:

```text
silver_open_meteo_15min
= 533376 rows
```

The exact equality between expected and persisted row counts validates the
historical Open-Meteo coverage for the selected interval.

**Status: VALIDATED**

---

## 11. Open-Meteo Recovery Validation

The Bronze-state implementation distinguishes between:

```text
complete location
incomplete location
missing location
```

A historical batch can therefore resume only the locations that still require
acquisition.

The existence of a Bronze object alone is not considered sufficient evidence
of completeness.

Expected temporal coverage is also inspected.

Automated tests cover this behaviour.

**Status: VALIDATED**

---

## 12. REE / ESIOS Validation

REE / ESIOS was validated using a real API credential supplied through:

```text
ESIOS_API_KEY
```

The final active configuration contains:

```text
11 hourly electricity-generation indicators
9 monthly installed-capacity indicators
```

The ESIOS connector supports source parameters required by the configured
indicator requests while keeping indicator selection outside the connector
implementation.

**Status: VALIDATED**

---

## 13. Final ESIOS Hourly Indicators

The validated hourly generation catalogue is:

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

These indicators feed:

```text
silver_esios_energy_hourly
```

and subsequently:

```text
gold_fact_province_hourly
```

---

## 14. Final ESIOS Monthly Indicators

The validated monthly installed-capacity catalogue is:

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

These indicators feed:

```text
silver_esios_installed_capacity_monthly
```

and subsequently:

```text
gold_fact_installed_capacity_monthly
```

---

## 15. ESIOS Real Availability Validation

Real API availability was tested for:

```text
2026-01-10 → 2026-01-15
```

All configured final indicators returned actual source data.

The validation result was:

```text
FAILED_DATASETS = []
ALL_ESIOS_AVAILABLE = True
```

This interval was therefore selected for the final historical end-to-end
technical validation.

The result proves that:

- the configured indicator IDs are valid;
- authentication works;
- the ESIOS connector works;
- the API can return actual observations for the selected historical period.

It does not imply that every recent interval contains published data for every
indicator.

**Status: VALIDATED**

---

## 16. ESIOS Empty-Response Validation

Real recent ESIOS requests demonstrated that HTTP success can occur while:

```text
indicator.values = []
```

The ingestion implementation was therefore modified so that an empty ESIOS
values collection is not treated as successful source acquisition.

The current behaviour is:

```text
HTTP response
      │
      ▼
Indicator structure
      │
      ▼
Validate indicator.values
      │
      ├── non-empty → continue
      │
      └── empty     → fail acquisition
```

Automated regression tests cover this behaviour.

The final orchestration behaviour for a legitimate recent publication delay is
not documented here as an implemented `NO_DATA` state because that behaviour
has not yet been validated.

**Status: VALIDATED AT INGESTION LEVEL**

---

## 17. CNIG / IGN Validation

CNIG / IGN provides the canonical geographical reference used downstream.

The current Bronze source masters are:

```text
provinces
municipalities
```

The resulting Silver geographical model contains:

```text
silver_cnig_provinces
= 52 rows

silver_cnig_autonomous_communities
= 19 rows

silver_cnig_municipalities
= 8132 rows
```

Official codes are preserved as strings where required so leading zeroes are
not lost.

CNIG therefore supplies the geographical reference used later by the
meteorological and energy normalization logic.

**Status: VALIDATED**

---

## 18. Bronze Storage Validation

MinIO is the production-like Bronze storage backend.

Bronze acquisitions are persisted below the configured:

```text
bronze/
```

prefix and organized logically by:

```text
source
dataset
ingestion year
ingestion month
ingestion day
```

The physical date path represents ingestion time rather than observation time.

The requested source interval remains preserved in object metadata.

Validation confirmed:

- connection to MinIO;
- object writing;
- object enumeration;
- object reading;
- JSON deserialization;
- metadata inspection;
- source-payload inspection;
- compatibility with downstream Spark processing.

**Status: VALIDATED**

---

## 19. Bronze Metadata Validation

The common Bronze metadata structure includes:

```text
source
dataset
ingestion_mode
ingestion_timestamp
requested_start_date
requested_end_date
```

Source-specific information may also be included.

Examples include:

```text
Open-Meteo
→ location_id
→ latitude
→ longitude

ESIOS
→ indicator_id
```

This metadata separates:

```text
when data was ingested
```

from:

```text
which source interval was requested
```

**Status: VALIDATED**

---

## 20. Error Handling Validation

Automated and real executions validate controlled handling for conditions
including:

- invalid temporal ranges;
- connection errors;
- HTTP failures;
- request timeouts;
- authentication errors;
- invalid JSON;
- malformed API structures;
- empty ESIOS values;
- incomplete Open-Meteo coverage;
- MinIO persistence failures.

A failed acquisition is not represented as a valid completed dataset.

**Status: VALIDATED**

---

## 21. Automated Test Suite

The ingestion implementation contains automated regression tests covering
components including:

```text
AEMET
Open-Meteo
ESIOS
date utilities
Bronze storage
Open-Meteo Bronze state
Open-Meteo historical batch behaviour
historical orchestration support
```

The latest validated complete ingestion regression execution finished with:

```text
68 passed
```

No failures remained in that execution.

The suite includes tests associated with:

- temporal-range validation;
- storage behaviour;
- ESIOS empty-response rejection;
- Open-Meteo endpoint behaviour;
- historical batch processing;
- temporal completeness;
- resumable acquisition.

**Automated ingestion test status: 68 PASSED**

---

## 22. Real Historical Bronze Validation

A complete historical Bronze execution was performed for:

```text
2026-01-10 → 2026-01-15
```

The final execution reported:

```text
BRONZE HISTORICAL LOAD COMPLETED
```

with:

```text
AEMET station master
= 1 Bronze object

CNIG masters
= 2 Bronze objects

ESIOS hourly
= 11 files

ESIOS monthly
= 9 files

Open-Meteo locations
= 926

Open-Meteo hourly
= 926 files

Open-Meteo 15-minute
= 926 files

AEMET current observations
= 1 file
```

AEMET current observations retained their real current timestamps and were not
rewritten as January historical observations.

**Status: VALIDATED**

---

## 23. Silver Compatibility Validation

The Bronze execution was processed through the complete final Silver
implementation.

The resulting physical Silver namespace contained exactly:

```text
9 tables
```

with:

```text
silver_aemet_stations
= 926

silver_aemet_current_observations
= 9786

silver_open_meteo_hourly
= 133344

silver_open_meteo_15min
= 533376

silver_cnig_provinces
= 52

silver_cnig_autonomous_communities
= 19

silver_cnig_municipalities
= 8132

silver_esios_energy_hourly
= 38443

silver_esios_installed_capacity_monthly
= 123
```

This validates that the ingestion output can be consumed by the implemented
Bronze-to-Silver Spark processing.

**Status: VALIDATED**

---

## 24. Gold Compatibility Validation

The same data was subsequently processed through the final Gold implementation.

Gold persistence completed successfully with exactly four tables:

```text
gold_dim_geography
gold_dim_time
gold_fact_installed_capacity_monthly
gold_fact_province_hourly
```

Validated row counts were:

```text
gold_dim_geography
= 71

gold_dim_time
= 158

gold_fact_installed_capacity_monthly
= 19

gold_fact_province_hourly
= 8147
```

**Status: VALIDATED**

---

## 25. Gold Functional Validation

The principal Gold fact:

```text
gold_fact_province_hourly
```

was validated at:

```text
Province × hour
```

with:

```text
province_hourly_rows
= 8147

rows_with_weather
= 8100

rows_with_energy
= 6768

rows_with_weather_and_energy
= 6721

duplicate_province_hour_keys
= 0
```

The full outer integration can also be verified arithmetically.

Weather-only rows:

```text
8100 - 6721
= 1379
```

Energy-only rows:

```text
6768 - 6721
= 47
```

Therefore:

```text
1379
+ 47
+ 6721
= 8147
```

which matches the exact fact-table row count.

This validates that valid records from either source block are retained.

**Status: VALIDATED**

---

## 26. Installed-Capacity Validation

The second analytical fact:

```text
gold_fact_installed_capacity_monthly
```

was validated with:

```text
rows
= 19

distinct months
= 1

month
= 2026-01

rows with installed-capacity values
= 19

duplicate Autonomous Community × month keys
= 0
```

Installed capacity remains at:

```text
Autonomous Community × month
```

and is not artificially disaggregated to provinces.

**Status: VALIDATED**

---

## 27. Trino Validation

The final Gold namespace was queried successfully through Trino.

The exact visible Gold tables were:

```text
gold_dim_geography
gold_dim_time
gold_fact_installed_capacity_monthly
gold_fact_province_hourly
```

Real integrated rows containing both:

```text
meteorological metrics
```

and:

```text
ESIOS electricity-generation metrics
```

were successfully returned.

This proves that data acquired by the ingestion layer reaches the final SQL
consumption layer.

**Status: VALIDATED**

---

## 28. Validated End-to-End Path

The final validated technical path is:

```text
Real external sources
        │
        ▼
Python ingestion
        │
        ▼
MinIO / Bronze
        │
        ▼
Apache Spark
        │
        ▼
Silver / Apache Iceberg
        │
        ▼
Apache Spark
        │
        ▼
Gold / Apache Iceberg
        │
        ▼
Trino
```

This flow has been executed successfully using real source data.

Therefore:

```text
APIs
→ Bronze
→ Silver
→ Gold
→ Trino
```

is technically validated.

---

## 29. Airflow Infrastructure Validation

Apache Airflow infrastructure has previously been validated.

Validated components include:

```text
Airflow Webserver
Airflow Scheduler
PostgreSQL metadata connectivity
DAG discovery
```

Earlier ingestion DAGs demonstrated that Python ingestion can execute from the
Airflow environment and persist Bronze objects in MinIO.

This validates the Airflow infrastructure and ingestion execution capability.

**Status: VALIDATED**

---

## 30. Historical Reload DAG Validation

The project contains:

```text
airflow/dags/historical_reload.py
```

The DAG is designed to coordinate:

```text
Bronze ingestion
      │
      ▼
Silver processing
      │
      ▼
Gold processing
```

The DAG implementation and task structure have been created.

The processing components that it invokes have already been independently
validated end to end.

However, a final complete execution of:

```text
Airflow trigger
      │
      ▼
Bronze
      │
      ▼
Silver
      │
      ▼
Gold
```

has not yet been accepted as fully runtime-validated.

**Status: IMPLEMENTED / FINAL AIRFLOW RUNTIME VALIDATION PENDING**

---

## 31. Current Recent-Data Limitation

Real testing showed that very recent ESIOS requests can return:

```text
HTTP success
```

while still containing:

```text
values = []
```

The ingestion implementation correctly rejects these responses as valid
completed datasets.

What is not yet validated is the final orchestration policy for treating a
legitimate publication delay gracefully without failing an entire recent-data
workflow.

A dedicated successful:

```text
NO_DATA
```

state must therefore not be described as currently implemented.

This behaviour belongs to the final orchestration validation.

---

## 32. Validation Evidence Summary

The current technical evidence includes:

- automated ingestion tests;
- `68 passed` regression result;
- real AEMET requests;
- real Open-Meteo requests;
- real ESIOS requests;
- real CNIG master processing;
- Open-Meteo historical 15-minute endpoint validation;
- 926-location Open-Meteo historical batch;
- Open-Meteo temporal-completeness validation;
- Open-Meteo resumability validation;
- ESIOS configured-indicator availability validation;
- ESIOS empty-response rejection;
- MinIO Bronze persistence;
- real six-day historical Bronze load;
- final Silver row-count validation;
- final Gold row-count validation;
- Gold natural-key uniqueness validation;
- full-outer integration validation;
- Trino query validation;
- Airflow infrastructure and DAG-discovery validation.

---

## 33. Final Validation Status

| Component | Status |
|---|---|
| Configuration loading | VALIDATED |
| Credential externalization | VALIDATED |
| Common HTTP layer | VALIDATED |
| AEMET station acquisition | VALIDATED |
| AEMET current observations | VALIDATED |
| Open-Meteo hourly historical acquisition | VALIDATED |
| Open-Meteo 15-minute historical acquisition | VALIDATED |
| Open-Meteo historical 15-minute endpoint | VALIDATED |
| Open-Meteo 926-location batch | VALIDATED |
| Open-Meteo completeness validation | VALIDATED |
| Open-Meteo resumable acquisition | VALIDATED |
| Final 11 ESIOS hourly indicators | VALIDATED |
| Final 9 ESIOS monthly indicators | VALIDATED |
| ESIOS empty-values rejection | VALIDATED |
| CNIG geographical masters | VALIDATED |
| MinIO Bronze persistence | VALIDATED |
| Bronze metadata | VALIDATED |
| Historical Bronze load | VALIDATED |
| Ingestion regression suite | 68 PASSED |
| Bronze → Silver | VALIDATED |
| Silver → Gold | VALIDATED |
| Gold → Trino | VALIDATED |
| Province × hour uniqueness | VALIDATED |
| FULL OUTER weather/energy integration | VALIDATED |
| CCAA × month installed-capacity uniqueness | VALIDATED |
| Airflow infrastructure | VALIDATED |
| Historical reload DAG implementation | IMPLEMENTED |
| Complete Airflow-controlled E2E runtime | PENDING FINAL VALIDATION |
| Graceful recent ESIOS `NO_DATA` orchestration | NOT YET VALIDATED |

The ingestion layer itself is implemented and validated for the current project
scope.

The remaining work concerns final orchestration behaviour and runtime proof
through Apache Airflow rather than redesign of the ingestion components.