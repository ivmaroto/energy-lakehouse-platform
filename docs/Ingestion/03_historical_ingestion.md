# Historical Ingestion

## 1. Overview

Historical ingestion is responsible for acquiring previously published data
required to populate or rebuild the Bronze layer of the Energy Lakehouse
Platform.

The current historical processing flow integrates four source domains:

- AEMET OpenData;
- Open-Meteo;
- REE / ESIOS;
- CNIG / IGN.

Historical behaviour is source-specific because the available temporal ranges,
API interfaces, geographical coverage and publication characteristics differ
between providers.

The general processing path is:

```text
Requested historical interval
            │
            ▼
     Source acquisition
            │
            ▼
    Technical validation
            │
            ▼
      MinIO / Bronze
            │
            ▼
       Apache Spark
            │
            ▼
   Apache Iceberg Silver
            │
            ▼
       Apache Spark
            │
            ▼
    Apache Iceberg Gold
```

Historical ingestion itself is responsible only for acquisition and Bronze
persistence.

Silver and Gold transformations remain separate processing stages.

---

## 2. Objectives

Historical ingestion has the following objectives:

- retrieve available historical information from external providers;
- accept an explicit requested temporal interval where applicable;
- preserve the source representation;
- perform technical response validation;
- persist valid acquisitions in Bronze;
- divide large requests into manageable source-specific units;
- allow interrupted acquisitions to resume where implemented;
- provide repeatable input for Silver and Gold processing;
- support rebuilding selected analytical periods when required.

Historical ingestion does not perform:

- business-level joins;
- geographical aggregation;
- source fallback;
- analytical metric calculation;
- Silver normalization;
- Gold integration.

---

## 3. Historical Execution Interval

Historical time-series acquisition uses:

```text
start_date
end_date
```

The interval is inclusive according to the source-specific implementation.

A historical execution does not imply that every dataset in the platform is
historical.

Some datasets are reference masters or current-only sources.

The source behaviour is:

```text
Open-Meteo
→ historical observations

REE / ESIOS
→ historical observations

AEMET stations
→ master/reference data

AEMET current_observations
→ current/recent observations

CNIG / IGN
→ master/reference data
```

Therefore, AEMET current observations must not be reinterpreted as observations
belonging to the requested historical period.

---

## 4. General Historical Flow

The historical flow follows:

```text
start_date + end_date
          │
          ▼
 Validate requested interval
          │
          ▼
 Source-specific preparation
          │
          ▼
 Split work where required
          │
          ▼
 External source request
          │
          ▼
 Technical validation
          │
          ▼
 Bronze persistence
```

Large historical requests may require multiple API calls.

The exact subdivision strategy depends on the provider.

---

## 5. AEMET in Historical Executions

The final active AEMET source scope is:

```text
stations
current_observations
```

### Station master

The station catalogue is loaded as reference data.

The currently validated catalogue contains:

```text
926 stations
```

It provides the meteorological location catalogue subsequently used by
Open-Meteo.

### Current observations

AEMET current observations represent recent/current meteorological
measurements.

They cannot be used as a generic mechanism for reconstructing an arbitrary
historical interval.

Therefore, when a complete historical platform execution includes:

```text
AEMET current_observations
```

those observations retain their actual recent timestamps.

They are not assigned to:

```text
start_date
end_date
```

of the historical Open-Meteo and ESIOS request.

Historical meteorological reconstruction is supplied by Open-Meteo.

---

## 6. Open-Meteo Historical Ingestion

Open-Meteo provides the reproducible historical meteorological coverage used by
the analytical model.

Historical acquisition operates over the AEMET station catalogue:

```text
926 locations
```

The current active historical datasets are:

```text
weather_hourly
weather_15min
```

The two datasets use different Open-Meteo services.

---

## 7. Open-Meteo Historical Hourly Data

Historical hourly weather is retrieved through:

```text
https://archive-api.open-meteo.com/v1/archive
```

The acquisition process is conceptually:

```text
AEMET station catalogue
          │
          ▼
  926 coordinates
          │
          ▼
Open-Meteo Archive API
          │
          ▼
Hourly weather response
          │
          ▼
Temporal validation
          │
          ▼
MinIO / Bronze
```

The historical interval is supplied explicitly.

---

## 8. Open-Meteo Historical 15-Minute Data

Historical 15-minute weather is retrieved through:

```text
https://historical-forecast-api.open-meteo.com/v1/forecast
```

The standard Forecast API is not used for arbitrary historical 15-minute
acquisition.

This endpoint distinction was validated against the real Open-Meteo service.

A real API validation for:

```text
2026-01-10 → 2026-01-15
```

returned:

```text
POINTS = 576
FIRST  = 2026-01-10T00:00
LAST   = 2026-01-15T23:45
```

for the tested location.

This corresponds exactly to:

```text
6 days × 24 hours × 4 observations/hour
= 576 observations
```

---

## 9. Open-Meteo Historical Variables

The current historical meteorological flow includes variables used downstream
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

The Bronze layer preserves the source representation.

Selection, aggregation and analytical naming are applied in downstream
processing.

---

## 10. Open-Meteo Batch Processing

Historical Open-Meteo acquisition must process hundreds of locations.

The implementation therefore contains dedicated batch logic in:

```text
ingestion/open_meteo/batch.py
```

Operational controls include:

- configurable retries;
- exponential backoff;
- request pacing;
- historical request splitting;
- location-by-location progress;
- temporal coverage validation;
- detection of already completed locations;
- resumable processing.

Bronze-state inspection is implemented in:

```text
ingestion/open_meteo/bronze_state.py
```

This prevents a large historical execution from needing to restart every
location after an interruption.

---

## 11. Open-Meteo Completeness Validation

The existence of a Bronze object is not sufficient to consider a historical
location complete.

Its temporal coverage must correspond to the requested interval.

For the validated interval:

```text
2026-01-10 → 2026-01-15
```

expected observations per location were:

### Hourly

```text
6 × 24
= 144
```

### 15-minute

```text
6 × 24 × 4
= 576
```

The completed historical acquisition covered:

```text
926 / 926 hourly locations
926 / 926 15-minute locations
```

The downstream Silver counts confirmed the expected complete coverage:

```text
926 × 144
= 133344 hourly rows
```

```text
926 × 576
= 533376 fifteen-minute rows
```

---

## 12. REE / ESIOS Historical Ingestion

REE / ESIOS provides historical electricity-system information.

The current active ESIOS scope contains:

```text
11 hourly electricity-generation indicators
9 monthly installed-capacity indicators
```

The selected indicators are stored in:

```text
config/esios_indicators.json
```

The connector itself remains generic and does not require the indicator
catalogue to be hardcoded inside the ingestion implementation.

---

## 13. ESIOS Hourly Historical Scope

The final hourly generation indicators are:

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

These datasets feed:

```text
silver_esios_energy_hourly
```

and subsequently:

```text
gold_fact_province_hourly
```

---

## 14. ESIOS Monthly Historical Scope

The final monthly installed-capacity indicators are:

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

These datasets feed:

```text
silver_esios_installed_capacity_monthly
```

and subsequently:

```text
gold_fact_installed_capacity_monthly
```

---

## 15. ESIOS Historical Availability

ESIOS availability is validated against the real API rather than assumed from
HTTP status alone.

For:

```text
2026-01-10 → 2026-01-15
```

all configured:

```text
11 hourly indicators
9 monthly indicators
```

returned actual data.

The validation result was:

```text
FAILED_DATASETS = []
ALL_ESIOS_AVAILABLE = True
```

This historical interval was therefore selected for the final technical
end-to-end historical validation.

Availability for this interval does not imply that every indicator necessarily
contains data for every arbitrary recent interval.

---

## 16. ESIOS Empty Responses

The current ESIOS ingestion logic validates:

```text
indicator.values
```

before successful Bronze persistence.

If:

```text
indicator.values = []
```

the ingestion attempt raises an error instead of treating an empty source
response as a successful dataset.

This prevents:

```text
HTTP 200
```

from being incorrectly interpreted as:

```text
valid observations available
```

The handling of legitimate recent-source publication delays belongs to the
orchestration strategy and must not be inferred from historical ingestion
behaviour.

---

## 17. CNIG / IGN Reference Data

CNIG / IGN provides the canonical geographical master used downstream.

The current Bronze master datasets are:

```text
provinces
municipalities
```

They are not observation datasets and therefore do not depend on the requested
historical analysis interval.

Downstream Silver normalization produces the canonical structure containing:

```text
52 province-level entities
19 autonomous communities
8132 municipalities
```

These datasets support geographical normalization of meteorological and energy
sources.

---

## 18. Request Chunking

Historical source requests can be divided into smaller units.

Conceptually:

```text
Requested historical period
           │
           ▼
  Source-specific splitting
           │
    ┌──────┼──────┐
    ▼      ▼      ▼
 Window 1 Window 2 ...
    │      │
    ▼      ▼
 External requests
```

Chunking improves:

- failure isolation;
- retry behaviour;
- memory usage;
- API-limit handling;
- execution traceability.

Chunk sizes and request subdivision are implementation configuration rather than
analytical business rules.

They may differ between AEMET, Open-Meteo and ESIOS.

---

## 19. Retry and Recovery

Historical ingestion uses multiple levels of failure handling.

### Common HTTP layer

Handles temporary conditions such as:

```text
timeouts
temporary HTTP errors
connection failures
```

### Open-Meteo batch layer

Adds:

```text
retry
backoff
pacing
coverage validation
resume capability
```

### Airflow orchestration layer

Can provide task-level retries when the historical process is executed through
Airflow.

These mechanisms operate at different levels and should not be treated as the
same retry system.

---

## 20. Bronze Output

Historical source information is persisted in MinIO under the Bronze hierarchy.

The general organization is:

```text
bronze/
├── aemet/
├── open_meteo/
├── esios/
└── cnig/
```

with dataset and ingestion-date subdivisions beneath each source.

Conceptually:

```text
bronze/
└── <source>/
    └── <dataset>/
        └── year=YYYY/
            └── month=MM/
                └── day=DD/
                    └── <object>
```

The physical:

```text
year/month/day
```

hierarchy represents the ingestion date.

The requested source period remains recorded in Bronze metadata.

---

## 21. Bronze Metadata

Historical Bronze objects contain metadata such as:

```text
source
dataset
ingestion_mode
ingestion_timestamp
requested_start_date
requested_end_date
```

Additional source-specific metadata may include:

```text
AEMET station information

Open-Meteo location_id
latitude
longitude

ESIOS indicator_id
```

This information provides traceability between the Bronze object and the
request that produced it.

---

## 22. Reprocessing

Persisted Bronze data allows Silver and Gold to be recalculated without
necessarily repeating the external API acquisition.

The architecture supports:

```text
External source
      │
      ▼
Bronze
      │
      ├──► Silver transformation version A
      │
      └──► Silver transformation version B
```

This separation is one of the reasons Bronze acquisition and Lakehouse
transformation remain independent.

A new transformation implementation therefore does not automatically require a
new API download when appropriate Bronze source data is already available.

---

## 23. Real Historical Bronze Validation

The final historical technical validation used the real interval:

```text
2026-01-10 → 2026-01-15
```

The completed Bronze execution reported:

```text
BRONZE HISTORICAL LOAD COMPLETED
```

with:

```text
AEMET stations
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

The AEMET current-observations object retained its real recent/current
timestamps and was not converted into January historical observations.

---

## 24. Downstream Silver Validation

The historical Bronze data was subsequently processed through Silver.

The final Silver namespace contained exactly:

```text
9 tables
```

with counts:

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

This confirms that the historical Bronze acquisition produced valid input for
the implemented Silver layer.

---

## 25. Downstream Gold Validation

The same execution was processed through Gold.

The final Gold namespace contained exactly:

```text
gold_dim_geography
gold_dim_time
gold_fact_installed_capacity_monthly
gold_fact_province_hourly
```

with counts:

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

The principal hourly fact contained:

```text
8100 rows with weather
6768 rows with energy
6721 rows with both weather and energy
```

with:

```text
0 duplicate Province × hour keys
```

The monthly installed-capacity fact also contained:

```text
0 duplicate Autonomous Community × month keys
```

---

## 26. Validated Historical End-to-End Flow

The historical processing path validated with real source data is:

```text
Open-Meteo ──────┐
REE / ESIOS ─────┤
AEMET ───────────┼──► Bronze / MinIO
CNIG / IGN ──────┘
                       │
                       ▼
                 Apache Spark
                       │
                       ▼
                 Silver / Iceberg
                       │
                       ▼
                 Apache Spark
                       │
                       ▼
                  Gold / Iceberg
                       │
                       ▼
                      Trino
```

Real integrated Gold rows containing both meteorological and ESIOS generation
metrics were successfully queried through Trino.

Therefore:

```text
Real APIs
→ Bronze
→ Silver
→ Gold
→ Trino
```

is technically validated.

---

## 27. Airflow Historical Orchestration

The project contains the historical orchestration DAG:

```text
airflow/dags/historical_reload.py
```

Its purpose is to coordinate:

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

However, the final complete Airflow-triggered historical:

```text
Bronze
→ Silver
→ Gold
```

runtime execution has not yet been accepted as fully validated.

The successful historical E2E execution described in this document validates
the underlying ingestion and processing components independently from that final
Airflow runtime proof.

---

## 28. Current Historical Ingestion Status

The current status is:

```text
Historical Open-Meteo hourly acquisition
= VALIDATED

Historical Open-Meteo 15-minute acquisition
= VALIDATED

Open-Meteo 926-location historical batch
= VALIDATED

Open-Meteo temporal completeness
= VALIDATED

Open-Meteo resumable historical acquisition
= VALIDATED

ESIOS historical hourly acquisition
= VALIDATED

ESIOS historical monthly acquisition
= VALIDATED

ESIOS configured historical indicators
= VALIDATED

CNIG master acquisition
= VALIDATED

AEMET station master acquisition
= VALIDATED

AEMET current-observation semantics
= VALIDATED AS CURRENT/RECENT SOURCE

Historical Bronze persistence in MinIO
= VALIDATED

Historical Bronze → Silver
= VALIDATED

Historical Silver → Gold
= VALIDATED

Historical Gold → Trino
= VALIDATED

Complete Airflow-triggered E2E historical runtime
= PENDING FINAL ORCHESTRATION VALIDATION
```

The historical ingestion implementation itself is therefore operational for the
current project scope.