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

The final historical Airflow workflow exposes the following required temporal
parameters:

```text
fecha_inicio
fecha_fin
```

Internally, source-specific ingestion functions use equivalent date objects
such as:

```text
start_date
end_date
```

The requested interval is validated before source acquisition.

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

AEMET current observations are deliberately excluded from the final
`historical_reload` workflow and are not reinterpreted as observations
belonging to the requested historical period.

---

## 4. General Historical Flow

The historical flow follows:

```text
fecha_inicio + fecha_fin
          │
          ▼
 Validate requested interval
          │
          ▼
 Apply persistence policy
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

The final Airflow-controlled historical workflow additionally coordinates the
subsequent Silver and Gold stages after Bronze ingestion has completed.

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

Within `historical_reload`, the station master is handled as an ensure-style
reference dataset:

```text
master exists
→ preserve it

master missing
→ ingest it
```

This means that PRESERVE and RANGE OVERWRITE keep the existing master unchanged,
while FULL DELETE rebuilds it after the active Bronze layer has been removed.

### Current observations

AEMET current observations represent recent/current meteorological
measurements.

They cannot be used as a generic mechanism for reconstructing an arbitrary
historical interval.

Therefore, AEMET current observations are deliberately excluded from the final
`historical_reload` DAG.

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

Historical Open-Meteo data is persisted in canonical daily objects per station.

The resume/completeness logic does not consider a day complete merely because
its object exists.

For the validated granularities:

```text
hourly
→ expected daily axis = 24 timestamps

15-minute
→ expected daily axis = 96 timestamps
```

An incomplete canonical daily object is therefore eligible for reconstruction
instead of being silently skipped.

---

## 11. Open-Meteo Completeness Validation

The existence of a Bronze object is not sufficient to consider a historical
location complete.

Its temporal coverage must correspond to the expected observation axis.

At daily canonical-object level:

### Hourly

```text
24 / 24 expected timestamps
→ complete

fewer than 24
→ incomplete
```

### 15-minute

```text
96 / 96 expected timestamps
→ complete

fewer than 96
→ incomplete
```

For the independently validated interval:

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

The current ESIOS ingestion logic validates the indicator response before
Bronze persistence.

A valid ESIOS response with:

```text
indicator.values = []
```

is handled as a valid:

```text
NO_DATA
```

result.

In that case, the ingestion method returns no persisted observations instead
of raising an error or fabricating records.

This distinction prevents:

```text
HTTP 200
```

from being incorrectly interpreted as proof that observations exist, while
also allowing a legitimate empty source response to be represented without
turning it into a false ingestion failure.

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

Time-series facts are physically governed by their source observation period,
not by the ingestion timestamp.

The validated canonical paths include:

```text
Open-Meteo hourly
bronze/open_meteo/weather_hourly/
year=YYYY/month=MM/day=DD/
station_id=<station_id>.json

Open-Meteo 15-minute
bronze/open_meteo/weather_15min/
year=YYYY/month=MM/day=DD/
station_id=<station_id>.json

ESIOS hourly
bronze/esios/<dataset>/
year=YYYY/month=MM/day=DD/
data.json

ESIOS monthly
bronze/esios/<dataset>/
year=YYYY/month=MM/
data.json

AEMET stations
bronze/aemet/stations/stations.json

AEMET current observations
bronze/aemet/current_observations/
year=YYYY/month=MM/day=DD/
observations.json

CNIG provinces
bronze/cnig/provinces/provinces.csv

CNIG municipalities
bronze/cnig/municipalities/municipalities.csv
```

The physical temporal hierarchy therefore represents the observation date or
observation month for analytical facts.

`ingestion_timestamp` remains audit metadata and is not used as the physical
business partition date.

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
necessarily repeating every external API acquisition.

The final historical orchestration supports three validated persistence
behaviours.

### PRESERVE

```text
sobreescribir_datos = false
eliminar_historial_completo = false
```

Existing Bronze data, masters and existing Silver/Gold natural keys are
preserved.

Missing historical coverage is added.

The historical Silver and Gold write tasks use:

```text
LAKEHOUSE_WRITE_POLICY=insert-only
```

so existing natural keys are not physically rewritten during PRESERVE.

### RANGE OVERWRITE

```text
sobreescribir_datos = true
eliminar_historial_completo = false
```

Only the requested historical interval is removed and rebuilt.

Data outside the requested range is preserved.

Existing AEMET/CNIG masters are also preserved.

### FULL DELETE

```text
eliminar_historial_completo = true
```

Full deletion has priority over range overwrite.

The active Bronze layer is deleted, the current Silver and Gold tables are
dropped/purged, residual physical objects under the active Silver/Gold warehouse
prefixes are removed, masters are rebuilt, and only the requested interval is
loaded again.

These behaviours preserve the architectural separation between acquisition,
Bronze persistence and downstream Lakehouse processing while supporting
controlled reconstruction when required.

---

## 23. Real Historical Bronze Validation

An independent historical technical validation used the real interval:

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

That independent validation predates the final `historical_reload` orchestration
policy and included an AEMET current-observations acquisition.

The AEMET current-observations object retained its real recent/current
timestamps and was not converted into January historical observations.

The final Airflow `historical_reload` DAG deliberately excludes AEMET current
observations.

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

These counts belong to the independent historical validation described above
and should not be interpreted as the final one-day FULL DELETE validation.

---

## 25. Downstream Gold Validation

The same independent execution was processed through Gold.

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

The final historical Airflow workflow has also been validated independently as
an orchestrated Bronze → Silver → Gold execution.

---

## 27. Airflow Historical Orchestration

The project contains the historical orchestration DAG:

```text
airflow/dags/historical_reload.py
```

Its validated purpose is to coordinate:

```text
Persistence policy
      │
      ▼
Bronze ingestion
      │
      ▼
Silver processing
      │
      ▼
Gold processing
```

The DAG exposes exactly four runtime parameters:

```text
fecha_inicio
fecha_fin
sobreescribir_datos
eliminar_historial_completo
```

The validated persistence policies are:

```text
PRESERVE
sobreescribir_datos = false
eliminar_historial_completo = false

RANGE OVERWRITE
sobreescribir_datos = true
eliminar_historial_completo = false

FULL DELETE
eliminar_historial_completo = true
```

FULL DELETE has priority over RANGE OVERWRITE.

The complete Airflow-triggered:

```text
Bronze
→ Silver
→ Gold
```

historical runtime has been validated with real data.

The validation demonstrated:

- PRESERVE keeps existing active Silver/Gold files unchanged and adds missing
  coverage without duplicate natural keys;
- RANGE OVERWRITE rebuilds only the requested interval while preserving data
  outside the range and preserving existing masters;
- FULL DELETE removes active Bronze, Silver, Gold and previous-run physical
  warehouse data before rebuilding the requested interval and masters;
- AEMET current observations remain outside the historical workflow.

Final DAG discovery also reported no import errors.

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

ESIOS valid empty response handling
= VALIDATED AS NO_DATA

CNIG master acquisition
= VALIDATED

AEMET station master acquisition
= VALIDATED

AEMET current-observation semantics
= VALIDATED AS CURRENT/RECENT SOURCE

AEMET current exclusion from historical_reload
= VALIDATED

Historical Bronze persistence in MinIO
= VALIDATED

Historical Bronze → Silver
= VALIDATED

Historical Silver → Gold
= VALIDATED

Historical Gold → Trino
= VALIDATED

Airflow-triggered E2E historical runtime
= VALIDATED

PRESERVE persistence policy
= VALIDATED

RANGE OVERWRITE persistence policy
= VALIDATED

FULL DELETE persistence policy
= VALIDATED

Previous-run physical Silver/Gold objects after FULL DELETE
= 0
```

After the final historical-orchestration and persistence changes, the complete
automated test suites passed:

```text
tests/ingestion = 84 passed
tests/silver    = 85 passed
tests/gold      = 72 passed
```

The historical ingestion and historical Airflow orchestration implementation
are therefore operational and validated for the current project scope.
