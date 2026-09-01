# Silver Layer Design

## 1. Purpose

The Silver layer transforms raw Bronze source data into normalized, typed,
deduplicated and reusable datasets while preserving the real temporal and
geographical semantics of each source.

The processing path is:

```text
MinIO / Bronze
      │
      ▼
Apache Spark / PySpark
      │
      ▼
Apache Iceberg / Silver
```

Silver is responsible for:

- parsing Bronze source payloads;
- explicit data typing;
- timestamp normalization;
- coordinate normalization;
- natural-key deduplication;
- geographical normalization where applicable;
- technical data-quality validation;
- preservation of source traceability.

Silver does not perform the final analytical integration between meteorology and
energy.

That responsibility belongs to Gold.

The principal downstream analytical target is:

```text
Province × hour
meteorology + electricity generation
```

while monthly installed capacity remains at:

```text
Autonomous Community × month
```

---

## 2. Final Physical Silver Model

The final implemented Silver layer contains exactly **9 Apache Iceberg tables**.

### AEMET

```text
silver_aemet_stations
silver_aemet_current_observations
```

### Open-Meteo

```text
silver_open_meteo_hourly
silver_open_meteo_15min
```

### CNIG / IGN

```text
silver_cnig_provinces
silver_cnig_autonomous_communities
silver_cnig_municipalities
```

### REE / ESIOS

```text
silver_esios_energy_hourly
silver_esios_installed_capacity_monthly
```

The following previously implemented or evaluated tables are **not part of the
final physical Silver model**:

```text
silver_aemet_daily_climatology
silver_open_meteo_historical_forecast
silver_esios_power_5min
```

Therefore:

```text
Final Silver tables = 9
```

---

## 3. Silver Design Principles

The final Silver design follows these principles.

### Preserve source granularity

Silver does not create a temporal resolution that is absent from the source.

Examples:

```text
15-minute source
→ remains 15-minute in Silver

hourly source
→ remains hourly in Silver

monthly source
→ remains monthly in Silver
```

Temporal aggregation belongs to Gold.

---

### Preserve real geography

Silver does not manufacture geographical detail.

```text
Province source
→ Province

Autonomous Community source
→ Autonomous Community

National / higher-level source
→ preserve actual geography
```

A higher-level observation must never be artificially expanded to provinces.

---

### Preserve valid NULL values

A missing metric is not automatically an error.

```text
NULL
≠
0
```

Values are not automatically replaced with:

- zero;
- averages;
- previous observations;
- synthetic values.

---

### Natural-key idempotency

Reprocessing overlapping Bronze acquisitions must not multiply the same logical
Silver observation.

Dataset-specific natural keys are therefore used during persistence.

---

### Source traceability

Silver retains the information required to identify:

- source;
- source dataset;
- observation;
- ingestion execution;
- source geography where relevant.

---

## 4. Final Source Scope

Silver is built from the following final Bronze scope.

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

The Silver model therefore reflects the final ingestion scope rather than all
datasets explored during previous implementation iterations.

---

# 5. Geographical Normalization

CNIG / IGN is the canonical territorial reference used by the Lakehouse.

The normalized geographical model contains:

```text
52 province-level entities
19 autonomous communities
8132 municipalities
```

Official codes are preserved as strings so leading zeroes are not lost.

The general geographical-normalization flow is:

```text
Source geography
      │
      ▼
Deterministic normalization
      │
      ▼
Controlled alias resolution if required
      │
      ▼
CNIG canonical geography
```

---

## 5.1 Province Name Normalization

Where a source provides province names, deterministic normalization is applied
before matching them against CNIG.

The normalization includes:

- trimming surrounding whitespace;
- uppercase conversion;
- Unicode decomposition;
- removal of diacritics.

Examples include:

```text
ALMERIA
→ Almería

ARABA/ALAVA
→ Araba/Álava

AVILA
→ Ávila

CACERES
→ Cáceres
```

The operation normalizes representation but does not translate or invent
geographical information.

---

## 5.2 Controlled Province Aliases

A controlled configuration is used for known source naming differences that
cannot be solved through deterministic normalization alone.

The configuration is:

```text
config/province_aliases.json
```

Validated aliases include:

```text
ALICANTE
→ Alacant/Alicante

BALEARES
→ Illes Balears

CASTELLON
→ Castelló/Castellón

STA. CRUZ DE TENERIFE
→ Santa Cruz de Tenerife

VALENCIA
→ València/Valencia
```

Aliases are explicit mappings.

They are not generated automatically from approximate string matching.

---

## 5.3 Canonical Geography Fields

Where province-level normalization is applicable, the canonical representation
uses:

```text
province_code
province_name
autonomous_community_code
autonomous_community_name
```

The original source geographical information can remain available separately
for traceability.

CNIG remains the authoritative territorial master.

---

# 6. Coordinate Normalization

AEMET station coordinates are converted to decimal coordinates.

Normalized coordinates must satisfy:

```text
latitude  ∈ [-90, 90]
longitude ∈ [-180, 180]
```

Invalid coordinates are treated as technical data-quality incidents.

The current validated AEMET point catalogue contains:

```text
926 stations
```

The same station catalogue supplies the point locations used by Open-Meteo.

---

# 7. Temporal Normalization

Silver normalizes temporal fields to explicit timestamp representations while
preserving the real source granularity.

The current temporal families are:

```text
AEMET current observations
→ recent/current observation timestamps

Open-Meteo hourly
→ 1 hour

Open-Meteo 15-minute
→ 15 minutes

ESIOS generation
→ 1 hour

ESIOS installed capacity
→ monthly
```

Missing mandatory timestamps are not generated.

Invalid mandatory timestamps are rejected during processing.

---

# 8. Natural Keys

The final approved natural keys are:

| Silver table | Natural key |
|---|---|
| `silver_aemet_stations` | `station_id` |
| `silver_aemet_current_observations` | `station_id + observation_timestamp` |
| `silver_open_meteo_hourly` | `station_id + observation_timestamp` |
| `silver_open_meteo_15min` | `station_id + observation_timestamp` |
| `silver_cnig_provinces` | `province_code` |
| `silver_cnig_autonomous_communities` | `autonomous_community_code` |
| `silver_cnig_municipalities` | `municipality_ine_code` |
| `silver_esios_energy_hourly` | `indicator_id + esios_geo_id + observation_timestamp` |
| `silver_esios_installed_capacity_monthly` | `indicator_id + esios_geo_id + observation_timestamp` |

Records with invalid mandatory natural-key fields must not be persisted as
canonical Silver observations.

---

# 9. Partitioning Strategy

The final partitioning strategy follows the temporal characteristics of each
dataset.

| Table | Partitioning |
|---|---|
| `silver_aemet_stations` | none |
| `silver_aemet_current_observations` | day of `observation_timestamp` |
| `silver_open_meteo_hourly` | day of `observation_timestamp` |
| `silver_open_meteo_15min` | day of `observation_timestamp` |
| `silver_cnig_provinces` | none |
| `silver_cnig_autonomous_communities` | none |
| `silver_cnig_municipalities` | none |
| `silver_esios_energy_hourly` | day of `observation_timestamp` |
| `silver_esios_installed_capacity_monthly` | month of `observation_timestamp` |

Reference masters are therefore not unnecessarily partitioned.

Time-series datasets use temporal Iceberg partitioning appropriate to their
source grain.

---

# 10. AEMET Silver Design

## 10.1 `silver_aemet_stations`

Purpose:

```text
official meteorological station master
```

Current validated cardinality:

```text
926 rows
```

Natural key:

```text
station_id
```

The transformation normalizes the structural station identifier and
coordinates while preserving the AEMET source attributes required by the
platform.

The station master provides the relationship between AEMET observations and
geography and supplies the location catalogue used for Open-Meteo.

Where applicable, station province information is normalized against CNIG.

Partitioning:

```text
none
```

---

## 10.2 `silver_aemet_current_observations`

Purpose:

```text
recent/current official meteorological observations
```

Natural key:

```text
station_id
+
observation_timestamp
```

The transformation performs minimal structural normalization such as:

```text
idema
→ station_id

fint
→ observation_timestamp

lat
→ latitude

lon
→ longitude
```

The original AEMET meteorological fields are preserved rather than being
prematurely renamed into Gold analytical metrics.

AEMET current observations are station-level observations.

They are not presented as arbitrary historical observations.

Geographical resolution required for Province-level Gold aggregation can use
the station master rather than manufacturing geography inside the source
observation.

Partitioning:

```text
day(observation_timestamp)
```

Historical evidence from the earlier E2E execution:

```text
2026-01-10 → 2026-01-15
```

contained:

```text
9786 rows
```

in this table.

This row count belongs to that specific execution and is not a permanent table
cardinality.

That execution predates the final `historical_reload` policy. In the final
orchestration design, AEMET `current_observations` is excluded from arbitrary
historical reconstruction.

---

# 11. Open-Meteo Silver Design

Open-Meteo uses the AEMET station catalogue as its point catalogue.

The current validated location count is:

```text
926
```

Open-Meteo remains independent from AEMET in Silver.

The two meteorological providers are integrated only later in Gold.

---

## 11.1 `silver_open_meteo_hourly`

Granularity:

```text
Station × hour
```

Natural key:

```text
station_id
+
observation_timestamp
```

The table contains normalized station and geographical information together
with hourly meteorological observations.

Variables used by the analytical flow include information such as:

```text
temperature_2m
relative_humidity_2m
precipitation
shortwave_radiation
direct_normal_irradiance
```

Additional validated Open-Meteo source fields may remain available in Silver
even when Gold does not use them directly.

Canonical geographical attributes include, where applicable:

```text
province_code
province_name
autonomous_community_code
autonomous_community_name
```

Partitioning:

```text
day(observation_timestamp)
```

Validated row count for the historical E2E execution described below:

```text
133344 rows
```

for:

```text
926 stations
×
144 hourly observations
```

over:

```text
2026-01-10 → 2026-01-15
```

This is execution-specific evidence and not a permanent table cardinality.

---

## 11.2 `silver_open_meteo_15min`

Granularity:

```text
Station × 15 minutes
```

Natural key:

```text
station_id
+
observation_timestamp
```

The table retains high-frequency meteorological variables required downstream,
including:

```text
wind_speed_80m
wind_direction_80m

wind_speed_120m
wind_direction_120m

shortwave_radiation
direct_normal_irradiance
```

along with the general meteorological and location attributes used by the
platform.

The 15-minute data remains at its original resolution in Silver.

Hourly aggregation is performed in Gold where required.

Partitioning:

```text
day(observation_timestamp)
```

Validated row count for the historical E2E execution described below:

```text
533376 rows
```

which corresponds exactly to:

```text
926 stations
×
576 observations
```

over:

```text
2026-01-10 → 2026-01-15
```

This is execution-specific evidence and not a permanent table cardinality.

---

# 12. CNIG Silver Design

CNIG / IGN provides the canonical geographical master.

The three normalized tables are:

```text
silver_cnig_provinces
silver_cnig_autonomous_communities
silver_cnig_municipalities
```

---

## 12.1 `silver_cnig_provinces`

Natural key:

```text
province_code
```

The normalized table includes the canonical province and autonomous-community
relationship.

Validated cardinality:

```text
52 rows
```

Partitioning:

```text
none
```

---

## 12.2 `silver_cnig_autonomous_communities`

The table is derived from the canonical province master.

Natural key:

```text
autonomous_community_code
```

Validated cardinality:

```text
19 rows
```

Partitioning:

```text
none
```

---

## 12.3 `silver_cnig_municipalities`

Natural key:

```text
municipality_ine_code
```

The approved CNIG mapping includes:

```text
municipality_ine_code
← COD_INE

municipality_code
← COD_GEO

province_code
← COD_PROV
```

`municipality_code` is retained as source geographical information but is not
used as the natural key.

The validated CNIG source contains records with:

```text
COD_GEO = 00000
```

therefore `COD_GEO` is not globally unique.

The approved canonical municipality key is:

```text
COD_INE
→ municipality_ine_code
```

Validated cardinality:

```text
8132 rows
```

Official codes remain strings.

Partitioning:

```text
none
```

---

# 13. ESIOS Silver Design

The final ESIOS Silver scope contains only two physical tables:

```text
silver_esios_energy_hourly
silver_esios_installed_capacity_monthly
```

The previously implemented:

```text
silver_esios_power_5min
```

is no longer part of the final model.

The final indicator configuration contains:

```text
11 hourly generation indicators
9 monthly installed-capacity indicators
```

and is maintained in:

```text
config/esios_indicators.json
```

---

## 13.1 Common ESIOS Observation Structure

The normalized ESIOS observation structure retains information such as:

```text
indicator_id
dataset
indicator_name
indicator_short_name

magnitude_id
magnitude_name

time_id
time_name

observation_timestamp
source_datetime
tz_time

esios_geo_id
esios_geo_name

value
values_updated_at

source
ingestion_timestamp
```

Where province-level normalization is applicable to hourly generation,
canonical geographical fields can additionally be present:

```text
province_code
province_name
autonomous_community_code
autonomous_community_name
```

Indicator-level structures such as:

```text
composited
disaggregated
step_type
geos
```

remain in Bronze rather than being unnecessarily repeated in every Silver
observation row.

---

## 13.2 `silver_esios_energy_hourly`

Purpose:

```text
normalized hourly electricity-generation observations
```

Natural key:

```text
indicator_id
+
esios_geo_id
+
observation_timestamp
```

Current active scope:

```text
11 indicators
```

The table preserves the real ESIOS observation value and geographical identity.

Province normalization is applied only where the source geography supports it.

Silver does not fabricate Province-level observations for source records that do
not provide Province-level geography.

Partitioning:

```text
day(observation_timestamp)
```

Historical E2E evidence from the validated execution contained:

```text
38443 rows
```

This is an execution-specific row count and must not be interpreted as a
permanent table cardinality.

---

## 13.3 `silver_esios_installed_capacity_monthly`

Purpose:

```text
normalized monthly installed-capacity observations
```

Natural key:

```text
indicator_id
+
esios_geo_id
+
observation_timestamp
```

Current active scope:

```text
9 indicators
```

Installed capacity represents:

```text
power
```

The validated analytical geography is:

```text
Autonomous Community
```

Silver does not distribute those values artificially to provinces.

Partitioning:

```text
month(observation_timestamp)
```

Historical E2E evidence from the validated execution contained:

```text
123 rows
```

This is an execution-specific row count and must not be interpreted as a
permanent table cardinality.

---

# 14. ESIOS Units

The platform maintains a strict distinction between:

```text
MW
```

and:

```text
MWh
```

They represent different physical quantities:

```text
MW
→ power
```

```text
MWh
→ energy
```

For an interval of exactly one hour, a power value expressed in MW can be
numerically equal to the corresponding energy in MWh for that interval, but the
physical quantity and unit remain conceptually different.

Silver preserves the validated ESIOS source magnitude, time and value semantics.

The implementation must not infer a unit solely from temporal granularity.

Installed capacity remains a power-capacity measurement, while hourly generation
retains the energy semantics validated for the corresponding source indicator
metadata.

---

# 15. Meteorological Source Separation

AEMET and Open-Meteo coexist as independent normalized sources in Silver.

```text
AEMET
→ official stations
→ recent/current official observations
```

```text
Open-Meteo
→ reproducible historical meteorological data
→ hourly data
→ 15-minute data
```

Silver does not apply the meteorological source hierarchy.

The approved downstream rule belongs to Gold:

```text
AEMET
→ principal / preferred meteorological source

Open-Meteo
→ enrichment / fallback source
```

Silver keeps both providers independently normalized so that Gold can apply the
validated source-selection logic without destroying source traceability.

---

# 16. Silver-to-Gold Contract

Silver exposes reusable normalized inputs to Gold.

### Main meteorological inputs

```text
silver_aemet_stations
silver_aemet_current_observations
silver_open_meteo_hourly
silver_open_meteo_15min
```

### Main energy inputs

```text
silver_esios_energy_hourly
silver_esios_installed_capacity_monthly
```

### Geography inputs

```text
silver_cnig_provinces
silver_cnig_autonomous_communities
silver_cnig_municipalities
```

Gold subsequently produces:

```text
gold_fact_province_hourly
gold_fact_installed_capacity_monthly
gold_dim_geography
gold_dim_time
```

---

# 17. Silver Data Quality

The Silver layer applies technical quality controls including:

1. null natural keys;
2. invalid or missing mandatory timestamps;
3. coordinates outside valid ranges;
4. unresolved geographical correspondence where matching is required;
5. duplicate natural keys;
6. invalid mandatory fields;
7. temporal coverage anomalies where applicable;
8. structural or physical inconsistencies.

The guiding principles are:

```text
Bronze remains preserved.

Values are not invented.

Allowed NULL values remain NULL.

Duplicates are resolved using natural keys.

Temporal gaps are detected, not automatically filled.

Plausible source outliers are preserved unless a validated rule rejects them.
```

---

# 18. Deduplication

Bronze can contain overlapping observations from repeated acquisitions.

Silver produces the canonical observation set.

For the final historical orchestration path:

```text
historical_reload
```

the validated write policy is:

```text
LAKEHOUSE_WRITE_POLICY=insert-only
```

Under PRESERVE, only missing natural keys are inserted.

Other workflows keep their default upsert behaviour unless explicitly configured
otherwise.

Conceptually:

```text
Bronze object A ──┐
                  │
Bronze object B ──┼──► Spark
                  │       │
Bronze object C ──┘       ▼
                    Natural-key
                    deduplication
                         │
                         ▼
                       Silver
```

Reprocessing the same Bronze input must not multiply rows with the same natural
key.

The final E2E validation confirmed consistent canonical Silver outputs.

---

# 19. Apache Iceberg Persistence

Silver is implemented using Apache Iceberg on MinIO.

The physical relationship is:

```text
Apache Spark
     │
     ▼
Apache Iceberg
     │
     ▼
   MinIO
     ▲
     │
   Trino
```

Spark is responsible for creating and writing the Silver tables.

Trino provides independent SQL access to the same persisted Iceberg tables.

Bronze remains raw MinIO object storage and is not converted into an Iceberg
landing layer.

---

# 20. Final E2E Silver Validation

A complete real-data execution was performed for the historical interval:

```text
2026-01-10 → 2026-01-15
```

This execution is retained as historical evidence.

It predates the final `historical_reload` policy because it included AEMET
`current_observations`, which the final historical workflow now excludes.

The row counts below therefore describe that concrete execution and must not be
interpreted as permanent table cardinalities.

The final Silver namespace contained exactly:

```text
9 tables
```

Trino returned the following row counts:

| Table | Rows |
|---|---:|
| `silver_aemet_stations` | 926 |
| `silver_aemet_current_observations` | 9786 |
| `silver_open_meteo_hourly` | 133344 |
| `silver_open_meteo_15min` | 533376 |
| `silver_cnig_provinces` | 52 |
| `silver_cnig_autonomous_communities` | 19 |
| `silver_cnig_municipalities` | 8132 |
| `silver_esios_energy_hourly` | 38443 |
| `silver_esios_installed_capacity_monthly` | 123 |

Open-Meteo coverage matched the expected historical interval exactly:

```text
926 × 144
= 133344 hourly rows
```

and:

```text
926 × 576
= 533376 15-minute rows
```

ESIOS also contained actual observations rather than empty placeholder
datasets.

---

# 21. Catalog Validation

The final Silver tables are persisted in Apache Iceberg and queryable through
Trino using the:

```text
iceberg.silver
```

namespace.

The exact final table inventory is:

```text
silver_aemet_current_observations
silver_aemet_stations
silver_cnig_autonomous_communities
silver_cnig_municipalities
silver_cnig_provinces
silver_esios_energy_hourly
silver_esios_installed_capacity_monthly
silver_open_meteo_15min
silver_open_meteo_hourly
```

No obsolete Silver table is part of the current final catalog.

---

# 22. Removed Silver Flows

The following physical tables were deliberately removed from the final model:

### AEMET daily climatology

```text
silver_aemet_daily_climatology
```

Reason:

The final meteorological analytical flow is based on AEMET current observations
and Open-Meteo historical data.

---

### Open-Meteo historical forecast table

```text
silver_open_meteo_historical_forecast
```

Reason:

Historical 15-minute acquisition now feeds the unified:

```text
silver_open_meteo_15min
```

table rather than requiring a separate historical-forecast Silver table.

---

### ESIOS 5-minute power

```text
silver_esios_power_5min
```

Reason:

The final analytical model was simplified around:

```text
hourly electricity generation
monthly installed capacity
```

The earlier 5-minute experimental flow is therefore outside the final physical
Silver model.

---

# 23. Final Analytical Scope

The final Silver layer supports two principal downstream analytical products.

### Province × hour

Depending on the execution context, normalized inputs available to Gold include:

```text
AEMET current observations
Open-Meteo hourly
Open-Meteo 15-minute
ESIOS hourly generation
CNIG geography
```

AEMET `current_observations` is available for recent/current processing but is
excluded from the final arbitrary historical `historical_reload` reconstruction
workflow.

Gold integrates the applicable sources into:

```text
gold_fact_province_hourly
```

---

### Autonomous Community × month

Input:

```text
ESIOS monthly installed capacity
```

Gold produces:

```text
gold_fact_installed_capacity_monthly
```

The Silver layer does not artificially alter the source geography to force both
products into the same grain.

---

# 24. Current Silver Status

The current implementation status is:

```text
Final physical Silver tables
= 9

AEMET Silver
= VALIDATED

Open-Meteo Silver
= VALIDATED

CNIG Silver
= VALIDATED

ESIOS Silver
= VALIDATED

Natural-key deduplication
= VALIDATED

Canonical geographical normalization
= VALIDATED

Apache Iceberg persistence
= VALIDATED

Trino queryability
= VALIDATED

Real Bronze → Silver execution
= VALIDATED

Silver input to Gold
= VALIDATED

Real Bronze → Silver → Gold → Trino
= VALIDATED
```

The Silver layer is therefore implemented and validated for the final project
scope.