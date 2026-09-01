# Silver Layer — Implementation and Validation

## 1. Purpose

This document describes the implementation and technical validation of the
Silver layer of the Energy Lakehouse Platform.

The Silver layer transforms raw Bronze acquisitions into normalized, typed,
deduplicated and queryable Apache Iceberg datasets.

The implemented processing path is:

```text
Bronze / MinIO
      │
      ▼
Apache Spark / PySpark
      │
      ├── parsing
      ├── typing
      ├── timestamp normalization
      ├── geographical normalization
      ├── natural-key deduplication
      └── data-quality validation
      │
      ▼
Apache Iceberg / Silver
      │
      ▼
MinIO
      │
      ▼
Trino
```

Silver preserves the real source granularity.

Analytical aggregation, source precedence and meteorology-energy integration are
performed later in Gold.

---

## 2. Final Implemented Silver Model

The final physical Silver model contains exactly:

```text
9 Apache Iceberg tables
```

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

The following previous tables are no longer part of the final physical model:

```text
silver_aemet_daily_climatology
silver_open_meteo_historical_forecast
silver_esios_power_5min
```

They must therefore not be counted as current Silver products.

---

## 3. Historical Silver Row Counts

An earlier validated real end-to-end execution produced the following Silver
counts:

| Table | Rows |
|---|---:|
| `silver_aemet_stations` | 926 |
| `silver_aemet_current_observations` | 9,786 |
| `silver_open_meteo_hourly` | 133,344 |
| `silver_open_meteo_15min` | 533,376 |
| `silver_cnig_provinces` | 52 |
| `silver_cnig_autonomous_communities` | 19 |
| `silver_cnig_municipalities` | 8,132 |
| `silver_esios_energy_hourly` | 38,443 |
| `silver_esios_installed_capacity_monthly` | 123 |

These counts were obtained from persisted Silver tables through Trino for that
concrete execution.

They are retained as historical validation evidence and must not be interpreted
as permanent Silver table cardinalities.

The execution also predates the final `historical_reload` policy because it
included AEMET `current_observations`, which the final historical workflow now
excludes.

---

## 4. Real Validation Interval

The principal historical end-to-end validation used:

```text
2026-01-10 → 2026-01-15
```

The interval contains six complete days.

This period was selected because the final configured ESIOS indicator set was
verified to contain actual data for the requested interval.

The same real Bronze acquisition was subsequently processed through Silver and
Gold.

This execution remains valid historical E2E evidence, but it predates the final
`historical_reload` orchestration policy because AEMET `current_observations`
was still included in that earlier run.

---

## 5. Apache Iceberg Persistence

Silver is physically implemented using Apache Iceberg.

The storage relationship is:

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

Spark creates and writes the managed Silver tables.

Trino accesses the same persisted Iceberg catalog independently for SQL
validation and analytical consumption.

Silver data is stored in the Lakehouse warehouse under the Silver namespace.

The persisted tables contain the Apache Iceberg physical structures required
for table management, including:

```text
data files
metadata
snapshots
manifests
```

Bronze itself remains raw object storage and is not implemented as Iceberg.

---

## 6. Catalog Validation

The final Silver namespace is available through:

```text
iceberg.silver
```

The catalog exposes exactly the final nine tables:

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

The obsolete Silver tables are not part of the current final catalog.

**Status: VALIDATED**

---

## 7. Partitioning

The implemented partitioning follows the real temporal nature of each dataset.

| Table | Partitioning |
|---|---|
| `silver_aemet_stations` | none |
| `silver_aemet_current_observations` | day |
| `silver_open_meteo_hourly` | day |
| `silver_open_meteo_15min` | day |
| `silver_cnig_provinces` | none |
| `silver_cnig_autonomous_communities` | none |
| `silver_cnig_municipalities` | none |
| `silver_esios_energy_hourly` | day |
| `silver_esios_installed_capacity_monthly` | month |

Reference/master tables do not require temporal partitioning.

Observation tables use their normalized observation timestamp.

---

## 8. Natural Keys

The final Silver natural keys are:

| Table | Natural key |
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

These keys are used to prevent repeated Bronze acquisitions from multiplying the
same logical Silver record.

---

## 9. AEMET Implementation

### `silver_aemet_stations`

The AEMET station master is normalized into:

```text
silver_aemet_stations
```

The current validated catalogue contains:

```text
926 rows
```

The structural source identifier is normalized to:

```text
station_id
```

Coordinates are converted into numeric latitude and longitude fields.

The station master is also used as the meteorological location catalogue for
Open-Meteo.

Where source province information is available, canonical geography is resolved
against CNIG.

Natural key:

```text
station_id
```

---

### `silver_aemet_current_observations`

The current AEMET observation dataset is normalized into:

```text
silver_aemet_current_observations
```

Structural normalization includes:

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

The remaining AEMET meteorological source fields are preserved without
prematurely translating them into Gold analytical metrics.

Historical row count for the earlier validated E2E execution:

```text
9786
```

This count belongs to that execution only.

In the final orchestration design, AEMET `current_observations` is excluded from
arbitrary historical reconstruction by `historical_reload`.

Natural key:

```text
station_id
+
observation_timestamp
```

The source contains recent/current observations and is not interpreted as a
generic historical reconstruction source.

---

## 10. Open-Meteo Implementation

Open-Meteo acquisition uses the AEMET station catalogue.

Current validated location count:

```text
926
```

The final Silver implementation contains two Open-Meteo tables.

---

### `silver_open_meteo_hourly`

Grain:

```text
Station × hour
```

Natural key:

```text
station_id
+
observation_timestamp
```

The table includes normalized meteorological information such as:

```text
temperature_2m
relative_humidity_2m
precipitation
shortwave_radiation
direct_normal_irradiance
```

as well as station and canonical geographical information.

The validated historical row count is:

```text
133344
```

This matches exactly:

```text
926 locations
×
144 hourly observations
=
133344 rows
```

for the validated six-day interval.

---

### `silver_open_meteo_15min`

Grain:

```text
Station × 15 minutes
```

Natural key:

```text
station_id
+
observation_timestamp
```

The dataset retains the high-frequency variables required for Gold, including:

```text
wind_speed_80m
wind_direction_80m

wind_speed_120m
wind_direction_120m

shortwave_radiation
direct_normal_irradiance
```

The validated row count is:

```text
533376
```

This matches exactly:

```text
926 locations
×
576 observations
=
533376 rows
```

for the validated six-day interval.

No 15-minute-to-hour aggregation is performed in Silver.

---

## 11. Open-Meteo Temporal Validation

For:

```text
2026-01-10 → 2026-01-15
```

the expected temporal coverage was:

### Hourly

```text
6 × 24
= 144 records/location
```

### 15-minute

```text
6 × 24 × 4
= 576 records/location
```

The persisted Silver counts exactly matched both calculations.

Therefore:

```text
silver_open_meteo_hourly
= COMPLETE FOR VALIDATED INTERVAL

silver_open_meteo_15min
= COMPLETE FOR VALIDATED INTERVAL
```

**Status: VALIDATED**

---

## 12. CNIG Implementation

CNIG / IGN is the canonical geographical master of the platform.

The final Silver geographical model contains:

```text
silver_cnig_provinces
silver_cnig_autonomous_communities
silver_cnig_municipalities
```

Validated cardinalities are:

```text
provinces
= 52

autonomous communities
= 19

municipalities
= 8132
```

---

### `silver_cnig_provinces`

Natural key:

```text
province_code
```

Official codes are stored as strings.

The table provides the canonical relationship between:

```text
Province
→ Autonomous Community
```

---

### `silver_cnig_autonomous_communities`

Derived from the canonical CNIG territorial information.

Natural key:

```text
autonomous_community_code
```

Validated rows:

```text
19
```

---

### `silver_cnig_municipalities`

The validated mapping includes:

```text
municipality_ine_code
← COD_INE

municipality_code
← COD_GEO

province_code
← COD_PROV
```

Natural key:

```text
municipality_ine_code
```

`municipality_code` is not used as the canonical natural key because the source
contains repeated:

```text
COD_GEO = 00000
```

values.

Validated rows:

```text
8132
```

---

## 13. Geographical Normalization

CNIG is used to resolve canonical province and Autonomous Community information.

The implemented resolution model is:

```text
Source geographical value
        │
        ▼
Deterministic normalization
        │
        ▼
Controlled alias fallback
        │
        ▼
CNIG canonical Province
        │
        ▼
CNIG Autonomous Community
```

Deterministic normalization handles differences in:

- capitalization;
- whitespace;
- diacritics.

No fuzzy geographical inference is used.

---

## 14. Province Alias Configuration

Known naming differences are maintained in:

```text
config/province_aliases.json
```

Validated mappings include:

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

These aliases are explicit controlled mappings and are applied only after
deterministic normalization fails.

---

## 15. Canonical Geography

Where applicable, normalized datasets use:

```text
province_code
province_name
autonomous_community_code
autonomous_community_name
```

The source geographical representation can remain available separately for
traceability.

The key principle is:

```text
Source geography is normalized,
not invented.
```

Province-level detail is only retained or derived when justified by the source
and canonical reference.

---

## 16. ESIOS Implementation

The final ESIOS Silver implementation contains two physical tables:

```text
silver_esios_energy_hourly
silver_esios_installed_capacity_monthly
```

The obsolete:

```text
silver_esios_power_5min
```

is not part of the final model.

The final active indicator scope is configured in:

```text
config/esios_indicators.json
```

and contains:

```text
11 hourly generation indicators
9 monthly installed-capacity indicators
```

---

## 17. `silver_esios_energy_hourly`

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

The normalized structure retains information such as:

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

esios_geo_id
esios_geo_name

value

source
ingestion_timestamp
```

The source geography is preserved.

Province-level information is not fabricated for records that do not provide
Province-level geography.

Historical row count for the earlier validated E2E execution:

```text
38443
```

This count is execution-specific and is not a permanent table cardinality.

---

## 18. `silver_esios_installed_capacity_monthly`

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

Historical row count for the earlier validated E2E execution:

```text
123
```

This count is execution-specific and is not a permanent table cardinality.

The validated analytical geography is:

```text
Autonomous Community
```

Installed-capacity values remain power metrics expressed in:

```text
MW
```

They are not artificially distributed to provinces.

---

## 19. ESIOS Source Semantics

Silver preserves the ESIOS magnitude and time metadata required to maintain
correct analytical semantics.

The project distinguishes strictly between:

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

For an interval of exactly one hour, a power value in MW can be numerically
equal to the corresponding energy in MWh over that hour, but the physical
quantity and unit remain conceptually different.

The implementation therefore preserves validated source magnitude and temporal
semantics and does not infer units only from observation granularity.

Installed capacity remains a power-capacity metric.

The validated hourly ESIOS generation indicators retain their corresponding
energy semantics.

---

## 20. ESIOS NO_DATA Handling

An ESIOS response containing:

```text
indicator.values = []
```

is a valid source response representing:

```text
NO_DATA
```

It must not be converted into fabricated observations or zero-valued
measurements.

Therefore:

```text
NO_DATA != zero-valued measurement
```

and:

```text
NULL != 0
```

For the earlier historical validation interval:

```text
2026-01-10 → 2026-01-15
```

the configured datasets used by that concrete E2E execution contained actual
observations and could therefore be processed through:

```text
Bronze
→ Silver
→ Gold
```

That historical availability must not be generalized to every future requested
interval.

---

## 21. Data Quality Validation

Silver applies quality controls including:

```text
null natural keys
invalid mandatory timestamps
invalid coordinates
unresolved canonical geography where applicable
duplicate natural keys
mandatory-field validation
temporal coverage anomalies
structural inconsistencies
```

The quality principles are:

```text
NULL values are not automatically errors.

Missing values are not replaced with zero.

Missing observations are not synthetically generated.

Plausible source values remain available.

Bronze remains preserved.
```

---

## 22. Deduplication and Idempotency

Repeated or overlapping Bronze acquisitions can contain the same logical source
observation.

Silver therefore deduplicates using the natural keys defined for each table.

For the final historical orchestration path:

```text
historical_reload
```

the validated downstream write policy is:

```text
LAKEHOUSE_WRITE_POLICY=insert-only
```

Under PRESERVE, only missing natural keys are inserted.

Other workflows keep their default upsert behaviour unless explicitly
configured otherwise.

Conceptually:

```text
Bronze A ──┐
           │
Bronze B ──┼──► PySpark
           │       │
Bronze C ──┘       ▼
               Natural key
                   │
                   ▼
              Deduplication
                   │
                   ▼
                 Silver
```

Reprocessing input data must not create multiple canonical rows for the same
natural key.

Natural-key uniqueness is part of the Silver validation strategy.

---

## 23. Silver Automated Tests

The Silver implementation includes tests covering areas such as:

```text
common Silver utilities
AEMET transformations
Open-Meteo transformations
CNIG transformations
ESIOS transformations
geographical normalization
Iceberg integration
physical schemas
persisted Silver data
end-to-end Silver processing
```

The latest validated complete Silver test suite finished with:

```text
85 passed
```

No failing Silver tests remained in that validated execution.

**Silver automated test status: 85 PASSED**

---

## 24. Historical Silver Reconstruction Evidence

The final nine-table Silver model was reconstructed and populated from real
Bronze data in the earlier validated E2E execution.

The resulting physical catalog contained exactly:

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

This confirms that the implemented Silver processing can create and populate
the complete nine-table model from real Bronze source data.

The row counts above remain execution-specific historical evidence rather than
permanent table cardinalities.

---

## 25. Trino Validation

The persisted Silver tables were queried through Trino.

Trino exposed exactly the nine final tables in:

```text
iceberg.silver
```

and returned the expected row counts for the validated execution.

This validates the shared catalog path:

```text
Spark
  │
  ▼
Iceberg / MinIO
  │
  ▼
Trino
```

The same Silver tables subsequently served as input to Gold processing.

**Status: VALIDATED**

---

## 26. Bronze-to-Silver End-to-End Validation

The validated Silver processing path was:

```text
Real Bronze objects in MinIO
          │
          ▼
     Apache Spark
          │
          ▼
Silver transformations
          │
          ▼
Natural-key handling
          │
          ▼
Geographical normalization
          │
          ▼
Apache Iceberg persistence
          │
          ▼
        Trino
```

All nine final Silver tables were successfully created, populated and queried.

**Status: VALIDATED**

---

## 27. Silver-to-Gold Compatibility

The final Silver data was subsequently consumed by the implemented Gold layer.

The complete validated path became:

```text
Bronze
  │
  ▼
Silver
  │
  ▼
Gold
  │
  ▼
Trino
```

Gold persistence completed successfully using the nine-table Silver model.

The resulting final Gold model contains:

```text
gold_dim_geography
gold_dim_time
gold_fact_installed_capacity_monthly
gold_fact_province_hourly
```

This validates Silver not only in isolation but also as the normalized contract
required by the final analytical layer.

---

## 28. Historical Gold Evidence Derived from Silver

The earlier validated E2E execution produced:

```text
8147 Province × hour rows
```

in the main Gold fact, with:

```text
8100 rows containing weather

6768 rows containing energy

6721 rows containing both weather and energy
```

and:

```text
0 duplicate Province × hour keys
```

The installed-capacity fact contained:

```text
19 Autonomous Community × month rows
```

with:

```text
0 duplicate keys
```

These figures are execution-specific historical evidence.

They are not permanent Gold cardinalities.

In particular, the final structural validation of `gold_dim_geography` was
performed later and established:

```text
PROVINCE = 52
AUTONOMOUS_COMMUNITY = 19
COUNTRY = 1
PENINSULA = 1

TOTAL = 73
```

This later structural validation supersedes earlier execution-specific
geographical row counts without invalidating the historical E2E evidence above.

---

## 29. Removed Previous Silver Components

During implementation, the model was simplified to match the final analytical
scope.

The following previous Silver tables were removed.

### `silver_aemet_daily_climatology`

The final historical meteorological flow is supplied by Open-Meteo while AEMET
is retained for station master and current observations.

---

### `silver_open_meteo_historical_forecast`

Historical 15-minute observations now feed the unified:

```text
silver_open_meteo_15min
```

dataset.

A separate physical historical-forecast Silver table is no longer required.

---

### `silver_esios_power_5min`

The final ESIOS analytical scope is:

```text
hourly electricity generation
+
monthly installed capacity
```

The previous 5-minute ESIOS physical Silver flow is therefore outside the
final model.

---

## 30. Final Validation Result

The final Silver implementation has been technically validated.

The following combines final structural/test validation with row counts retained
from the earlier real E2E execution:

```text
Final Silver tables
= 9

AEMET station rows in validated catalogue
= 926

AEMET current-observation rows in earlier E2E execution
= 9786

Open-Meteo hourly rows in earlier six-day E2E execution
= 133344

Open-Meteo 15-minute rows in earlier six-day E2E execution
= 533376

CNIG province rows
= 52

CNIG Autonomous Community rows
= 19

CNIG municipality rows
= 8132

ESIOS hourly rows in earlier E2E execution
= 38443

ESIOS monthly rows in earlier E2E execution
= 123

Silver automated tests
= 85 PASSED

Apache Iceberg persistence
= VALIDATED

Trino catalog access
= VALIDATED

Bronze → Silver
= VALIDATED

Silver → Gold
= VALIDATED

Bronze → Silver → Gold → Trino
= VALIDATED

historical_reload E2E runtime
= VALIDATED
```

The Silver layer is therefore implemented, persisted and technically validated
for the final Energy Lakehouse Platform scope.