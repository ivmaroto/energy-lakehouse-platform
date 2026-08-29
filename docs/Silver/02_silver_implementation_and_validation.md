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

## 3. Final Silver Row Counts

The current real end-to-end validation produced the following Silver counts:

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

These counts were obtained from the final persisted Silver tables through
Trino.

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

Current validated row count:

```text
9786
```

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

Current validated row count:

```text
38443
```

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

Current validated row count:

```text
123
```

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

The project distinguishes between:

```text
MW
```

and:

```text
MWh
```

Installed capacity represents:

```text
power
→ MW
```

Hourly generation feeds the Gold analytical energy metrics represented as:

```text
energy
→ MWh
```

The two units are never treated as interchangeable physical quantities.

---

## 20. ESIOS Empty-Data Protection

The ingestion layer rejects an ESIOS acquisition when:

```text
indicator.values = []
```

Therefore, the final current Silver validation is based on actual ESIOS
observations rather than successful-but-empty API responses.

For the historical validation interval:

```text
2026-01-10 → 2026-01-15
```

all configured final ESIOS datasets were verified to contain data.

This enabled the real:

```text
Bronze
→ Silver
→ Gold
```

validation.

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

## 24. Final Silver Reconstruction

The current Silver model was reconstructed and populated from real Bronze data.

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
the complete final model from the current Bronze source scope.

---

## 25. Trino Validation

The persisted Silver tables were queried through Trino.

Trino exposed exactly the nine final tables in:

```text
iceberg.silver
```

and returned the expected row counts.

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

## 28. Final Gold Evidence Derived from Silver

The main Gold fact generated from the final Silver model contains:

```text
8147 Province × hour rows
```

with:

```text
8100 rows containing weather

6768 rows containing energy

6721 rows containing both weather and energy
```

and:

```text
0 duplicate Province × hour keys
```

The installed-capacity fact contains:

```text
19 Autonomous Community × month rows
```

with:

```text
0 duplicate keys
```

This confirms that the final Silver datasets provide valid normalized input to
the intended analytical products.

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

Current evidence confirms:

```text
Final Silver tables
= 9

AEMET station rows
= 926

AEMET current-observation rows
= 9786

Open-Meteo hourly rows
= 133344

Open-Meteo 15-minute rows
= 533376

CNIG province rows
= 52

CNIG Autonomous Community rows
= 19

CNIG municipality rows
= 8132

ESIOS hourly rows
= 38443

ESIOS monthly rows
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
```

The Silver layer is therefore implemented, persisted and technically validated
for the final Energy Lakehouse Platform scope.