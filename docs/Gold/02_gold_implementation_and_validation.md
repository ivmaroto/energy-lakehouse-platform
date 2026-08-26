# Gold Layer Implementation and Validation

## 1. Purpose

This document records the implementation and validation evidence for the
Gold layer of the **Energy Lakehouse Platform**.

It complements `01_gold_design.md`.

The design document defines the approved Gold model, grains, schemas,
transformations, integration rules, partitioning, quality controls, and
load strategy. This document records what has actually been implemented
and validated.

Only completed and evidenced work is marked as validated.

## 1.1 Validated Implementation Architecture

The validated Gold execution path is:

`Silver Iceberg → Spark Gold transformations → Gold Iceberg → MinIO → Trino`

The Gold layer contains four fact tables and two conformed dimensions.
Spark performs the approved Silver-to-Gold transformations and persists
the resulting datasets as Apache Iceberg tables whose warehouse is
stored in MinIO. Trino reads the same Iceberg catalog for downstream SQL
consumption.

The validated downstream boundary at this checkpoint is Trino. The
subsequent visualization layer is not claimed as validated here.

At the current checkpoint:

- **4.5.1 — Gold structure preparation:** completed.
- **4.5.2 — Silver → Gold transformations:** completed and validated.
- **4.5.3 — Gold automated tests:** completed and validated.
- **4.5.4 — Physical Gold table creation:** completed and validated.
- **4.5.5 — Real Gold persistence:** completed and validated.
- **4.5.6 — Persisted Gold data quality:** completed and validated.
- **4.5.7 — Analytical integration:** completed and validated.
- **4.5.8 — Trino validation:** completed and validated.
- **4.5.9 — End-to-end validation:** completed and validated.
- **4.5.10 — Documentation:** completed by consolidating the validated evidence in `01_gold_design.md` and this document.

---

# 2. 4.5.1 — Gold Structure Preparation

## 2.1 Status

**COMPLETED**

## 2.2 Implemented Structure

The Gold implementation uses:

- `spark/jobs/gold/`
- `tests/gold/`
- `config/gold_config.json`

The Gold code is mounted inside the Spark container through the existing
project Docker configuration.

Validated container paths include:

- `/opt/spark/jobs`
- `/opt/spark/tests`
- `/opt/config`
- `/opt/spark/conf`

The Spark working directory is:

`/opt/spark/work-dir`

## 2.3 Gold Configuration

The validated Gold configuration is:

```json
{
  "esios_time_gap_hours": 1,
  "peninsula_excluded_province_codes": [
    "07",
    "35",
    "38",
    "51",
    "52"
  ]
}
```

The ESIOS temporal gap is therefore externally configured and is not
hardcoded inside the Gold transformations.

The validated non-peninsular province codes are:

- `07` — Illes Balears
- `35` — Las Palmas
- `38` — Santa Cruz de Tenerife
- `51` — Ceuta
- `52` — Melilla

---

# 3. 4.5.2 — Silver → Gold Transformations

## 3.1 Status

**COMPLETED AND VALIDATED**

## 3.2 Implemented Gold Modules

The transformation implementation includes the following Gold modules:

- `spark/jobs/gold/common.py`
- `spark/jobs/gold/geography.py`
- `spark/jobs/gold/temporal.py`
- `spark/jobs/gold/metrics.py`
- `spark/jobs/gold/weather.py`
- `spark/jobs/gold/province_hourly_integration.py`
- `spark/jobs/gold/country_15min_integration.py`

The implemented logic covers:

- Silver table reading;
- temporal preparation;
- geographical aggregation and normalization;
- energy metric selection;
- weather metric preparation;
- Province × hour weather-energy integration;
- Spain/Peninsula × 15-minute weather-energy integration;
- duplicate protection;
- approved null, sign, and unit rules.

---

# 4. Temporal Transformation Validation

## 4.1 ESIOS Temporal Alignment

The implemented rule is:

`gold_timestamp = observation_timestamp + configured gap`

The current configured value is:

`esios_time_gap_hours = 1`

The transformation receives the gap as a parameter and does not embed a
fixed `+1 hour` inside the transformation logic.

## 4.2 Open-Meteo 15 Minutes → Hour

Open-Meteo wind observations are aggregated from 15-minute intervals to
hourly values per point.

Validated rule:

`4 × 15-minute observations → 1 hourly point observation`

Wind speed uses arithmetic average.

Wind direction uses circular mean.

## 4.3 ESIOS Power MW → Interval Energy MWh

For every real 5-minute ESIOS observation:

`energy_mwh_5min = power_mw × (5 / 60)`

The original power sign is preserved.

Examples validated in the automated tests include:

- `120 MW → 10 MWh`
- `-120 MW → -10 MWh`
- `0 MW → 0 MWh`

No absolute-value transformation is applied.

## 4.4 ESIOS 5 Minutes → 15 Minutes

Three real 5-minute interval-energy observations are aggregated into one
15-minute energy interval:

`energy_mwh_15min = SUM(three energy_mwh_5min values)`

The implementation records:

`source_interval_count`

The approved valid count is:

`3`

`SUM(power_mw)` is not used to represent 15-minute energy.

---

# 5. Geographical Transformation Validation

## 5.1 Canonical Geography

CNIG remains the canonical geographical master for:

- Province;
- Autonomous Community.

The validated national Gold keys are:

- Spain → `COUNTRY:ES`
- Peninsula → `PENINSULA:ES-PEN`

The two scopes are kept distinct.

## 5.2 Province Normalization

The real Gold geographical validation confirmed successful canonical
province resolution.

Validated result:

- canonical province matches: successful;
- unmatched province mappings in the validated Gold normalization: `0`;
- province source names with multiple matches: `0`.

## 5.3 Autonomous Community Normalization

The validated CNIG-based CCAA normalization produced:

- `19` canonical autonomous communities;
- unmatched CCAA mappings: `0`;
- CCAA source names with multiple matches: `0`.

Duplicate `autonomous_community_code` values in the canonical master are
treated as an error.

## 5.4 Peninsula Scope

The validated canonical geographical preparation contains 52
province-level entities.

After excluding:

- `07`
- `35`
- `38`
- `51`
- `52`

the validated Peninsula aggregation uses:

`47` eligible province entities.

Peninsula weather is calculated independently from province-level data.

Spain weather is never relabelled as Peninsula weather.

---

# 6. Weather Transformation Validation

## 6.1 Approved Gold Weather Metrics

The Gold weather preparation includes:

- `temperature`
- `humidity`
- `precipitation`
- `wind_speed_80m`
- `wind_direction_80m`
- `wind_speed_120m`
- `wind_direction_120m`
- `solar_radiation`
- `direct_normal_irradiance`

## 6.2 AEMET Fallback Policy

For:

- temperature;
- humidity;
- precipitation;

AEMET is preferred when the corresponding valid measurement is
available.

Open-Meteo is used only as fallback for the specific missing metric.

The fallback is metric-specific, not row-wide.

Missing values are not converted to zero.

## 6.3 AEMET Unresolved Stations

Real validation identified AEMET observations whose station IDs could
not be resolved through the available station catalogue.

Validated evidence:

- AEMET current observations: `9688`
- station catalogue rows: `921`
- unresolved observation rows: `584`
- unresolved station IDs: `49`
- resolved observation rows: `9104`

Gold does not invent a province for unresolved AEMET stations.

Those observations are excluded from the Province × hour Gold
aggregation while remaining available upstream.

## 6.4 Real Weather Integration Results

Validated real results include:

### AEMET Province × hour

- rows: `612`
- provinces: `51`

### Open-Meteo Province × hour

- rows: `4992`
- provinces: `52`

### Open-Meteo wind Point × hour

- rows: `88416`

### Open-Meteo wind Province × hour

- rows: `4992`
- provinces: `52`

### Combined Province × hour weather

- rows: `5604`
- provinces: `52`
- temporal range:
  `2026-07-28 00:00` → `2026-08-17 12:00`

Temperature source coverage in the validated result:

- AEMET rows: `612`
- Open-Meteo rows: `4992`

### Open-Meteo Province × 15 minutes

- rows: `19968`
- provinces: `52`

### Spain × 15 minutes

- rows: `384`
- temporal range:
  `2026-07-28 00:00` → `2026-07-31 23:45`

### Peninsula × 15 minutes

- source provinces: `52`
- excluded provinces: `5`
- eligible Peninsula provinces: `47`
- result rows: `384`
- duplicated grains: `0`
- temporal range:
  `2026-07-28 00:00` → `2026-07-31 23:45`

The real weather integration validation finished successfully.

---

# 7. Energy Metric Validation

## 7.1 Hourly ESIOS Energy Metrics

The approved hourly indicators are:

| Indicator | Gold metric |
|---:|---|
| 1159 | `wind_generation_mwh` |
| 1161 | `solar_photovoltaic_generation_mwh` |
| 1162 | `solar_thermal_generation_mwh` |
| 10035 | `hydraulic_generation_mwh` |
| 1153 | `nuclear_generation_mwh` |
| 1156 | `combined_cycle_generation_mwh` |
| 1158 | `gas_natural_steam_turbine_generation_mwh` |
| 1164 | `gas_natural_cogeneration_mwh` |
| 10036 | `coal_generation_mwh` |
| 10041 | `other_renewables_generation_mwh` |
| 10043 | `total_generation_mwh` |

Excluded hourly indicators:

- `10195`
- `1193`
- `10267`

Hourly energy values are used directly as MWh.

The official total generation indicator `10043` is preserved and is not
reconstructed from selected technology columns.

## 7.2 Installed Capacity Metrics

The approved installed-capacity indicators are:

| Indicator | Gold metric |
|---:|---|
| 1475 | `hydraulic_installed_capacity_mw` |
| 1485 | `wind_installed_capacity_mw` |
| 1486 | `solar_photovoltaic_installed_capacity_mw` |
| 1487 | `solar_thermal_installed_capacity_mw` |
| 10302 | `renewable_total_installed_capacity_mw` |
| 1477 | `nuclear_installed_capacity_mw` |
| 1478 | `coal_installed_capacity_mw` |
| 1483 | `combined_cycle_installed_capacity_mw` |
| 1488 | `other_renewables_installed_capacity_mw` |

Installed capacity remains in MW.

Indicator `10302` is preserved as the official ESIOS renewable total and
is not reconstructed.

## 7.3 High-Frequency Metrics

The approved high-frequency indicators are:

| Indicator | Scope |
|---:|---|
| 1293 | Peninsula |
| 2038 | Spain |
| 2039 | Spain |
| 2040 | Spain |
| 2041 | Spain |
| 2042 | Spain |
| 2044 | Spain |
| 2045 | Spain |
| 2046 | Spain |
| 2051 | Spain |
| 2065 | Spain |

Indicator `10004` remains excluded.

Spain and Peninsula high-frequency scopes do not overlap.

---

# 8. Province × Hour Weather-Energy Integration

## 8.1 Integration Rule

The approved integration uses:

`FULL OUTER JOIN`

on:

`(province_code, gold_timestamp)`

Both sides are validated for grain uniqueness before the join.

Missing metrics remain `NULL`.

No missing metric is automatically replaced by zero.

Contradictory canonical geography is rejected.

## 8.2 Unit Validation

The Province × hour integration unit suite contains:

`8 tests`

Validated behaviors include:

- matching weather and energy rows;
- weather-only rows;
- energy-only rows;
- FULL OUTER grain preservation;
- duplicate-weather rejection;
- duplicate-energy rejection;
- contradictory-geography rejection;
- final grain uniqueness.

Result:

`8/8 PASSED`

## 8.3 Real Silver Validation

Validated real result:

- weather rows: `5604`
- energy rows: `4418`
- matched rows: `4418`
- weather-only rows: `1186`
- energy-only rows: `0`
- expected final rows: `5604`
- final rows: `5604`
- final distinct grains: `5604`
- final duplicated grains: `0`
- final NULL grains: `0`
- final distinct provinces: `52`
- temporal range:
  `2026-07-28 00:00` → `2026-08-17 12:00`

The real Province × hour integration validation finished successfully.

---

# 9. Spain/Peninsula × 15-Minute Integration

## 9.1 Integration Rule

The approved integration uses:

`FULL OUTER JOIN`

on:

`(geography_key, gold_timestamp)`

Supported geography levels are:

- `COUNTRY`
- `PENINSULA`

Spain and Peninsula are treated as different scopes.

Missing metrics remain `NULL`.

No Spain-to-Peninsula or Peninsula-to-Spain conversion is performed.

## 9.2 Unit Validation

The country 15-minute integration unit suite contains:

`9 tests`

Validated behaviors include:

- matching Spain grain;
- matching Peninsula grain;
- Peninsula demand preservation;
- weather-only rows;
- energy-only rows;
- union of grains;
- duplicate-weather rejection;
- duplicate-energy rejection;
- contradictory/unsupported geography rejection.

Result:

`9/9 PASSED`

## 9.3 Real Silver Validation

The real validation pipeline used:

`Open-Meteo 15 min → Spain weather`

and independently:

`Open-Meteo 15 min → eligible provinces → Peninsula weather`

Energy followed:

`ESIOS 5 min power → configurable temporal alignment → 5 min energy → 15 min interval energy → indicator scope → pivot`

The validation requires:

`source_interval_count = 3`

for each valid 15-minute ESIOS energy bucket.

The validation also confirms:

- Spain demand metric remains `NULL`;
- Peninsula Spain-scoped generation/pumping metrics remain `NULL`;
- Spain and Peninsula are not mixed.

Final validation result:

`ALL GOLD COUNTRY-15MIN REAL SILVER INTEGRATION VALIDATED`

---

# 10. Duplicate and Grain Protection

The final Gold duplicate policy is:

- fact duplicate by natural grain → **ERROR**
- metric duplicate by grain + `indicator_id` → **ERROR**
- duplicate AEMET `station_id` → **ERROR**
- duplicate CNIG `autonomous_community_code` → **ERROR**
- analytical duplicates must not be silently removed

`dropDuplicates()` must not be used to hide fact duplication.

The remaining distinct geographical projections used in
`province_hourly_integration.py` are limited to contradiction-detection
sets and do not remove analytical fact observations.

---

# 11. Static Rule Audit

A final static audit of the Gold transformation code was performed.

Validated result:

## 11.1 Hardcoded ESIOS Gap

No executable hardcoded `+1 hour` Gold transformation was found.

The gap is obtained from configuration.

## 11.2 Sign Modification

No executable use of `ABS()` was found for changing ESIOS metric signs.

## 11.3 NULL-to-Zero Conversion

No executable general:

`COALESCE(metric, 0)`

was found in the Gold transformations.

The occurrences found were comments/docstrings describing the
prohibited rule.

## 11.4 Power Summation

No executable:

`SUM(power_mw)`

was found for 15-minute energy construction.

The occurrences found were comments/docstrings describing the
prohibited rule.

## 11.5 Deduplication

No analytical fact deduplication is used to conceal invalid duplicate
grains.

The static transformation-rule audit therefore passed.

---

# 12. 4.5.3 — Gold Unit Tests

## 12.1 Status

**COMPLETED AND VALIDATED**

## 12.2 Pytest Coverage

The final Gold pytest suite contains:

`111 tests`

Coverage includes:

- metric mappings and exclusions;
- Spain/Peninsula indicator scopes;
- hourly energy semantics;
- installed-capacity semantics;
- MW → MWh conversion;
- 5-minute and 15-minute energy metrics;
- NULL versus zero;
- sign preservation;
- weather contracts;
- circular wind-direction means;
- AEMET fallback behavior;
- unresolved-station exclusion;
- Province × hour integration;
- Spain/Peninsula × 15-minute integration;
- configurable ESIOS temporal gap;
- natural hourly timestamps;
- natural 15-minute buckets;
- CNIG CCAA duplicate rejection;
- canonical national geography keys;
- deterministic Province and Autonomous Community `geography_key` generation;
- deterministic `time_key` generation for all four grains;
- persisted natural-key validation;
- Gold dimension business-key validation;
- Gold fact and dimension builders;
- `gold_created_at` behavior;
- `MERGE` generation and preservation of `gold_created_at`;
- Gold persistence orchestration.

## 12.3 Final Test Execution

Final command:

```powershell
$env:PYTHONPATH="$PWD\spark\jobs"
pytest tests\gold -v
```

Final result:

```text
111 passed in 204.68s (0:03:24)
```

The dedicated persistence test module also validated:

```text
tests/gold/test_write_gold.py → 17 passed
```

Therefore:

- collected tests: `111`
- passed tests: `111`
- failed tests: `0`

The complete Gold pytest suite is validated.

---

# 13. 4.5.4 — Physical Gold Table Creation

## 13.1 Status

**COMPLETED AND VALIDATED**

## 13.2 Initial Catalog State

Before Gold physical creation:

`SHOW NAMESPACES IN lakehouse`

returned only:

`silver`

`lakehouse.gold` did not exist.

Attempting:

`SHOW TABLES IN lakehouse.gold`

returned:

`Namespace does not exist: gold`

This established the initial state before creation.

## 13.3 Namespace Creation

The namespace was created with:

```sql
CREATE NAMESPACE IF NOT EXISTS lakehouse.gold;
```

After creation, the Lakehouse catalog contained:

- `silver`
- `gold`

The Gold namespace initially contained zero tables.

---

# 14. Physical Gold Table Inventory

The implementation created exactly the 6 approved physical Gold tables:

1. `gold_fact_province_hourly`
2. `gold_fact_installed_capacity_monthly`
3. `gold_fact_country_15min`
4. `gold_fact_country_5min`
5. `gold_dim_time`
6. `gold_dim_geography`

The creation script is:

`spark/jobs/gold/create_tables.py`

It uses:

- `CREATE NAMESPACE IF NOT EXISTS`
- `CREATE TABLE IF NOT EXISTS`
- `USING iceberg`

Validated inventory:

```text
EXPECTED_TABLES = 6
EXISTING_TABLES = 6
MISSING_TABLES = []
UNEXPECTED_TABLES = []
```

All six physical tables were registered with:

`PROVIDER = iceberg`

The creation Spark application terminated successfully with:

`exitCode 0`

---

# 15. Validated Physical Schemas and Partitioning

`SHOW CREATE TABLE` was executed for all six Gold tables.

The physical schemas matched the approved schemas defined in
`01_gold_design.md`.

## 15.1 `gold_fact_province_hourly`

Validated partition specification:

```sql
PARTITIONED BY (days(gold_timestamp))
```

## 15.2 `gold_fact_installed_capacity_monthly`

Validated partition specification:

```sql
PARTITIONED BY (year_month)
```

## 15.3 `gold_fact_country_15min`

Validated partition specification:

```sql
PARTITIONED BY (days(gold_timestamp))
```

## 15.4 `gold_fact_country_5min`

Validated partition specification:

```sql
PARTITIONED BY (days(gold_timestamp))
```

## 15.5 `gold_dim_time`

Validated with:

`no Iceberg partitioning`

## 15.6 `gold_dim_geography`

Validated with:

`no Iceberg partitioning`

---

# 16. Iceberg Physical Properties

The physical tables were validated as Apache Iceberg tables.

Observed table properties include:

- `format = iceberg/parquet`
- `format-version = 2`
- `write.parquet.compression-codec = zstd`

Validated warehouse locations follow:

`s3://energy-lakehouse/warehouse/gold/<table_name>`

At the end of 4.5.4:

`current-snapshot-id = none`

This was the expected historical state immediately after physical table
creation and before 4.5.5. Real Gold persistence and the resulting
snapshots are documented later in this document.

---

# 17. CREATE TABLE Idempotency Validation

## 17.1 Initial Iceberg Metadata

Immediately after physical table creation, each table contained one
initial metadata version.

Validated metadata files:

### `gold_fact_province_hourly`

`00000-af118b19-7516-4e4e-be05-1277b628c21e.metadata.json`

### `gold_fact_installed_capacity_monthly`

`00000-bc63f138-e5c2-4111-b9d4-3539ca0ddf0a.metadata.json`

### `gold_fact_country_15min`

`00000-782615f2-6015-42fa-8894-18bb3f67a55f.metadata.json`

### `gold_fact_country_5min`

`00000-880487f9-ecbf-40f4-ab77-1cc8d8da4501.metadata.json`

### `gold_dim_time`

`00000-d4825fe2-f77e-42c6-a682-1759880ac767.metadata.json`

### `gold_dim_geography`

`00000-e6c073e2-676b-41ae-b5ba-de3c244144dc.metadata.json`

## 17.2 Second Creation Execution

`spark/jobs/gold/create_tables.py` was executed a second time.

The six existing tables were loaded from the Iceberg catalog.

The physical inventory remained:

```text
EXPECTED_TABLES = 6
EXISTING_TABLES = 6
MISSING_TABLES = []
UNEXPECTED_TABLES = []
```

All six remained registered as Iceberg.

## 17.3 Metadata Comparison

After the second execution:

- every table retained the same original `00000-...metadata.json`;
- no new `00001-...metadata.json` appeared;
- no table was dropped;
- no table was recreated;
- no replacement operation occurred.

This validates the required creation behavior:

`CREATE TABLE IF NOT EXISTS`

and demonstrates that existing Gold tables are not unnecessarily
recreated.

---

---

# 18. 4.5.2–4.5.9 Consolidated Validation Status

## 18.1 4.5.2 — Silver → Gold Transformations

**COMPLETED AND VALIDATED**

Validated:

- Silver reading;
- temporal aggregation;
- geographical aggregation;
- Province normalization;
- Autonomous Community normalization;
- metric selection;
- weather preparation;
- Province × hour weather-energy integration;
- Spain/Peninsula × 15-minute integration;
- duplicate protection;
- approved transformation and semantic rules.

## 18.2 4.5.3 — Automated Tests

**COMPLETED AND VALIDATED**

Validated result:

`111 passed`

`0 failed`

## 18.3 4.5.4 — Physical Gold Tables

**COMPLETED AND VALIDATED**

Validated:

- namespace `lakehouse.gold`;
- exactly 6 physical tables;
- Apache Iceberg provider;
- approved schemas;
- approved partitioning;
- `CREATE TABLE IF NOT EXISTS`;
- no unnecessary table recreation;
- original initial Iceberg metadata preserved across repeated creation.

## 18.4 4.5.5 — Real Gold Persistence

**COMPLETED AND VALIDATED**

Validated:

- real Silver → Gold execution;
- persistence of all six Gold datasets;
- Iceberg snapshots;
- Parquet data files;
- Iceberg metadata JSON;
- manifests and snapshot manifest lists;
- physical MinIO objects under `warehouse/gold/`;
- logical idempotency across a repeated execution.

## 18.5 4.5.6 — Persisted Gold Quality

**COMPLETED AND VALIDATED**

Validated:

- row counts;
- structural NULL controls;
- duplicate natural keys;
- timestamp alignment;
- geography integrity;
- metric NULL/anomaly interpretation;
- grain integrity;
- temporal coverage;
- reconciliation with Silver.

## 18.6 4.5.7 — Analytical Integration

**COMPLETED AND VALIDATED**

Validated real analytical pairings include weather versus generation,
weather versus demand, territorial analysis, and installed capacity
versus observed generation at compatible grains.

## 18.7 4.5.8 — Trino Validation

**COMPLETED AND VALIDATED**

Validated:

- catalog and schema discovery;
- Gold table discovery;
- physical schema interpretation;
- row counts;
- real analytical SQL queries;
- readiness of Gold for downstream SQL consumption.

## 18.8 4.5.9 — End-to-End Validation

**COMPLETED AND VALIDATED**

Validated execution path:

`Silver Iceberg → Gold transformation → Gold Iceberg → MinIO → Trino`

A real ESIOS observation was traced through the complete path and the
same analytical result was reproduced through Spark and Trino.

---

# 19. 4.5.5 — Real Gold Persistence

## 19.1 Status

**COMPLETED AND VALIDATED**

## 19.2 Persistence Implementation

The persistence entry point is:

`spark/jobs/gold/write_gold.py`

The implementation builds the four fact datasets and the two dimensions,
validates their approved schemas and natural keys, and persists them to
the six previously created Iceberg tables.

All six Gold tables are persisted through `MERGE`.

Validated MERGE keys are:

| Table | MERGE key |
|---|---|
| `gold_fact_province_hourly` | `(province_code, gold_timestamp)` |
| `gold_fact_installed_capacity_monthly` | `(autonomous_community_code, year_month)` |
| `gold_fact_country_15min` | `(geography_key, gold_timestamp)` |
| `gold_fact_country_5min` | `(geography_key, gold_timestamp)` |
| `gold_dim_time` | `time_key` |
| `gold_dim_geography` | `geography_key` |

`gold_created_at` is populated when a row is first inserted and is not
updated during subsequent matched MERGE operations.

## 19.3 Deterministic Keys

The persisted implementation uses deterministic keys.

Province `geography_key`:

`SHA-256(PROVINCE + province_code)`

Autonomous Community `geography_key`:

`SHA-256(AUTONOMOUS_COMMUNITY + autonomous_community_code)`

National keys remain explicit canonical values:

- Spain → `COUNTRY:ES`
- Peninsula → `PENINSULA:ES-PEN`

`time_key` is generated using SHA-256 from:

`time_grain + canonical temporal value`

where the canonical value is `gold_timestamp` for submonthly grains and
`year_month` for the monthly grain. No artificial monthly timestamp is
created in `gold_dim_time`.

## 19.4 First Real Persistence Execution

The first real execution produced:

| Gold table | Persisted rows |
|---|---:|
| `gold_fact_province_hourly` | 5,604 |
| `gold_fact_installed_capacity_monthly` | 19 |
| `gold_fact_country_15min` | 776 |
| `gold_fact_country_5min` | 2,304 |
| `gold_dim_time` | 1,649 |
| `gold_dim_geography` | 73 |

The persistence process completed successfully for all six tables.

## 19.5 Repeated Execution and Logical Idempotency

The same persistence process was executed a second time using the same
validated Silver state.

The second execution produced exactly the same logical row counts:

- `5,604`
- `19`
- `776`
- `2,304`
- `1,649`
- `73`

No natural-key duplicates were introduced.

The original `gold_created_at` values were also preserved. Validated
first-insert timestamps remained unchanged after the second execution:

| Gold table | Preserved `gold_created_at` |
|---|---|
| `gold_fact_province_hourly` | `2026-08-25 19:58:02.327642` |
| `gold_fact_installed_capacity_monthly` | `2026-08-25 19:58:02.680437` |
| `gold_fact_country_15min` | `2026-08-25 19:58:06.960601` |
| `gold_fact_country_5min` | `2026-08-25 19:58:07.351707` |
| `gold_dim_time` | `2026-08-25 19:58:11.568541` |
| `gold_dim_geography` | `2026-08-25 19:58:15.024405` |

This validates logical idempotency: the repeated execution did not
append duplicate logical rows and preserved creation timestamps.

A full cryptographic comparison of every table value between executions
was not separately calculated. Physical Iceberg snapshots are allowed to
change while the logical table state remains idempotent.

## 19.6 Iceberg Metadata Validation

Iceberg metadata tables were queried for all six Gold tables.

Validated counts were:

| Table | Snapshots | Current data files | Manifests | Partition rows |
|---|---:|---:|---:|---:|
| `gold_fact_province_hourly` | 2 | 5 | 2 | 5 |
| `gold_fact_installed_capacity_monthly` | 2 | 1 | 2 | 1 |
| `gold_fact_country_15min` | 2 | 5 | 2 | 5 |
| `gold_fact_country_5min` | 2 | 5 | 2 | 5 |
| `gold_dim_time` | 2 | 1 | 2 | 1 |
| `gold_dim_geography` | 2 | 1 | 2 | 1 |

For unpartitioned dimensions, the Iceberg `partitions` metadata table
returns one logical partition row; this does not mean that a physical
partition specification was added.

For `gold_fact_province_hourly`, the two snapshots were observed as:

- initial persistence → `append`;
- repeated MERGE → `overwrite`.

The current files for this table were five Parquet data files covering
five day partitions.

## 19.7 Direct MinIO Physical Validation

The MinIO container was inspected directly.

The Gold warehouse physically contains all six tables under:

`/data/energy-lakehouse/warehouse/gold/`

Each table contains both:

- `data/`
- `metadata/`

Direct inspection of `gold_fact_province_hourly` confirmed:

- Iceberg metadata JSON objects;
- manifest `.avro` objects;
- snapshot manifest-list `.avro` objects;
- day-partitioned data paths;
- Parquet data objects.

Validated day partitions were:

- `2026-07-28`
- `2026-07-29`
- `2026-07-30`
- `2026-07-31`
- `2026-08-17`

The repeated MERGE generated new physical Parquet objects while the
logical row state remained unchanged.

---

# 20. 4.5.6 — Persisted Gold Data Quality

## 20.1 Status

**COMPLETED AND VALIDATED**

## 20.2 Row Counts, NULL Natural Keys, and Duplicates

Validated persisted results:

| Table | Rows | NULL natural keys | Duplicate natural keys |
|---|---:|---:|---:|
| `gold_fact_province_hourly` | 5,604 | 0 | 0 |
| `gold_fact_installed_capacity_monthly` | 19 | 0 | 0 |
| `gold_fact_country_15min` | 776 | 0 | 0 |
| `gold_fact_country_5min` | 2,304 | 0 | 0 |
| `gold_dim_time` | 1,649 | 0 | 0 |
| `gold_dim_geography` | 73 | 0 | 0 |

## 20.3 Timestamp, Grain, and Coverage Validation

Validated temporal results:

| Table / grain | Invalid grain rows | Validated coverage |
|---|---:|---|
| Province × hour | 0 | `2026-07-28 00:00:00` → `2026-08-17 12:00:00` |
| CCAA × month | 0 | `2026-07` |
| Country/Peninsula × 15 min | 0 | `2026-07-28 00:00:00` → `2026-08-01 00:45:00` |
| Country/Peninsula × 5 min | 0 | `2026-07-28 01:00:00` → `2026-08-01 00:55:00` |

`gold_dim_time` contains:

| Grain | Members |
|---|---:|
| `HOUR` | 108 |
| `FIFTEEN_MINUTES` | 388 |
| `FIVE_MINUTES` | 1,152 |
| `MONTH` | 1 |

All validated dimension grain checks returned zero invalid rows.

## 20.4 Geography Integrity

`gold_dim_geography` contains exactly:

| Level | Members |
|---|---:|
| `PROVINCE` | 52 |
| `AUTONOMOUS_COMMUNITY` | 19 |
| `COUNTRY` | 1 |
| `PENINSULA` | 1 |
| **Total** | **73** |

Validated geography controls returned:

- NULL geographical codes: `0`;
- NULL geographical names: `0`;
- invalid geography levels: `0`;
- fact rows unmatched to `gold_dim_geography`: `0`;
- fact/dimension geography mismatches: `0`.

CNIG reconciliation produced:

- Province: `52 Silver = 52 Gold`, `0` missing, `0` extra, `0` mismatches;
- Autonomous Community: `19 Silver = 19 Gold`, `0` missing, `0` extra, `0` mismatches.

The two validated national members are:

- `COUNTRY:ES / COUNTRY / ES / España`;
- `PENINSULA:ES-PEN / PENINSULA / ES-PEN / Península`.

## 20.5 Weather Metric Validation

Province-hour physical weather checks returned zero invalid values for:

- humidity outside `0..100`;
- negative precipitation;
- negative wind speed at 80 m;
- negative wind speed at 120 m;
- wind direction outside `0..360`;
- negative solar radiation;
- negative DNI.

The same eight physical checks returned zero invalid values in the
15-minute national weather product.

Persisted Province × hour weather coverage was:

- AEMET temperature/humidity/precipitation rows: `612`;
- Open-Meteo fallback rows: `4,992`;
- total rows for these three metrics: `5,604`;
- wind 80/120 m, wind directions, radiation, and DNI values: `4,992`;
- metric values with missing source traceability: `0`;
- invalid source labels: `0`.

For `gold_fact_country_15min`:

- Spain fact rows: `388`;
- Peninsula fact rows: `388`;
- weather values per scope: `384`;
- weather coverage per scope:
  `2026-07-28 00:00:00` → `2026-07-31 23:45:00`.

The four additional integrated timestamps per scope are explained by
energy coverage beyond the weather range and are not treated as data
quality failures.

## 20.6 ESIOS Sign and Anomaly Traceability

Negative values found in persisted Gold were explicitly reconciled to
Silver before being classified.

Examples:

| Indicator / metric | Silver negatives | Gold negatives | Validated minimum |
|---|---:|---:|---:|
| `10035` hydraulic hourly energy | 121 | 121 | `-547.566` |
| `10043` total hourly generation | 102 | 102 | `-325.659` |
| `2042` hydraulic 5-min power | 339 | 339 | `-2840.0 MW` |
| `2065` pumping consumption 5-min power | 1,064 | 1,064 | `-3722.0 MW` |

The values already exist in Silver and are preserved by Gold. They are
therefore not transformation-generated anomalies.

## 20.7 Silver-to-Gold Metric Reconciliation

### Hourly ESIOS energy

All 11 selected indicators were reconciled using:

- number of non-null values;
- sum;
- minimum;
- maximum.

Result:

`11/11 = OK`

### Installed capacity

All 9 selected capacity indicators were reconciled by indicator, CCAA,
month, and value.

Result:

- missing in Gold: `0`;
- extra in Gold: `0`;
- value mismatches: `0`.

### ESIOS 5-minute power

All 11 selected high-frequency indicators contained:

`1,152 Silver values = 1,152 Gold values`

for each indicator.

Result:

- missing in Gold: `0`;
- extra in Gold: `0`;
- value mismatches: `0`.

### MW → MWh/5min conversion

For all 11 selected indicators:

`energy_mwh_5min = power_mw × 5 / 60`

Result:

- NULL power/energy pair mismatches: `0`;
- conversion mismatches: `0`.

### 5 min → 15 min

For all 11 selected indicators:

- expected 15-minute values per indicator: `384`;
- persisted Gold values per indicator: `384`;
- intervals not built from exactly 3 source intervals: `0`;
- missing in Gold: `0`;
- extra in Gold: `0`;
- value mismatches: `0`.

This validates that 15-minute energy is constructed from three real
5-minute interval-energy values rather than from a sum of power.

---

# 21. 4.5.7 — Analytical Integration Validation

## 21.1 Status

**COMPLETED AND VALIDATED**

## 21.2 Province × Hour Analytical Coverage

Validated real pair counts:

| Analytical pairing | Available pairs |
|---|---:|
| wind speed 80 m ↔ wind generation | 3,752 |
| wind speed 120 m ↔ wind generation | 3,752 |
| wind direction ↔ wind generation | 3,752 |
| solar radiation ↔ photovoltaic generation | 3,734 |
| DNI ↔ photovoltaic generation | 3,734 |
| precipitation ↔ hydraulic generation | 4,273 |

Validated territorial coverage with weather and at least one selected
renewable-generation metric:

- provinces: `47`;
- autonomous communities: `15`.

## 21.3 Spain and Peninsula Analytical Separation

Validated `gold_fact_country_15min` coverage:

| Scope | Fact rows | Weather rows | Temperature ↔ demand pairs | Weather ↔ Spain-generation pairs |
|---|---:|---:|---:|---:|
| `COUNTRY:ES` | 388 | 384 | 0 | 380 |
| `PENINSULA:ES-PEN` | 388 | 384 | 380 | 0 |

This is the intended result. Demand indicator `1293` is peninsular,
while the selected high-frequency generation indicators are Spain scope.
Gold does not mix the two geographies.

## 21.4 Installed Capacity Analytical Coverage

For `2026-07`, the monthly installed-capacity fact contains all `19`
autonomous communities.

Available metric counts include:

- wind capacity: `17`;
- photovoltaic capacity: `19`;
- hydraulic capacity: `16`;
- official renewable total: `19`.

## 21.5 Generation versus Installed Capacity

For the approved capacity-versus-generation analysis, hourly provincial
generation was first aggregated to:

`Autonomous Community × month`

Only then was it joined to monthly installed capacity.

Validated result:

- joined CCAA-months: `19`;
- wind generation/capacity pairs: `14`;
- photovoltaic generation/capacity pairs: `15`;
- hydraulic generation/capacity pairs: `15`.

This confirms that the analysis can be performed without directly
joining incompatible hourly and monthly grains.

---

# 22. 4.5.8 — Trino Validation

## 22.1 Status

**COMPLETED AND VALIDATED**

## 22.2 Catalog and Schema Discovery

The validated Trino container is:

`energy-trino`

`SHOW CATALOGS` returned:

- `iceberg`;
- `system`.

`SHOW SCHEMAS FROM iceberg` returned:

- `gold`;
- `information_schema`;
- `silver`;
- `system`.

## 22.3 Gold Table Discovery

`SHOW TABLES FROM iceberg.gold` returned exactly the six approved Gold
tables.

## 22.4 DESCRIBE Validation

`DESCRIBE` succeeded for all six Gold tables.

Trino correctly interpreted the Iceberg physical types, including:

- `varchar`;
- `double`;
- `bigint`;
- `integer`;
- `date`;
- `timestamp(6) with time zone`.

## 22.5 Row-Count Reconciliation

Trino returned exactly the same persisted counts previously obtained
through Spark/Iceberg:

| Gold table | Trino rows |
|---|---:|
| `gold_fact_province_hourly` | 5,604 |
| `gold_fact_installed_capacity_monthly` | 19 |
| `gold_fact_country_15min` | 776 |
| `gold_fact_country_5min` | 2,304 |
| `gold_dim_time` | 1,649 |
| `gold_dim_geography` | 73 |

## 22.6 Real Analytical Query — Wind versus Wind Generation

A real Province × hour Trino query used:

- `COUNT`;
- `AVG`;
- `corr()`;
- `GROUP BY`;
- `ORDER BY`;
- `LIMIT`.

The query returned real provincial analytical results. Examples include:

- Cádiz → correlation `0.8608`;
- A Coruña → `0.8117`;
- Cuenca → `0.6934`;
- León → `0.6703`;
- Burgos → `0.6564`.

These values demonstrate query capability and observed association for
the loaded period; they are not interpreted as causal effects.

## 22.7 Real Analytical Query — Temperature versus Demand

The query was correctly restricted to:

`PENINSULA:ES-PEN`

Validated result:

- observations: `380`;
- minimum temperature: `18.322`;
- maximum temperature: `34.012`;
- average temperature: `26.412`;
- minimum demand: `6431.250 MWh/15min`;
- maximum demand: `10314.833 MWh/15min`;
- average demand: `8741.182 MWh/15min`;
- observed correlation: `0.9474`.

## 22.8 Real Analytical Query — Generation versus Capacity

Trino successfully:

1. aggregated Province × hour generation to CCAA × month;
2. joined the result to `gold_fact_installed_capacity_monthly`;
3. returned real wind, photovoltaic, and hydraulic generation/capacity
   combinations.

The query returned 15 autonomous communities with at least one requested
generation/capacity combination available for `2026-07`.

## 22.9 Readiness for Visualization Consumption

Gold is validated as consumable through Trino SQL.

The validated path supports:

- filtering;
- grouping;
- temporal aggregation;
- geographical aggregation;
- joins;
- statistical functions;
- direct analytical queries over Gold Iceberg tables.

This demonstrates that Gold is prepared for subsequent consumption by a
visualization layer through Trino.

The actual visualization-tool connection is outside 4.5.8 and is not
claimed as validated in this document.

---

# 23. 4.5.9 — End-to-End Validation

## 23.1 Status

**COMPLETED AND VALIDATED**

## 23.2 Validated End-to-End Path

The complete validated path is:

```text
Silver Iceberg
    ↓
Gold transformation
    ↓
Gold Iceberg
    ↓
MinIO
    ↓
Trino
```

## 23.3 Real Observation Traceability

A real observation was selected from persisted Gold and traced back to
Silver.

Validated observation:

- Province: `02 — Albacete`;
- indicator: `1159 — Generación medida Eólica terrestre`;
- Silver timestamp: `2026-07-28 00:00:00`;
- Gold timestamp: `2026-07-28 01:00:00`;
- Silver value: `430.464 MWh`;
- Gold value: `430.464 MWh`;
- value validation: `OK`.

The timestamp reflects the approved configurable `+1 hour` Gold
alignment. The energy value itself is preserved.

## 23.4 MinIO Traceability

The traced Gold observation belongs to:

`gold_timestamp_day=2026-07-28`

The corresponding physical Gold partition was directly validated in
MinIO during 4.5.5 and contains Parquet data objects plus the associated
Iceberg metadata structures.

## 23.5 Trino Traceability

The same persisted observation was queried through Trino and returned:

- Province: `02 — Albacete`;
- Gold timestamp: `2026-07-28 01:00:00 UTC`;
- wind generation: `430.464 MWh`.

## 23.6 Analytical Reproducibility

The same Albacete wind-analysis query was executed independently through
Spark and Trino.

Both engines returned exactly:

| Measure | Spark | Trino |
|---|---:|---:|
| observations | 94 | 94 |
| average wind speed at 80 m | 13.872 | 13.872 |
| average wind generation | 283.021 MWh | 283.021 MWh |
| correlation | 0.1405 | 0.1405 |

This validates analytical reproducibility across both query engines for
the tested Gold result.

---

# 24. Implementation Decisions Consolidated During 4.5

The following decisions were fixed during implementation and validated
with real execution evidence.

## 24.1 Deterministic Keys

- Province and Autonomous Community keys use deterministic SHA-256.
- Spain and Peninsula retain explicit canonical keys.
- Temporal members use deterministic SHA-256 based on grain and
  canonical temporal value.
- Monthly temporal members are represented by `year_month` without an
  artificial timestamp.

## 24.2 Temporal Alignment

The ESIOS gap remains a Gold configuration concern.

Current validated configuration:

`esios_time_gap_hours = 1`

It is not hardcoded and is not automatically applied to monthly
installed capacity.

## 24.3 Geography

- CNIG remains the canonical master for Province and Autonomous
  Community.
- Province is the preferred analytical geography when the source can
  support it.
- Spain and Peninsula remain distinct.
- Peninsula weather is independently aggregated from the 47 validated
  eligible province entities.
- No unsupported lower-level geography is fabricated.

## 24.4 Weather Integration

AEMET fallback to Open-Meteo applies independently to temperature,
humidity, and precipitation only.

Unresolved AEMET stations are excluded from Province × hour Gold rather
than assigned an invented province.

## 24.5 Integration Strategy

Prepared datasets are aggregated to the target analytical grain before
integration.

Province × hour and country 15-minute products use `FULL OUTER JOIN` so
that legitimate source-only coverage is retained as rows with NULL
metrics on the missing side.

## 24.6 Energy Semantics

- Hourly ESIOS energy values are preserved directly as MWh.
- Installed capacity remains MW.
- 5-minute interval energy is derived as `MW × 5/60`.
- 15-minute energy is the sum of three real 5-minute energy intervals.
- Original ESIOS signs are preserved.
- Official totals are preserved instead of reconstructed where approved.

## 24.7 Persistence Strategy

All six Gold tables are persisted through `MERGE`.

`gold_created_at` is preserved on matched updates.

Logical idempotency is distinguished from physical Iceberg file/snapshot
rewrites.

## 24.8 Temporal Dimension Relationship

The Gold facts do not physically materialize a `time_key` foreign-key
column.

`gold_dim_time` is a conformed temporal dimension whose members are
validated against:

- `gold_timestamp` for submonthly facts;
- `year_month` for the monthly fact.

The geographical relationship is physically materialized through
`geography_key`.

---

# 25. Known Limitations and Current Data-Coverage Constraints

The following limitations are known and evidenced at the current Gold
checkpoint. They are not silently treated as implementation failures.

## 25.1 Loaded Temporal Coverage

Gold currently represents the temporal ranges available in the loaded
validated Silver state. It is not a claim of complete historical
coverage.

Source gaps are preserved; missing timestamps are not fabricated.

## 25.2 AEMET Station Resolution

The current AEMET station mapping contains:

- `584` unresolved observation rows;
- `49` unresolved station IDs.

These rows remain upstream and are excluded from Province × hour Gold.

## 25.3 Different Source Coverages

Weather, energy, demand, and capacity sources do not have identical
territorial or temporal coverage.

Examples include:

- 52 province-level weather entities versus 47 provinces with validated
  weather-energy analytical overlap;
- 388 country-15min fact rows per scope but 384 weather values per
  scope;
- installed-capacity technologies not being present in every autonomous
  community.

These differences result in legitimate metric NULLs.

## 25.4 Spain versus Peninsula

Demand indicator `1293` is Peninsula scope, while the selected
high-frequency generation indicators are Spain scope.

Gold intentionally does not combine or relabel these scopes.

## 25.5 Negative ESIOS Values

Negative hydraulic, pumping, and total-generation values observed in
Gold are present in Silver and are preserved by design.

They are not automatically corrected, inverted, or converted to absolute
values.

## 25.6 Physical Iceberg Rewrite on Repeated MERGE

A repeated logically idempotent MERGE may create a new Iceberg snapshot
and new physical Parquet objects even when the logical Gold result is
unchanged.

This is accepted at the current implementation checkpoint. Physical
Iceberg optimization is deferred to phase 4.7, where optimizations will
be evaluated against the real persisted tables.

## 25.7 Visualization Layer

Trino readiness for downstream visualization is validated.

The actual Superset-to-Trino connection and dashboards are not part of
4.5 and are therefore not claimed as validated here.

## 25.8 Analytical Interpretation

Correlations calculated during validation demonstrate analytical query
capability and observed association over the currently loaded period.
They must not be interpreted as proof of causality.

---

# 26. 4.5.10 — Documentation Status

## 26.1 Status

**COMPLETED**

The Gold documentation now records the validated implementation through
4.5.9.

The two Gold documentation sources are:

- `docs/Gold/01_gold_design.md`
- `docs/Gold/02_gold_implementation_and_validation.md`

Together they document:

- architecture;
- physical Gold tables;
- schemas and physical types;
- temporal and geographical grains;
- aggregation rules;
- geographical integration;
- temporal alignment;
- selected indicators;
- weather variables and fallback policy;
- automated tests;
- real persisted results;
- Iceberg/MinIO persistence evidence;
- Trino consumption;
- end-to-end traceability;
- known limitations;
- implementation decisions.

---

# 27. Current Gold Checkpoint

The current validated Gold implementation checkpoint is:

```text
4.5.1   Gold structure preparation              COMPLETED
4.5.2   Silver → Gold transformations           COMPLETED AND VALIDATED
4.5.3   Gold automated tests                    COMPLETED AND VALIDATED
4.5.4   Physical Gold table creation            COMPLETED AND VALIDATED
4.5.5   Real Gold persistence                   COMPLETED AND VALIDATED
4.5.6   Persisted Gold data quality             COMPLETED AND VALIDATED
4.5.7   Analytical integration                  COMPLETED AND VALIDATED
4.5.8   Trino validation                        COMPLETED AND VALIDATED
4.5.9   End-to-end validation                   COMPLETED AND VALIDATED
4.5.10  Documentation                           COMPLETED
```

The next Gold implementation step is:

`4.5.11 — final Gold audit`

No later Gold step is marked as completed in this document without
execution evidence.