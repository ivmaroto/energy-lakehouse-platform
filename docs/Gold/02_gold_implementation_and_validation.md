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

At the current checkpoint:

- **4.5.1 — Gold structure preparation:** completed.
- **4.5.2 — Silver → Gold transformations:** completed and validated.
- **4.5.3 — Gold unit tests:** completed and validated.
- **4.5.4 — Physical Gold table creation:** completed and validated.
- **4.5.5 — Real Gold persistence:** pending.

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

`88 tests`

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
- canonical national geography keys.

## 12.3 Final Test Execution

Final command:

```powershell
$env:PYTHONPATH="$PWD\spark\jobs"
pytest tests\gold -v
```

Final result:

```text
88 passed in 162.17s (0:02:42)
```

Therefore:

- collected tests: `88`
- passed tests: `88`
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

This is expected because the tables have been physically created but
real Gold data has not yet been persisted.

Real data persistence belongs to 4.5.5.

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

# 18. 4.5.2–4.5.4 Consolidated Validation Status

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
- approved 4.4 transformation rules.

## 18.2 4.5.3 — Unit Tests

**COMPLETED AND VALIDATED**

Validated result:

`88 passed`

`0 failed`

## 18.3 4.5.4 — Physical Gold Tables

**COMPLETED AND VALIDATED**

Validated:

- namespace `lakehouse.gold`;
- exactly 6 physical tables;
- Apache Iceberg;
- approved schemas;
- approved partitioning;
- `CREATE TABLE IF NOT EXISTS`;
- no unnecessary table recreation;
- original Iceberg metadata preserved across repeated creation.

---

# 19. Current Gold Checkpoint

The current validated Gold implementation checkpoint is:

```text
4.5.1  Gold structure preparation              COMPLETED
4.5.2  Silver → Gold transformations           COMPLETED AND VALIDATED
4.5.3  Gold unit tests                         COMPLETED AND VALIDATED
4.5.4  Physical Gold table creation            COMPLETED AND VALIDATED
4.5.5  Real Gold persistence                   PENDING
```

The six physical Gold tables currently exist but remain empty.

No statement in this document should be interpreted as evidence that
4.5.5 persistence has already been completed.

---

# 20. Next Step — 4.5.5 Real Gold Persistence

**Status: PENDING**

The next implementation step must persist the validated Gold datasets
into the six physical Iceberg tables and then validate the persisted
state.

The pending work includes, at minimum:

- real Silver → Gold execution;
- writes to the approved Iceberg tables;
- physical objects in MinIO;
- Parquet data files;
- Iceberg metadata and snapshots;
- row counts;
- physical partition validation;
- persisted natural-key uniqueness;
- persisted structural NULL controls;
- load idempotency;
- evidence that rerunning the same logical input does not duplicate
  Gold rows.

These items remain pending until execution evidence is produced.
