# Silver Layer — Implementation and Validation

## 1. Purpose

This document describes the implementation and technical validation of the Silver layer of the Energy Lakehouse Platform.

The Silver layer transforms the raw datasets stored in the Bronze layer into normalized, typed, deduplicated and queryable datasets persisted as Apache Iceberg tables.

The implemented processing flow is:

Bronze (MinIO / S3-compatible storage)
→ PySpark transformations
→ Apache Iceberg Silver tables
→ MinIO persistent storage
→ Trino SQL access

The implementation preserves the original data granularity whenever possible. Aggregation and analytical selection are delegated to the Gold layer.

---

## 2. Implemented Silver Tables

A total of 12 Apache Iceberg tables have been implemented.

### AEMET

| Table | Validated rows |
|---|---:|
| `silver_aemet_stations` | 921 |
| `silver_aemet_daily_climatology` | 2,420 |
| `silver_aemet_current_observations` | 9,688 |

### Open-Meteo

| Table | Validated rows |
|---|---:|
| `silver_open_meteo_hourly` | 88,416 |
| `silver_open_meteo_historical_forecast` | 88,416 |
| `silver_open_meteo_15min` | 353,664 |

### CNIG

| Table | Validated rows |
|---|---:|
| `silver_cnig_provinces` | 52 |
| `silver_cnig_autonomous_communities` | 19 |
| `silver_cnig_municipalities` | 8,132 |

### ESIOS

| Table | Validated rows |
|---|---:|
| `silver_esios_energy_hourly` | 30,107 |
| `silver_esios_power_5min` | 13,824 |
| `silver_esios_installed_capacity_monthly` | 123 |

---

## 3. Apache Iceberg Persistence

The 12 Silver datasets are persisted as managed Apache Iceberg tables.

The validated warehouse location is:

` s3://energy-lakehouse/warehouse/silver/ `

Physical persistence was verified directly in MinIO.

For the inspected Iceberg table, the physical structure contained:

- `data/` for the persisted data files.
- `metadata/` for Apache Iceberg metadata.
- Iceberg metadata JSON files.
- Avro manifest and snapshot files.
- Physical data partitions.

For example, `silver_open_meteo_hourly` is physically partitioned by day using:

`observation_timestamp_day`

The table was also validated as:

- Provider: `iceberg`
- Format: Iceberg/Parquet
- Iceberg format version: 2
- Parquet compression: ZSTD

---

## 4. Partitioning

The physical partitioning validated for the Silver tables follows the designed temporal granularities.

Examples validated during implementation include:

- `silver_aemet_current_observations`: day of `observation_timestamp`
- `silver_open_meteo_hourly`: day of `observation_timestamp`
- `silver_open_meteo_historical_forecast`: day of `observation_timestamp`
- `silver_open_meteo_15min`: day of `observation_timestamp`
- `silver_esios_energy_hourly`: day of `observation_timestamp`
- `silver_esios_power_5min`: day of `observation_timestamp`
- `silver_esios_installed_capacity_monthly`: month of `observation_timestamp`
- `silver_aemet_daily_climatology`: month of `observation_date`

Reference/master datasets that do not require temporal partitioning are stored without temporal partitions.

---

## 5. Data Quality Validation

Data quality controls were executed against the Silver tables after their physical persistence in Apache Iceberg.

The validation covered:

- Expected row counts.
- Null natural keys.
- Duplicate natural keys.
- Null observation timestamps or dates.
- Null ingestion timestamps.
- Invalid geographical coordinates.
- Temporal granularity.

All 12 persisted Silver tables matched their expected row counts.

No duplicate natural keys were detected in any of the validated tables.

No null natural keys were detected.

No null mandatory observation timestamps or dates were detected.

No null ingestion timestamps were detected.

No invalid coordinates were detected in the datasets where coordinate validation was applicable.

### Open-Meteo temporal validation

The persisted Open-Meteo datasets preserved their expected temporal granularities:

| Dataset | Expected granularity | Temporal differences | Matching | Differences |
|---|---:|---:|---:|---:|
| `silver_open_meteo_hourly` | 60 min | 87,495 | 87,495 | 0 |
| `silver_open_meteo_historical_forecast` | 60 min | 87,495 | 87,495 | 0 |
| `silver_open_meteo_15min` | 15 min | 352,743 | 352,743 | 0 |

### ESIOS temporal validation

The persisted ESIOS 5-minute dataset preserved its expected temporal granularity:

| Dataset | Expected granularity | Temporal differences | Matching | Differences |
|---|---:|---:|---:|---:|
| `silver_esios_power_5min` | 5 min | 13,812 | 13,812 | 0 |

For `silver_esios_energy_hourly`, the validation produced:

- Total temporal differences: 29,765
- 60-minute differences: 29,399
- Non-60-minute differences: 366

A separate inspection of these differences confirmed gaps greater than one hour across several source datasets.

These gaps are preserved in Silver rather than synthetically filled. Silver normalizes and preserves the available source observations; it does not generate missing observations.

---

## 6. Idempotent Silver Writes

Silver persistence uses natural-key-based merge operations against the Apache Iceberg tables.

Idempotency was validated by executing the Silver write process again after data had already been persisted.

The second execution maintained the same target row counts for the previously populated tables instead of inserting duplicate records.

The complete write execution finished successfully for all 12 Silver tables.

Examples:

| Table | Source rows | Target rows after merge |
|---|---:|---:|
| `silver_aemet_stations` | 921 | 921 |
| `silver_aemet_daily_climatology` | 2,420 | 2,420 |
| `silver_aemet_current_observations` | 9,688 | 9,688 |
| `silver_open_meteo_hourly` | 88,416 | 88,416 |
| `silver_open_meteo_historical_forecast` | 88,416 | 88,416 |
| `silver_open_meteo_15min` | 353,664 | 353,664 |
| `silver_cnig_provinces` | 52 | 52 |
| `silver_cnig_autonomous_communities` | 19 | 19 |
| `silver_cnig_municipalities` | 8,132 | 8,132 |
| `silver_esios_energy_hourly` | 30,107 | 30,107 |
| `silver_esios_power_5min` | 13,824 | 13,824 |
| `silver_esios_installed_capacity_monthly` | 123 | 123 |

This validation demonstrates that rerunning the Silver persistence process does not produce duplicate rows for the same natural keys.

---

## 7. Trino Catalog Validation

The Silver namespace and its Apache Iceberg tables were validated from Trino.

The `iceberg` catalog exposed the following schemas:

- `information_schema`
- `silver`
- `system`

The `iceberg.silver` schema exposed all 12 implemented Silver tables.

A real SQL query was executed from Trino against:

`iceberg.silver.silver_open_meteo_hourly`

The query:

`SELECT COUNT(*) FROM iceberg.silver.silver_open_meteo_hourly`

returned:

`88416`

This result matches the row count obtained during the PySpark transformation, Iceberg persistence and persisted-data validation.

This confirms that the Silver tables created and populated through Spark are accessible from Trino through the shared Iceberg catalog.

---

## 8. Automated Tests

The Silver implementation includes unit, integration, physical-schema, persisted-data and end-to-end validation tests.

The Silver test directory contains tests covering:

- Common Silver functionality.
- AEMET transformations.
- AEMET integration with real Bronze data.
- Open-Meteo transformations.
- Open-Meteo integration with real Bronze data.
- CNIG transformations.
- CNIG integration with real Bronze data.
- ESIOS transformations.
- ESIOS integration with real Bronze data.
- Iceberg integration.
- Silver integration.
- Physical Iceberg table validation.
- Persisted Silver data validation.
- End-to-end Silver validation.

Additional inspection scripts were used during implementation to inspect real Bronze schemas and investigate source-data characteristics such as ESIOS hourly temporal gaps.

Validated unit-test executions include:

- AEMET: 7 tests passed.
- Open-Meteo: 7 tests passed.
- ESIOS: 7 tests passed.

Integration tests were additionally executed against real Bronze datasets before persistence.

---

## 9. End-to-End Validation

A complete Silver end-to-end validation was executed using real Bronze data.

The validated processing path was:

Bronze data in MinIO
→ PySpark Silver transformations
→ persisted Apache Iceberg Silver tables
→ SQL queries against persisted tables

For each of the 12 Silver datasets, the validation compared:

- Rows produced by the Bronze-to-Silver transformation.
- Rows persisted in the corresponding Iceberg table.
- Expected validated row count.
- Source and target column order.
- SQL row count obtained from the persisted Iceberg table.

All 12 datasets produced:

- `SOURCE_TARGET_COUNT_MATCH = True`
- `EXPECTED_COUNT_MATCH = True`
- `COLUMN_ORDER_MATCH = True`

SQL queries against all 12 persisted Iceberg tables also returned their expected validated row counts.

The execution completed with:

`SILVER END-TO-END VALIDATION COMPLETE`

without validation exceptions.

---

## 10. Validation Result

The implemented Silver layer has been technically validated across the complete processing chain.

The validation confirms:

- 12 Apache Iceberg Silver tables implemented.
- Real Bronze data transformed with PySpark.
- Natural-key deduplication applied.
- Expected schemas and partitioning validated.
- Data-quality controls executed.
- Expected temporal granularities preserved.
- Known source-data gaps detected and preserved without synthetic filling.
- Data physically persisted in MinIO.
- Iceberg metadata and physical data files present.
- Idempotent Silver writes validated.
- Silver tables accessible through Trino.
- End-to-end Bronze-to-Silver processing validated.

Therefore, the implemented Silver data-processing and persistence flow is considered technically validated for the datasets and executions documented above.