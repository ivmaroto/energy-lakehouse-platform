# Silver Layer — Implementation and Validation

## 1. Purpose

This document describes the implementation and technical validation of the Silver layer of the Energy Lakehouse Platform.

The Silver layer transforms the raw datasets stored in the Bronze layer into normalized, typed, deduplicated and queryable datasets persisted as Apache Iceberg tables.

The implemented processing flow is:

```text
Bronze (MinIO / S3-compatible storage)
→ PySpark transformations
→ geographical normalization against CNIG when applicable
→ Apache Iceberg Silver tables
→ MinIO persistent storage
→ Trino SQL access
```

The implementation preserves the original data granularity whenever possible. Aggregation and analytical selection are delegated to the Gold layer.

---

## 2. Implemented Silver Tables

A total of 12 Apache Iceberg tables have been implemented and physically validated.

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

The complete Silver namespace was subsequently reconstructed and the 12 tables were again confirmed from Trino.

---

## 3. Apache Iceberg Persistence

The 12 Silver datasets are persisted as managed Apache Iceberg tables.

The validated warehouse location is:

```text
s3://energy-lakehouse/warehouse/silver/
```

Physical persistence was verified directly in MinIO.

For the inspected Iceberg table, the physical structure contained:

- `data/` for the persisted data files;
- `metadata/` for Apache Iceberg metadata;
- Iceberg metadata JSON files;
- Avro manifest and snapshot files;
- physical data partitions.

For example, `silver_open_meteo_hourly` is physically partitioned using the Iceberg transformation:

```text
days(observation_timestamp)
```

The tables were also validated as Apache Iceberg tables through their physical metadata and catalog representation.

The physical-schema validation confirmed for all 12 tables:

```text
PROVIDER_OK = True
LOCATION_OK = True
```

The validated Silver warehouse locations use:

```text
s3://energy-lakehouse/warehouse/silver/
```

---

## 4. Partitioning

The physical partitioning validated for the Silver tables follows the designed temporal granularities.

| Table | Validated partitioning |
|---|---|
| `silver_aemet_stations` | none |
| `silver_aemet_daily_climatology` | `months(observation_date)` |
| `silver_aemet_current_observations` | `days(observation_timestamp)` |
| `silver_open_meteo_hourly` | `days(observation_timestamp)` |
| `silver_open_meteo_historical_forecast` | `days(observation_timestamp)` |
| `silver_open_meteo_15min` | `days(observation_timestamp)` |
| `silver_cnig_provinces` | none |
| `silver_cnig_autonomous_communities` | none |
| `silver_cnig_municipalities` | none |
| `silver_esios_energy_hourly` | `days(observation_timestamp)` |
| `silver_esios_power_5min` | `days(observation_timestamp)` |
| `silver_esios_installed_capacity_monthly` | `months(observation_timestamp)` |

The physical-schema validation confirmed:

```text
TABLE_COUNT = 12
MISSING_TABLES = []
```

For every table:

```text
PARTITION_OK = True
PROVIDER_OK = True
LOCATION_OK = True
```

Reference/master datasets that do not require temporal partitioning are stored without temporal partitions.

---

## 5. Geographical Normalization

CNIG is implemented as the canonical geographical master for province and autonomous-community normalization.

The implemented resolution process is:

```text
source province
→ deterministic normalization
→ controlled alias fallback when required
→ CNIG province
→ CNIG autonomous community
```

### 5.1 Deterministic normalization

Province names are normalized before comparison with the CNIG master.

The normalization process handles differences such as capitalization and diacritics without changing the geographical meaning of the source value.

Validated examples include:

```text
ALMERIA      → Almería
ARABA/ALAVA  → Araba/Álava
AVILA        → Ávila
CACERES      → Cáceres
CADIZ        → Cádiz
CORDOBA      → Córdoba
JAEN         → Jaén
LEON         → León
MALAGA       → Málaga
```

### 5.2 Controlled aliases

Cases that cannot be resolved exclusively through deterministic normalization use the controlled configuration:

```text
config/province_aliases.json
```

The validated aliases are:

```text
ALICANTE               → Alacant/Alicante
BALEARES               → Illes Balears
CASTELLON              → Castelló/Castellón
STA. CRUZ DE TENERIFE  → Santa Cruz de Tenerife
VALENCIA                → València/Valencia
```

Persisted Silver validation confirmed the following mappings:

| Source province | Province code | Canonical province | Autonomous-community code | Canonical autonomous community |
|---|---|---|---|---|
| `ALICANTE` | `03` | `Alacant/Alicante` | `10` | `Comunitat Valenciana` |
| `BALEARES` | `07` | `Illes Balears` | `04` | `Illes Balears` |
| `CASTELLON` | `12` | `Castelló/Castellón` | `10` | `Comunitat Valenciana` |
| `STA. CRUZ DE TENERIFE` | `38` | `Santa Cruz de Tenerife` | `05` | `Canarias` |
| `VALENCIA` | `46` | `València/Valencia` | `10` | `Comunitat Valenciana` |

### 5.3 Canonical geographical columns

The geographical enrichment materializes:

```text
province_code
province_name
autonomous_community_code
autonomous_community_name
```

in the five applicable meteorological Silver tables:

```text
silver_aemet_stations
silver_aemet_daily_climatology
silver_open_meteo_hourly
silver_open_meteo_historical_forecast
silver_open_meteo_15min
```

The original source province fields remain available for traceability.

`silver_aemet_current_observations` is not enriched through this province-name mechanism because its validated source schema does not contain the province field required by the matching process.

### 5.4 Persisted geographical validation

Persisted-data validation was executed against the five enriched meteorological tables.

Validated result:

```text
silver_aemet_stations                    921 rows     0 unmatched provinces     0 unmatched autonomous communities
silver_aemet_daily_climatology          2420 rows     0 unmatched provinces     0 unmatched autonomous communities
silver_open_meteo_historical_forecast  88416 rows     0 unmatched provinces     0 unmatched autonomous communities
silver_open_meteo_hourly               88416 rows     0 unmatched provinces     0 unmatched autonomous communities
silver_open_meteo_15min               353664 rows     0 unmatched provinces     0 unmatched autonomous communities
```

Therefore, all records in the five applicable persisted Silver tables were successfully resolved to their canonical CNIG province and autonomous community.

The physical-schema validation also confirmed that the expected canonical geographical columns are present in the applicable tables.

---

## 6. Data Quality Validation

Data quality controls were executed against the Silver tables after their physical persistence in Apache Iceberg.

The validation covered:

- expected row counts;
- null natural keys;
- duplicate natural keys;
- null observation timestamps or dates;
- null ingestion timestamps;
- invalid geographical coordinates;
- geographical correspondence against CNIG where applicable;
- temporal granularity.

All 12 persisted Silver tables matched their expected row counts.

No duplicate natural keys were detected in any of the validated tables.

No null natural keys were detected.

No null mandatory observation timestamps or dates were detected.

No null ingestion timestamps were detected.

No invalid coordinates were detected in the datasets where coordinate validation was applicable.

The five meteorological tables requiring canonical province enrichment produced zero unmatched provinces and zero unmatched autonomous communities.

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

```text
Total temporal differences = 29,765
60-minute differences      = 29,399
Non-60-minute differences  = 366
```

A separate inspection of these differences confirmed gaps greater than one hour across several source datasets.

These gaps are preserved in Silver rather than synthetically filled. Silver normalizes and preserves the available source observations; it does not generate missing observations.

---

## 7. Idempotent Silver Writes

Silver persistence uses natural-key-based merge operations against the Apache Iceberg tables.

Idempotency was validated by executing the Silver write process again after data had already been persisted.

The second execution maintained the same target row counts for the previously populated tables instead of inserting duplicate records.

The complete write execution finished successfully for all 12 Silver tables.

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

## 8. Complete Silver Reconstruction

After incorporating the canonical geographical normalization, the Silver tables were reconstructed from the implemented Silver processing code.

The reconstruction completed successfully with:

```text
SILVER ICEBERG TABLE CREATION COMPLETE
```

Trino subsequently exposed exactly the 12 expected tables:

```text
silver_aemet_current_observations
silver_aemet_daily_climatology
silver_aemet_stations
silver_cnig_autonomous_communities
silver_cnig_municipalities
silver_cnig_provinces
silver_esios_energy_hourly
silver_esios_installed_capacity_monthly
silver_esios_power_5min
silver_open_meteo_15min
silver_open_meteo_historical_forecast
silver_open_meteo_hourly
```

After persistence, the validated row counts were:

```text
silver_aemet_stations                     921
silver_aemet_daily_climatology           2420
silver_aemet_current_observations        9688

silver_open_meteo_hourly                88416
silver_open_meteo_historical_forecast  88416
silver_open_meteo_15min                353664

silver_cnig_provinces                      52
silver_cnig_autonomous_communities         19
silver_cnig_municipalities               8132

silver_esios_energy_hourly              30107
silver_esios_power_5min                 13824
silver_esios_installed_capacity_monthly   123
```

This reconstruction verifies that the implemented Silver code is capable of recreating the expected Silver table structure and repopulating the persisted datasets.

---

## 9. Trino Catalog Validation

The Silver namespace and its Apache Iceberg tables were validated from Trino.

The `iceberg` catalog exposed the Silver schema and all 12 implemented Silver tables.

Real SQL queries were executed directly against persisted Iceberg tables.

For example:

```sql
SELECT COUNT(*)
FROM iceberg.silver.silver_open_meteo_hourly;
```

returned:

```text
88416
```

The complete persisted row-count validation from Trino returned:

```text
silver_esios_installed_capacity_monthly   123
silver_esios_power_5min                 13824
silver_esios_energy_hourly              30107
silver_cnig_municipalities               8132
silver_cnig_autonomous_communities         19
silver_cnig_provinces                      52
silver_open_meteo_15min                353664
silver_open_meteo_historical_forecast  88416
silver_open_meteo_hourly                88416
silver_aemet_current_observations        9688
silver_aemet_stations                     921
silver_aemet_daily_climatology           2420
```

Schema inspection through Trino also confirmed the canonical geographical fields in the applicable meteorological tables.

For example, `silver_aemet_stations` contains:

```text
province_code
province_name
autonomous_community_code
autonomous_community_name
```

and `silver_open_meteo_hourly` contains the same four canonical geographical fields.

This confirms that the Silver tables created and populated through Spark are accessible from Trino through the shared Iceberg catalog.

---

## 10. Physical Schema Validation

Automated physical validation was executed against the 12 persisted Apache Iceberg tables.

The validation checked:

- table existence;
- expected table count;
- physical schemas;
- canonical geographical columns where applicable;
- Iceberg partitioning;
- table provider;
- physical warehouse location.

Validated result:

```text
TABLE_COUNT = 12
MISSING_TABLES = []
```

For all 12 tables:

```text
CANONICAL_GEOGRAPHY_SCHEMA_OK = True
PARTITION_OK = True
PROVIDER_OK = True
LOCATION_OK = True
```

No validation result returned `False`.

This confirms that the physical Iceberg implementation matches the approved Silver table structure, including the canonical geographical enrichment where applicable.

---

## 11. Automated Tests

The Silver implementation includes unit, integration, physical-schema, persisted-data and end-to-end validation tests.

The Silver test directory contains tests covering:

- common Silver functionality;
- AEMET transformations;
- AEMET integration with real Bronze data;
- Open-Meteo transformations;
- Open-Meteo integration with real Bronze data;
- CNIG transformations;
- CNIG integration with real Bronze data;
- ESIOS transformations;
- ESIOS integration with real Bronze data;
- geographical normalization;
- Iceberg integration;
- Silver integration;
- physical Iceberg table validation;
- persisted Silver data validation;
- end-to-end Silver validation.

Additional inspection scripts were used during implementation to inspect real Bronze schemas and investigate source-data characteristics such as ESIOS hourly temporal gaps.

Earlier validated unit-test executions included:

```text
AEMET      = 7 passed
Open-Meteo = 7 passed
ESIOS      = 7 passed
```

Integration tests were additionally executed against real Bronze datasets before persistence.

Following the geographical-normalization implementation and the final Silver corrections, the complete Silver test suite was executed successfully.

Validated final result:

```text
73 passed
```

No failing Silver tests remained in the final validated execution.

---

## 12. End-to-End Validation

A complete Silver end-to-end validation was executed using real Bronze data.

The validated processing path was:

```text
Bronze data in MinIO
→ PySpark Silver transformations
→ canonical geographical enrichment where applicable
→ persisted Apache Iceberg Silver tables
→ SQL queries against persisted tables
```

For each of the 12 Silver datasets, the validation compared:

- rows produced by the Bronze-to-Silver transformation;
- rows persisted in the corresponding Iceberg table;
- expected validated row count;
- source and target column order;
- SQL row count obtained from the persisted Iceberg table.

All 12 datasets produced:

```text
SOURCE_TARGET_COUNT_MATCH = True
EXPECTED_COUNT_MATCH = True
COLUMN_ORDER_MATCH = True
```

SQL queries against all 12 persisted Iceberg tables also returned their expected validated row counts.

The execution completed with:

```text
SILVER END-TO-END VALIDATION COMPLETE
```

without validation exceptions.

The later full reconstruction and geographical validation additionally confirmed that the canonical geographical enrichment remains valid in the physically persisted tables.

---

## 13. Final Validation Result

The implemented Silver layer has been technically validated across the complete processing chain.

The validation confirms:

- 12 Apache Iceberg Silver tables implemented;
- real Bronze data transformed with PySpark;
- natural-key deduplication applied;
- expected schemas and partitioning validated;
- CNIG used as the canonical geographical master;
- deterministic province normalization implemented;
- controlled province aliases externalized in `config/province_aliases.json`;
- canonical province and autonomous-community fields persisted where applicable;
- zero unmatched provinces in the five applicable meteorological tables;
- zero unmatched autonomous communities in the five applicable meteorological tables;
- data-quality controls executed;
- expected temporal granularities preserved;
- known source-data gaps detected and preserved without synthetic filling;
- data physically persisted in MinIO;
- Iceberg metadata and physical data files present;
- idempotent Silver writes validated;
- Silver tables accessible through Trino;
- all 12 Silver tables successfully reconstructed and repopulated;
- physical schema, partitioning, provider and location validation successful for all 12 tables;
- end-to-end Bronze-to-Silver processing validated;
- final Silver automated test suite completed with `73 passed`.

Therefore, the implemented Silver data-processing and persistence flow is considered technically validated for the datasets and executions documented above.