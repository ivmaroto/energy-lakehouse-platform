# Bronze Storage

## 1. Overview

The Bronze layer is the initial persistence layer of the Energy Lakehouse
Platform.

Its main purpose is to preserve the information acquired from external data
sources with minimal modification before cleaning, normalization,
deduplication and analytical transformations are applied.

The Bronze layer receives data from:

- AEMET OpenData.
- Open-Meteo.
- REE / ESIOS.

Both historical and incremental ingestion processes write data to the same
logical Bronze layer.

The production-like storage backend used by the platform is MinIO, providing
S3-compatible object storage.

---

## 2. Objectives

The Bronze layer has the following objectives:

- Preserve the information returned by each source.
- Maintain separation between data providers and datasets.
- Provide traceability of individual ingestion executions.
- Allow source data to be reprocessed without requesting it again.
- Support historical and incremental ingestion.
- Preserve repeated acquisitions without overwriting previous objects.
- Store technical ingestion metadata.
- Provide the input for subsequent Silver-layer transformations.

The Bronze layer is not intended to provide the final analytical model of the
platform.

---

## 3. Data Flow

The implemented relationship between ingestion and Bronze storage is:

```text
+-------------------+
| External APIs     |
| AEMET / Open-Meteo|
| ESIOS             |
+---------+---------+
          |
          v
+-------------------+
| Python Ingestion  |
+---------+---------+
          |
          v
+-------------------+
| Common Storage    |
| Component         |
+---------+---------+
          |
          v
+-------------------+
| MinIO             |
| Bronze Layer      |
+---------+---------+
          |
          v
+-------------------+
| Silver Processing |
| PySpark           |
+-------------------+
```

The persistence logic is centralized in:

```text
ingestion/common/storage.py
```

This prevents individual source connectors from implementing their own object
storage logic.

---

## 4. Source and Dataset Separation

Bronze objects are organized first by source and then by dataset.

The implemented logical structure is:

```text
bronze/
|
|-- aemet/
|   |-- current_observations/
|   |-- daily_climatological_values/
|   |-- radiation/
|   `-- stations/
|
|-- open_meteo/
|   |-- weather/
|   |-- weather_hourly/
|   |-- weather_15min/
|   `-- weather_historical_forecast/
|
`-- esios/
    |-- demand_real_5min/
    |-- demanda_real/
    |-- generacion_medida_eolica_terrestre/
    |-- potencia_instalada_eolica/
    |-- solar_photovoltaic_generation/
    `-- <additional configured datasets>/
```

This structure maintains clear separation between providers while allowing
multiple datasets from the same provider to coexist independently.

---

## 5. Temporal Partitioning

Bronze objects use the following partition structure:

```text
bronze/
`-- <source>/
    `-- <dataset>/
        `-- year=YYYY/
            `-- month=MM/
                `-- day=DD/
                    `-- <object>
```

The `year`, `month` and `day` partitions represent the **ingestion date** of
the Bronze object.

They do not necessarily represent the observation date or requested source
period.

For example, an ESIOS dataset requested for:

```text
2025-08-13
```

and ingested on:

```text
2026-08-15
```

is stored under:

```text
year=2026/month=08/day=15/
```

The requested source period is preserved separately in the object's ingestion
metadata.

This design provides traceability of when information entered the platform
while retaining the source temporal context inside the persisted object.

---

## 6. Object Naming

Each Bronze object filename includes both the UTC ingestion timestamp and a UUID.

Example:

```text
esios_demand_real_5min_20260815T102355585017Z_<uuid>.json
```

This prevents a new successful acquisition from overwriting an existing
Bronze object.

The combination of:

```text
source
dataset
ingestion-date partition
ingestion timestamp
```

provides technical traceability for each acquisition.

---

## 7. Raw Data Preservation

The Bronze layer follows a raw-data preservation principle.

Data obtained from an external source is stored with minimal technical
modification.

The ingestion layer performs operations required for persistence and
traceability, including:

- Adding ingestion metadata.
- Assigning source and dataset identifiers.
- Recording the ingestion timestamp.
- Recording requested temporal boundaries.
- Organizing objects by source, dataset and ingestion date.
- Serializing data for object storage.

The Bronze ingestion layer does not perform:

- Business calculations.
- Cross-source joins.
- Geographic harmonization.
- Analytical aggregations.
- Analytical unit standardization.
- KPI construction.
- Definitive deduplication.

These operations belong to subsequent Lakehouse processing layers.

---

## 8. JSON Bronze Structure

JSON-based datasets are persisted using a wrapper containing technical metadata
and the acquired source payload.

Conceptually:

```json
{
  "metadata": {
    "source": "<source>",
    "dataset": "<dataset>",
    "ingestion_mode": "<mode>",
    "ingestion_timestamp": "<UTC timestamp>",
    "requested_start_date": "<value or null>",
    "requested_end_date": "<value or null>",
    "<source_specific_key>": "<source-specific value>"
  },
  "data": {
    "...": "source payload"
  }
}
```

The exact content of `data` depends on the source API.

In addition to the common technical metadata fields, ingestion processes can
persist source- or dataset-specific metadata when required for traceability.

Examples include the AEMET station identifier, the ESIOS indicator identifier,
and Open-Meteo location information such as location identifier, latitude and
longitude.

This structure has been validated using real AEMET, Open-Meteo and ESIOS
acquisitions stored in MinIO.

---

## 9. Validated Metadata

The implemented Bronze metadata contains the following common fields:

```text
source
dataset
ingestion_mode
ingestion_timestamp
requested_start_date
requested_end_date
```

Additional source- or dataset-specific metadata can also be included when
required for traceability. Currently implemented examples include:

```text
AEMET       -> station_id
ESIOS       -> indicator_id
Open-Meteo  -> location_id, latitude, longitude
```

For datasets without an explicit requested temporal window, the corresponding
date fields may be `null`.

For high-frequency ingestion using exact datetime windows, the requested
boundaries can contain complete UTC datetime values.

This allows the acquisition window to remain traceable independently from the
physical ingestion-date partition.

---

## 10. Source Format Preservation

Most API responses used by the project are persisted as JSON.

AEMET radiation data is an exception.

The radiation endpoint provides source information as text/CSV, and the Bronze
layer preserves this representation as a `.csv` object.

Example:

```text
bronze/aemet/radiation/
`-- year=2026/
    `-- month=08/
        `-- day=15/
            `-- aemet_radiation_<timestamp>.csv
```

Analytical parsing of this dataset is separated from raw Bronze persistence.

---

## 11. Historical Data

Historical ingestion writes initial source datasets into the same Bronze
hierarchy used by incremental ingestion.

```text
Historical API requests
          |
          v
+----------------------+
| Historical ingestion |
+----------+-----------+
           |
           v
+----------------------+
|    MinIO / Bronze    |
+----------------------+
```

Large historical periods can be acquired in multiple request windows.

Each successful acquisition is persisted independently, reducing the impact of
failures and allowing individual periods to be reprocessed.

---

## 12. Incremental Data

Incremental ingestion writes newly acquired information to the same logical
Bronze structure.

```text
Historical load --------\
                         \
Incremental load 1 -------> Bronze
                         /
Incremental load 2 ------/
```

Downstream processing therefore uses a common Bronze input regardless of
whether an object originated from historical or incremental acquisition.

---

## 13. Append-Only Strategy

The Bronze layer follows an append-only strategy.

A successful ingestion execution creates a new object rather than updating or
overwriting a previously persisted acquisition.

For example, two executions requesting exactly the same source window produce:

```text
esios_validation_idempotency_<timestamp_A>.json
esios_validation_idempotency_<timestamp_B>.json
```

Both objects are retained.

This behaviour preserves:

- Acquisition history.
- Source traceability.
- Reprocessing capability.
- Evidence of repeated executions.

---

## 14. Idempotency and Duplicate Handling

Bronze is not physically idempotent by design.

Re-executing the same temporal window can produce another Bronze object
containing the same business observations.

This behaviour was explicitly validated during Phase 3.

Two consecutive ESIOS requests using the same:

- Indicator.
- Dataset.
- Start datetime.
- End datetime.

produced different Bronze objects while validation confirmed:

```text
Same requested window: True
Same source data:      True
```

The repeated acquisition therefore does not overwrite or corrupt the previous
object.

Business-level duplicate detection and definitive deduplication are delegated
to the Silver layer.

---

## 15. Error Safety

Validation errors are raised before Bronze persistence whenever possible.

An invalid ESIOS temporal range where:

```text
start_datetime > end_datetime
```

was validated to raise:

```text
InvalidDateRangeError
```

before a Bronze object was persisted.

Existing Bronze information remains unaffected by failed acquisition attempts.

This supports safe retries and independent recovery of failed ingestion
windows.

---

## 16. MinIO Storage

The platform uses MinIO as its S3-compatible object storage system.

Bronze objects are stored in the configured Lakehouse bucket under the
`bronze/` prefix.

The validated deployment uses:

```text
energy-lakehouse/
`-- bronze/
```

Real Phase 3 validation confirmed successful persistence from:

- Local Python ingestion executions.
- Apache Airflow container executions.

The MinIO Python client was also used to enumerate and read the persisted
objects directly.

---

## 17. Validated MinIO Objects

Phase 3 validation confirmed Bronze objects for datasets including:

```text
AEMET
  current_observations
  daily_climatological_values
  radiation
  stations

Open-Meteo
  weather
  weather_hourly
  weather_15min
  weather_historical_forecast

ESIOS
  demand_real_5min
  demanda_real
  generacion_medida_eolica_terrestre
  potencia_instalada_eolica
  solar_photovoltaic_generation
```

Additional temporary validation datasets were also generated during technical
testing.

These validation objects are runtime data and are not part of the Git source
repository.

---

## 18. Real Data Validation

Real Bronze objects were read directly from MinIO during Phase 3 validation.

### AEMET

A validated `current_observations` object contained:

```text
9760 records
```

and included variables such as:

```text
temperature
humidity
precipitation
wind
pressure
station coordinates
```

### ESIOS

A complete daily validation of the 5-minute demand dataset contained:

```text
288 values
```

corresponding to:

```text
24 hours * 12 observations/hour
```

The persisted metadata correctly retained the requested historical date.

### Open-Meteo

A complete `weather_15min` daily object contained:

```text
96 observations
```

corresponding to:

```text
24 hours * 4 observations/hour
```

The validated source payload included meteorological variables such as
temperature, humidity, precipitation, pressure, radiation and wind at several
heights.

---

## 19. Relationship with Apache Airflow

Apache Airflow executes the ingestion components but does not implement Bronze
storage directly.

The flow is:

```text
Airflow
   |
   v
Python ingestion
   |
   v
Common storage component
   |
   v
MinIO / Bronze
```

This integration was validated from inside the Airflow scheduler container.

A real AEMET ingestion executed from the scheduler container successfully
created a Bronze object in MinIO.

---

## 20. Relationship with Apache Iceberg

Bronze is currently the raw landing layer for source acquisitions.

Apache Iceberg is part of the Lakehouse architecture for managed analytical
tables.

The implementation and validation of Iceberg-based processing belongs to the
Lakehouse implementation phase.

Phase 3 therefore focuses on reliable source acquisition and Bronze object
persistence rather than managed analytical-table transformations.

---

## 21. Relationship with Silver

Bronze provides the source input for Silver processing.

```text
Bronze
   |
   | Raw acquisitions
   v
PySpark
   |
   | Parsing
   | Cleaning
   | Normalization
   | Deduplication
   | Geographic harmonization
   | Data-quality processing
   v
Silver
```

In particular, Silver is responsible for resolving duplicated business
observations that can result from Bronze re-executions.

This separation allows transformation and deduplication logic to evolve
without requiring the original external data to be downloaded again.

---

## 22. Version Control

Generated Bronze data is not committed to the Git repository.

Git versions:

- Ingestion source code.
- Airflow definitions.
- Configuration templates.
- Documentation.
- Tests.
- Infrastructure definitions.

Runtime Bronze objects remain in MinIO or ignored local runtime storage.

Sensitive configuration remains outside source control through the project's
environment configuration strategy.

---

## 23. Validation Status

Bronze storage has been technically validated during Phase 3.

The following elements have been confirmed:

- Source-based organization.
- Dataset-based organization.
- Ingestion-date partitioning.
- Unique timestamped object naming.
- Historical persistence.
- Incremental persistence.
- JSON metadata wrapper.
- Raw CSV persistence for AEMET radiation.
- MinIO connectivity.
- Object enumeration in MinIO.
- Direct reading of persisted Bronze objects.
- Real AEMET persistence.
- Real Open-Meteo persistence.
- Real ESIOS persistence.
- Airflow-to-MinIO persistence.
- Append-only re-execution behaviour.
- Preservation of repeated acquisitions.
- Error handling before persistence.
- Separation of Bronze preservation from Silver deduplication.

The Bronze persistence component is therefore considered validated for
Phase 3.