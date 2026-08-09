# Bronze Storage

## 1. Overview

The Bronze layer is the initial persistence layer of the Energy Lakehouse
Platform.

Its main purpose is to preserve the information acquired from the external data
sources with minimal modification before cleaning, normalization and analytical
transformations are applied.

The Bronze layer receives data from:

- AEMET OpenData.
- Open-Meteo.
- REE / ESIOS.

Both historical and incremental ingestion processes write data to the same
logical Bronze layer.

---

## 2. Objectives

The Bronze layer has the following objectives:

- Preserve the original information returned by each source.
- Maintain separation between data providers.
- Provide traceability of ingestion executions.
- Allow source data to be reprocessed without requesting it again.
- Support historical and incremental ingestion.
- Provide the input for subsequent Silver-layer transformations.

The Bronze layer is not intended to provide the final analytical model of the
platform.

---

## 3. Data Flow

The relationship between ingestion and Bronze storage is:

```text
+-------------------+
| AEMET OpenData    |
+---------+---------+
          |
          |
+---------v---------+
|                   |
| Python Ingestion  |
|                   |
+---------+---------+
          |
          v
+-------------------+
|                   |
|      Bronze       |
|                   |
+---------+---------+
          |
          v
+-------------------+
| Silver Processing |
|     PySpark       |
+-------------------+
```

The same pattern applies to Open-Meteo and REE / ESIOS.

---

## 4. Source Separation

Data is organized by source so that information from different providers remains
independent during ingestion.

The logical structure is:

```text
bronze/
|
|-- aemet/
|
|-- open_meteo/
|
`-- esios/
```

Each source directory can contain additional subdirectories representing the
datasets retrieved from that provider.

For example:

```text
bronze/
|
|-- aemet/
|   `-- <dataset>/
|
|-- open_meteo/
|   `-- <dataset>/
|
`-- esios/
    `-- <dataset>/
```

The definitive dataset names will be established after the API endpoints and
indicators have been technically validated.

---

## 5. Temporal Organization

Bronze data may be organized using temporal partitions.

A conceptual structure is:

```text
bronze/
`-- <source>/
    `-- <dataset>/
        `-- year=YYYY/
            `-- month=MM/
                `-- day=DD/
```

Example:

```text
bronze/
`-- open_meteo/
    `-- weather/
        `-- year=2026/
            `-- month=08/
                `-- day=09/
```

Temporal organization provides several advantages:

- Easier identification of ingestion periods.
- More efficient downstream processing.
- Simplified reprocessing of specific periods.
- Improved traceability.
- Reduced need to scan unrelated data.

The definitive partitioning strategy will depend on the granularity and volume
of each dataset.

---

## 6. Raw Data Preservation

The Bronze layer follows a raw-data preservation principle.

Data obtained from an external API should be stored with minimal modification.

The ingestion layer may perform technical operations required for persistence,
such as:

- Adding ingestion metadata.
- Assigning source identifiers.
- Recording ingestion timestamps.
- Organizing files by source and temporal period.

However, the ingestion layer must not perform transformations such as:

- Business calculations.
- Cross-source joins.
- Geographic harmonization.
- Analytical aggregations.
- Unit standardization for analytical purposes.
- Construction of KPIs.

These operations belong to the Silver and Gold processing layers.

---

## 7. Ingestion Metadata

Technical metadata can be associated with ingested data to improve traceability.

Relevant metadata may include:

```text
source
dataset
ingestion_timestamp
requested_start_date
requested_end_date
ingestion_mode
```

Where appropriate, additional information may be registered, such as the
execution identifier or source endpoint.

The final metadata structure will be defined alongside the implementation of
the persistence component.

---

## 8. Historical Data

Historical ingestion writes the initial datasets into Bronze.

Conceptually:

```text
Historical API requests
          |
          v
+---------------------+
| Historical ingestion|
+----------+----------+
           |
           v
+---------------------+
|       Bronze        |
+---------------------+
```

Large historical periods may be acquired in multiple request windows.

Each successfully acquired interval can be persisted independently, reducing the
impact of failures during long-running historical loads.

---

## 9. Incremental Data

Incremental ingestion writes newly available information to the same logical
Bronze structure.

```text
Initial historical load
          |
          v
       Bronze
          ^
          |
Incremental load 1
          ^
          |
Incremental load 2
          ^
          |
         ...
```

Downstream processing therefore does not need separate storage mechanisms for
historical and incremental acquisition.

---

## 10. Idempotency and Reprocessing

The ingestion architecture must support re-execution of previously requested
periods.

Bronze prioritizes preservation of source data and ingestion traceability.

When a temporal period needs to be acquired again, the storage strategy must
avoid uncontrolled corruption or accidental loss of previously acquired data.

Definitive deduplication and business-level uniqueness rules are applied during
subsequent Lakehouse processing where appropriate.

---

## 11. Local Development Storage

The ingestion layer is designed so that connectors can be developed and tested
independently from the final object-storage infrastructure.

During local development, Bronze output can be written to the project data
directory:

```text
data/
`-- bronze/
    |-- aemet/
    |-- open_meteo/
    `-- esios/
```

This local storage mechanism allows ingestion logic to be tested without
requiring the complete platform infrastructure.

The persistence logic is isolated in the common storage component:

```text
ingestion/common/storage.py
```

This separation prevents API connectors from depending directly on a specific
storage implementation.

---

## 12. Final Lakehouse Storage

In the complete platform deployment, Bronze data will be persisted using the
object-storage infrastructure defined by the Lakehouse architecture.

The platform uses MinIO as its S3-compatible object storage system.

Conceptually:

```text
External APIs
      |
      v
Python ingestion
      |
      v
Storage abstraction
      |
      +--------------------+
      |                    |
      v                    v
Local filesystem       MinIO / S3
Development            Platform
```

This approach allows the ingestion code to remain independent from the
underlying storage backend.

---

## 13. Relationship with Apache Iceberg

Bronze represents the initial landing layer for source data.

Apache Iceberg is used by the Lakehouse architecture for managed analytical
tables.

The definitive use of Iceberg across the Bronze, Silver and Gold layers will be
implemented and validated during the Lakehouse implementation phase.

The ingestion layer therefore remains focused on reliable acquisition and raw
data persistence rather than analytical table transformations.

---

## 14. Relationship with Silver

Bronze provides the input for the Silver processing layer.

```text
Bronze
   |
   | Raw source data
   v
PySpark
   |
   | Cleaning
   | Normalization
   | Geographic harmonization
   | Data-quality processing
   v
Silver
```

This separation makes it possible to modify transformation logic without
requiring the external data to be downloaded again.

---

## 15. Version Control

Data generated by ingestion processes must not be committed to the Git
repository.

Git is used to version:

- Ingestion source code.
- Configuration templates.
- Documentation.
- Tests.
- Infrastructure definitions.

Generated Bronze datasets are runtime data and must remain outside version
control.

The project `.gitignore` must therefore exclude the corresponding runtime data
directories.

---

## 16. Validation Status

The Bronze storage architecture is defined independently from the final
containerized deployment.

During final integration testing, the following elements will be validated:

- Local Bronze persistence.
- Directory and dataset organization.
- Historical data persistence.
- Incremental data persistence.
- Re-execution behaviour.
- Ingestion metadata.
- MinIO connectivity.
- Object persistence in the final storage layer.
- Reading Bronze data from the processing environment.

The results of these tests will be documented in
`06_validation_and_testing.md`.