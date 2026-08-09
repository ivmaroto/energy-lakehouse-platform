# Lakehouse Design

## 1. Lakehouse Architecture

The platform follows a Lakehouse architecture, combining the flexibility of a Data Lake with the reliability and analytical capabilities traditionally associated with a Data Warehouse.

Data is stored as Apache Iceberg tables on MinIO object storage, allowing the platform to maintain a unified storage layer for both raw and curated datasets while supporting efficient analytical workloads.

The Lakehouse is organized using the Medallion Architecture pattern, separating the data lifecycle into multiple layers with clearly defined responsibilities. This approach improves data quality, simplifies maintenance, and enables the progressive refinement of datasets from ingestion to business-ready analytics.

Apache Spark and Spark SQL provide the distributed processing capabilities required to transform data across the different Lakehouse layers. Apache Iceberg provides the table abstraction over the data stored in MinIO, while Trino provides the distributed SQL query layer used to expose curated analytical datasets to downstream consumers such as Apache Superset.

The Lakehouse therefore acts as the central data repository of the platform, supporting historical storage, incremental updates, distributed processing, and interactive analytical querying.

## 2. Medallion Architecture

The Lakehouse is organized following the Medallion Architecture pattern, which separates data into multiple layers according to its level of processing and refinement.

This approach enables data quality improvements to be applied progressively while preserving the original datasets and maintaining a clear separation between raw, curated, and business-ready data.

Each layer has a specific responsibility within the data lifecycle:

- **Bronze** stores the raw data ingested from the public APIs with minimal modifications.
- **Silver** contains validated, cleaned, standardized, and integrated datasets prepared for analytical processing.
- **Gold** stores curated datasets optimized for reporting, dashboards, KPIs, and analytical consumption.

By separating the data into these layers, the platform improves maintainability, traceability, and scalability while reducing the complexity of downstream analytical processes.

The general data progression is:

```text
Public APIs
    │
    ▼
 Bronze
    │
    ▼
Spark / Spark SQL
    │
    ▼
 Silver
    │
    ▼
Spark / Spark SQL
    │
    ▼
  Gold
    │
    ▼
 Trino
    │
    ▼
Superset
```

## 3. Bronze Layer

The Bronze layer is the entry point of the Lakehouse and stores data ingested directly from the public APIs.

Its primary purpose is to preserve the original information received from each data source while ensuring that the ingestion process is reliable and traceable. Only the minimum transformations required to store the data consistently are applied at this stage.

The Bronze layer serves as the immutable source of truth for the platform, allowing datasets to be reprocessed whenever transformation logic changes or new analytical requirements arise.

Main responsibilities of the Bronze layer include:

- Storing raw data from public APIs.
- Preserving the original information.
- Recording ingestion metadata.
- Supporting historical data storage.
- Supporting incremental data ingestion.
- Providing the source data for downstream processing.

The Bronze layer contains data originating from:

- AEMET.
- Open-Meteo.
- REE / ESIOS.

## 4. Silver Layer

The Silver layer contains validated, cleaned, standardized, and enriched datasets derived from the Bronze layer.

At this stage, data quality issues are addressed, data types are standardized, missing or invalid values are handled when appropriate, and datasets from different sources are prepared for integration.

Apache Spark and Spark SQL perform the transformations required to promote datasets from Bronze to Silver.

The objective of the Silver layer is to create reliable and consistent datasets that can be reused by multiple analytical processes without repeatedly applying the same transformations.

Main responsibilities of the Silver layer include:

- Validating ingested data.
- Cleaning and standardizing datasets.
- Harmonizing data formats and units.
- Handling invalid or missing values.
- Preparing datasets for integration.
- Standardizing geographical and temporal dimensions.
- Producing high-quality analytical datasets.

The Silver layer provides the standardized foundation required to combine meteorological and energy information in subsequent processing stages.

## 5. Gold Layer

The Gold layer contains curated datasets designed specifically for analytical consumption, reporting, KPI calculation, and dashboard development.

Data in this layer is derived from the Silver layer and is organized according to the analytical requirements of the project. It may include integrated datasets, aggregated metrics, calculated indicators, and structures optimized for efficient analytical querying.

Apache Spark and Spark SQL are responsible for generating the Gold datasets from the standardized information available in the Silver layer.

Once created, Gold datasets are stored as Apache Iceberg tables and exposed through Trino for analytical consumption.

Apache Superset accesses these datasets through Trino rather than interacting directly with the Spark processing layer.

The analytical consumption path is therefore:

```text
Silver
   │
   ▼
Spark / Spark SQL
   │
   ▼
 Gold
   │
   ▼
Apache Iceberg
   │
   ▼
 Trino
   │
   ▼
Apache Superset
```

Main responsibilities of the Gold layer include:

- Integrating meteorological and energy datasets.
- Creating analytical aggregates and indicators.
- Calculating Key Performance Indicators.
- Structuring data for reporting and dashboard consumption.
- Optimizing datasets for analytical queries.
- Supporting comparisons between Autonomous Communities.
- Supporting temporal analysis.
- Providing consistent and reusable analytical data products.

The objective of the Gold layer is to ensure that downstream analytical consumers do not need to implement additional complex transformations.

## 6. Apache Iceberg and Storage Model

Apache Iceberg provides the table format used across the Lakehouse layers.

The physical data and table metadata are stored using MinIO, which provides an S3-compatible object storage interface.

This architecture separates the logical table representation from the underlying object storage.

Conceptually:

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

Both Spark and Trino can therefore operate over the same Lakehouse tables while serving different purposes.

Apache Spark is primarily responsible for data processing and table creation or modification, while Trino provides SQL-oriented analytical access to the resulting datasets.

This separation is one of the main architectural characteristics of the platform.

## 7. Data Lifecycle

Data progresses through the Lakehouse following a structured refinement process based on the Medallion Architecture.

The lifecycle begins in the Bronze layer, where raw data is ingested from the public APIs and stored with minimal technical transformations.

The data is then promoted to the Silver layer using Apache Spark, where quality validation, cleansing, standardization, and preparation for integration are performed.

Silver datasets are subsequently processed to generate the Gold analytical layer. At this stage, datasets from different domains can be integrated and transformed into aggregates, indicators, and analytical models.

Once the Gold datasets are available as Apache Iceberg tables, Trino exposes them through SQL for analytical consumption.

Apache Superset uses this query layer to create interactive dashboards, KPIs, reports, and exploratory visualizations.

The complete lifecycle can therefore be represented as:

```text
AEMET ────────┐
Open-Meteo ───┼──► Ingestion
REE / ESIOS ──┘        │
                       ▼
                    Bronze
                       │
                       ▼
                Spark / Spark SQL
                       │
                       ▼
                    Silver
                       │
                       ▼
                Spark / Spark SQL
                       │
                       ▼
                     Gold
                       │
                       ▼
               Apache Iceberg
                       │
                       ▼
                     Trino
                       │
                       ▼
               Apache Superset
```

This layered approach preserves data lineage, improves traceability, and allows transformations to be applied progressively while maintaining a clear separation between ingestion, processing, storage, querying, and analytical consumption.

## 8. Processing and Query Separation

A fundamental architectural decision is the separation between data processing and interactive analytical querying.

Apache Spark and Spark SQL are responsible for:

- Data validation.
- Data cleansing.
- Dataset standardization.
- Data integration.
- Bronze-to-Silver transformations.
- Silver-to-Gold transformations.
- Writing and maintaining Apache Iceberg tables.

Trino is responsible for:

- Providing SQL access to curated Lakehouse datasets.
- Executing interactive analytical queries.
- Exposing Gold datasets to Apache Superset.
- Decoupling analytical workloads from Spark processing workloads.

This separation prevents the visualization layer from depending directly on the processing engine and creates a more modular architecture.

## 9. Design Principles

The Lakehouse has been designed according to the following principles:

- **Data immutability**

  Raw data stored in the Bronze layer is preserved to ensure traceability and allow data to be reprocessed if transformation logic changes.

- **Progressive refinement**

  Data quality improvements are applied incrementally as datasets move through the Medallion Architecture.

- **Single source of truth**

  Each layer has a clearly defined responsibility, avoiding duplicated transformation logic across the platform.

- **Scalability**

  The architecture supports future growth in data volume, additional data sources, and new analytical use cases without requiring major structural changes.

- **Modularity**

  Each component of the Lakehouse can evolve independently while maintaining well-defined interfaces between layers.

- **Maintainability**

  The separation of responsibilities simplifies development, testing, debugging, and long-term maintenance.

- **Analytical readiness**

  The Gold layer exposes curated datasets specifically designed for analytical consumption without requiring additional complex transformations.

- **Processing and query separation**

  Apache Spark handles distributed data processing while Trino provides the interactive SQL query layer.

- **Technology interoperability**

  Apache Iceberg provides a common Lakehouse table format that can be accessed by multiple compatible processing and analytical engines.

- **Governed analytical consumption**

  Analytical consumers access curated Gold datasets rather than raw or intermediate Lakehouse layers.