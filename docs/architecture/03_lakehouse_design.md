# Lakehouse Design

## 1. Lakehouse Architecture

The platform follows a Lakehouse architecture, combining the flexibility of a Data Lake with the reliability and analytical capabilities of a Data Warehouse.

Data is stored as Apache Iceberg tables on MinIO object storage, allowing the platform to maintain a unified storage layer for both raw and curated datasets while supporting efficient analytical workloads.

The Lakehouse is organized using the Medallion Architecture pattern, separating the data lifecycle into multiple layers with clearly defined responsibilities. This approach improves data quality, simplifies maintenance, and enables the progressive refinement of datasets from ingestion to business-ready analytics.

The Lakehouse acts as the central repository of the platform, supporting historical storage, incremental updates, distributed processing, and analytical querying through Apache Spark.

## 2. Medallion Architecture

The Lakehouse is organized following the Medallion Architecture pattern, which separates data into multiple layers according to its level of processing and refinement.

This approach enables data quality improvements to be applied progressively while preserving the original datasets and maintaining a clear separation between raw, curated, and business-ready data.

Each layer has a specific responsibility within the data lifecycle:

- **Bronze** stores the raw data ingested from the public APIs with minimal modifications.
- **Silver** contains validated, cleaned, standardized, and integrated datasets prepared for analytical processing.
- **Gold** stores curated datasets optimized for reporting, dashboards, and business analysis.

By separating the data into these layers, the platform improves maintainability, traceability, and scalability while reducing the complexity of downstream analytical processes.

## 3. Bronze Layer

The Bronze layer is the entry point of the Lakehouse and stores data ingested directly from the public APIs.

Its primary purpose is to preserve the original information received from each data source while ensuring that the ingestion process is reliable and traceable. Only the minimum transformations required to store the data consistently are applied at this stage.

The Bronze layer serves as the immutable source of truth for the platform, allowing datasets to be reprocessed whenever transformation logic changes or new analytical requirements arise.

Main responsibilities of the Bronze layer include:

- Storing raw data from public APIs.
- Preserving the original information.
- Recording ingestion metadata.
- Supporting historical data storage.
- Providing the source data for downstream processing.

## 4. Silver Layer

The Silver layer contains validated, cleaned, standardized, and enriched datasets derived from the Bronze layer.

At this stage, data quality issues are addressed, data types are standardized, missing or invalid values are handled when appropriate, and datasets from different sources are prepared for integration.

The objective of the Silver layer is to create reliable and consistent datasets that can be reused by multiple analytical processes without repeatedly applying the same transformations.

Main responsibilities of the Silver layer include:

- Validating ingested data.
- Cleaning and standardizing datasets.
- Harmonizing data formats and units.
- Preparing datasets for integration.
- Producing high-quality analytical datasets.

## 5. Gold Layer

The Gold layer contains curated datasets designed for analytical consumption, reporting, and dashboard development.

Data in this layer is derived from the Silver layer and is organized according to the analytical requirements of the project. It may include integrated datasets, aggregated metrics, calculated indicators, and structures optimized for efficient querying.

The objective of the Gold layer is to provide business-ready data that can be consumed directly through Spark SQL and Apache Superset without requiring additional complex transformations.

Main responsibilities of the Gold layer include:

- Integrating meteorological and energy datasets.
- Creating analytical aggregates and indicators.
- Structuring data for reporting and dashboard consumption.
- Optimizing datasets for analytical queries.
- Providing consistent and reusable data products.

## 6. Data Lifecycle

Data progresses through the Lakehouse following a structured refinement process based on the Medallion Architecture.

The lifecycle begins in the Bronze layer, where raw data is ingested from the public APIs and stored with minimal technical transformations.

The data is then promoted to the Silver layer, where quality validation, cleansing, standardization, and preparation for integration are performed.

Finally, the processed datasets are transformed into business-ready analytical models in the Gold layer, where they become available for querying through Spark SQL and visualization in Apache Superset.

This layered approach preserves data lineage, improves traceability, and allows transformations to be applied progressively while maintaining a clear separation between ingestion, data preparation, and analytical consumption.

## 7. Design Principles

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
  
  The Gold layer exposes curated datasets that can be consumed directly by analytical tools without requiring additional transformations.