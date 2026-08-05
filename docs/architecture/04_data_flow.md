# Data Flow

## 1. Overview

The platform follows a structured data flow that covers the complete lifecycle of each dataset, from data acquisition to analytical consumption.

Data is retrieved from multiple public APIs, processed using Apache Spark, stored in the Lakehouse following the Medallion Architecture, and finally exposed for analytical querying through Spark SQL and visualization in Apache Superset.

The processing workflow has been designed to support both an initial historical data load and subsequent incremental updates, ensuring that the analytical datasets remain consistent and up to date while preserving the complete historical information.

The overall data flow is designed to be modular, reproducible, and fully automated through the orchestration layer implemented with Apache Airflow.

## 2. Historical Data Ingestion

The first execution of the platform performs a historical data ingestion process to populate the Lakehouse with all the historical information available from each public API.

Each data source is queried independently, retrieving all accessible historical records according to the capabilities and limitations of the corresponding API.

The retrieved datasets are validated and stored in the Bronze layer before being processed through the subsequent Lakehouse layers.

The historical ingestion process is executed only once during the initial deployment of the platform. Future executions are only required if the platform is rebuilt or if a complete data reload becomes necessary.

## 3. Incremental Data Ingestion

After the initial historical load, the platform performs periodic incremental data ingestion to keep the Lakehouse synchronized with the latest information published by the public APIs.

Instead of retrieving the complete historical dataset again, each execution requests only the new data available since the previous successful ingestion. The retrieval strategy is adapted to the capabilities of each API while maintaining a consistent ingestion workflow across the platform.

Incremental datasets are first stored in the Bronze layer before being validated, transformed, and promoted through the Silver and Gold layers.

This approach minimizes data transfer, reduces processing time, and ensures that the analytical datasets remain continuously up to date.

## 4. Data Processing Flow

Once data has been ingested into the Bronze layer, it progresses through the Lakehouse following the Medallion Architecture.

The processing workflow consists of a sequence of transformations that progressively improve data quality and prepare datasets for analytical consumption.

The processing flow follows these stages:

1. Raw data is ingested and stored in the Bronze layer.
2. Data quality validation and standardization are performed in the Silver layer.
3. Clean datasets are integrated and transformed into analytical models in the Gold layer.
4. The Gold datasets become available for querying through Spark SQL.
5. Apache Superset consumes the analytical datasets to generate dashboards and visualizations.

Each stage receives data exclusively from the previous layer, ensuring a clear separation of responsibilities while maintaining full data lineage throughout the platform.

## 5. Data Validation

Data validation is integrated into the processing workflow to ensure the reliability and consistency of the analytical datasets.

Validation is primarily performed during the transition from the Bronze layer to the Silver layer, where data quality issues can be identified and corrected before further processing.

The validation process includes:

- Verifying data integrity after ingestion.
- Detecting missing or invalid values.
- Validating data types and formats.
- Checking temporal consistency.
- Identifying duplicate records when applicable.
- Ensuring that datasets meet the minimum quality requirements for analytical processing.

Specific validation rules will be defined during the implementation phase according to the characteristics of each public API.

## 6. Data Consumption

The final stage of the data flow focuses on analytical consumption.

Once datasets have been processed and curated in the Gold layer, they become available for analytical querying through Spark SQL. These datasets are then consumed by Apache Superset to build interactive dashboards, reports, and visualizations.

The Gold layer represents the only data source used for analytical consumption, ensuring that all reports are generated from validated, standardized, and business-ready datasets.

This approach guarantees consistency across all analytical outputs while preventing reporting tools from accessing intermediate or raw datasets.