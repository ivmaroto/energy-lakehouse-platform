# Data Flow

## 1. Overview

The platform follows a structured data flow that covers the complete lifecycle of each dataset, from data acquisition to analytical consumption.

Data is retrieved from multiple public APIs, ingested using Python, processed using Apache Spark and Spark SQL, and stored in the Lakehouse following the Medallion Architecture.

Apache Iceberg provides the Lakehouse table format over MinIO object storage. Once datasets reach the Gold layer, Trino provides the distributed SQL query interface used by Apache Superset for analytical consumption and visualization.

The processing workflow has been designed to support both an initial historical data load and subsequent incremental updates, ensuring that the analytical datasets remain consistent and up to date while preserving historical information.

The overall data flow is designed to be modular, reproducible, and automated through the orchestration layer implemented with Apache Airflow.

The high-level data flow is:

```text
Public APIs
    │
    ▼
Python Ingestion
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
Apache Iceberg / MinIO
    │
    ▼
   Trino
    │
    ▼
Apache Superset
```

## 2. Historical Data Ingestion

The first execution of the platform performs a historical data ingestion process to populate the Lakehouse with the historical information available from each public API.

Each data source is queried independently, retrieving the accessible historical records according to the capabilities and limitations of the corresponding API.

The historical ingestion process follows these general stages:

1. The source API is queried using its corresponding Python connector.
2. The API response is retrieved and technically validated.
3. Ingestion metadata is generated where required.
4. The retrieved information is stored in the Bronze layer.
5. Apache Spark processes the Bronze datasets.
6. Validated and standardized data is promoted to the Silver layer.
7. Analytical datasets are generated in the Gold layer.

The historical ingestion process is primarily executed during the initial population of the platform. A complete historical reload can also be performed if the environment is rebuilt or if data reprocessing becomes necessary.

Preserving the Bronze data allows downstream datasets to be regenerated without necessarily retrieving the original information again from the external APIs.

## 3. Incremental Data Ingestion

After the initial historical load, the platform performs periodic incremental data ingestion to keep the Lakehouse synchronized with the latest information published by the public APIs.

Instead of retrieving the complete historical dataset again, each execution requests only the new data required since the previous successful ingestion whenever the source API supports this strategy.

The exact incremental retrieval mechanism may vary between AEMET, Open-Meteo, and REE/ESIOS according to the capabilities and limitations of each source.

The general incremental flow is:

```text
Last Successful Ingestion
          │
          ▼
      API Request
          │
          ▼
      New Records
          │
          ▼
        Bronze
          │
          ▼
        Silver
          │
          ▼
         Gold
```

Incremental datasets are first stored in the Bronze layer before being validated, transformed, and promoted through the Silver and Gold layers.

This approach minimizes unnecessary data transfer and processing while ensuring that the analytical datasets remain updated.

## 4. Data Processing Flow

Once data has been ingested into the Bronze layer, it progresses through the Lakehouse following the Medallion Architecture.

Apache Spark and Spark SQL provide the distributed processing capabilities required to transform datasets between the different layers.

The processing workflow consists of a sequence of transformations that progressively improve data quality and prepare datasets for analytical consumption.

The processing flow follows these stages:

1. Raw data is ingested and stored in the Bronze layer.
2. Apache Spark reads the Bronze datasets.
3. Data quality validation, cleansing, and standardization are performed.
4. Validated datasets are written to the Silver layer.
5. Silver datasets are integrated and transformed according to the analytical model.
6. Aggregations, indicators, and analytical datasets are generated.
7. The resulting datasets are written to the Gold layer as Apache Iceberg tables.
8. Trino exposes the Gold datasets through SQL.
9. Apache Superset queries the analytical datasets through Trino.

Each Lakehouse processing stage receives data from the preceding layer, ensuring a clear separation of responsibilities while maintaining data lineage throughout the platform.

The processing and consumption responsibilities are therefore separated:

```text
                DATA PROCESSING

Bronze ──► Spark / Spark SQL ──► Silver
                             
Silver ──► Spark / Spark SQL ──► Gold


               DATA CONSUMPTION

Gold ──► Trino ──► Apache Superset
```

## 5. Data Validation

Data validation is integrated into the processing workflow to ensure the reliability and consistency of the analytical datasets.

Validation is primarily performed during the transition from the Bronze layer to the Silver layer, where data quality issues can be identified and handled before further processing.

The validation process includes:

- Verifying data integrity after ingestion.
- Detecting missing or invalid values.
- Validating data types and formats.
- Checking temporal consistency.
- Identifying duplicate records when applicable.
- Validating expected schema structures.
- Verifying geographical identifiers when applicable.
- Ensuring that datasets meet the minimum quality requirements for analytical processing.

Validation rules are adapted to the characteristics of each source because AEMET, Open-Meteo, and REE/ESIOS expose different structures, variables, and temporal characteristics.

Datasets that successfully pass the required validation and standardization processes can be promoted to the Silver layer.

## 6. Data Integration

The platform integrates information originating from different meteorological and energy sources.

The Silver layer provides standardized datasets that can subsequently be combined according to common dimensions such as:

- Time.
- Date.
- Autonomous Community.
- Meteorological variables.
- Energy variables.

Apache Spark performs the required joins, transformations, and aggregations to generate integrated datasets.

The resulting analytical models are stored in the Gold layer and are designed to support the analysis of relationships between meteorological conditions and the Spanish electricity system.

The integration flow can be represented as:

```text
AEMET Silver ───────┐
                    │
Open-Meteo Silver ──┼──► Spark ──► Integrated Gold datasets
                    │
REE/ESIOS Silver ───┘
```

## 7. Data Consumption

The final stage of the data flow focuses on analytical consumption.

Once datasets have been processed and curated in the Gold layer, they become available for analytical querying through Trino.

Trino accesses the Apache Iceberg tables stored in the Lakehouse and provides a distributed SQL interface for downstream analytical consumers.

Apache Superset connects to Trino to build:

- Interactive dashboards.
- Analytical charts.
- Time-series visualizations.
- Regional comparisons.
- Key Performance Indicators.
- Integrated meteorological and energy analyses.

The analytical consumption path is:

```text
Gold Layer
    │
    ▼
Apache Iceberg
    │
    ▼
   Trino
    │
    ▼
Apache Superset
    │
    ▼
Dashboards / KPIs / Analysis
```

The Gold layer represents the only Lakehouse layer intended for Business Intelligence consumption, ensuring that reports and dashboards are generated from validated, standardized, and business-ready datasets.

This approach guarantees consistency across analytical outputs while preventing the visualization layer from depending on raw or intermediate datasets.

## 8. Workflow Orchestration

Apache Airflow coordinates the execution of the complete data flow.

The orchestration layer is responsible for managing dependencies between ingestion and processing tasks and for ensuring that the different stages execute in the required order.

A typical workflow follows this sequence:

```text
Start
  │
  ▼
API Ingestion
  │
  ▼
Bronze Storage
  │
  ▼
Data Validation
  │
  ▼
Bronze → Silver
  │
  ▼
Silver → Gold
  │
  ▼
Quality Checks
  │
  ▼
Gold Available for Trino
  │
  ▼
End
```

Airflow also provides scheduling, retries, execution logs, failure management, and pipeline monitoring.

This orchestration model allows both historical and incremental processes to use a controlled and reproducible workflow.

## 9. End-to-End Data Flow

The complete end-to-end flow of the platform can be represented as:

```text
AEMET ──────────┐
Open-Meteo ─────┼──► Python Connectors
REE / ESIOS ────┘          │
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
                          MinIO
                            ▲
                            │
                          Trino
                            │
                            ▼
                    Apache Superset
                            │
                            ▼
              Dashboards / KPIs / Analysis

           Apache Airflow orchestrates the pipeline
```

This architecture provides a clear separation between data acquisition, storage, processing, analytical querying, and visualization while maintaining a reproducible Open Source Data Engineering environment.