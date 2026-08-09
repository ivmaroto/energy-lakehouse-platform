# Architecture Overview

## 1. Project Objective

The objective of this project is to design and implement an Open Source Lakehouse platform for the integration, processing, storage, and analysis of public meteorological and energy data from Spain.

The platform will ingest data from AEMET, Open-Meteo, and REE/ESIOS, perform an initial historical load, and subsequently execute periodic incremental updates.

The solution will be deployed locally using Docker Compose and will rely entirely on Open Source technologies. The platform will process data with Python and Apache Spark, store it in Apache Iceberg tables on MinIO, orchestrate workflows with Apache Airflow, provide interactive SQL access through Trino, and visualize analytical information using Apache Superset.

The main analytical objective is to enable the study of relationships between meteorological conditions, electricity generation, energy demand, and electricity prices at Autonomous Community level.

## 2. High-Level Architecture

The platform follows a Lakehouse architecture to support the complete data lifecycle, from data ingestion to analytical visualization.

Data is collected from multiple public APIs, processed using Apache Spark, and stored in Apache Iceberg tables on MinIO object storage. Apache Airflow orchestrates the execution of the different data pipelines, while PostgreSQL provides metadata storage for the platform services and the Iceberg catalog.

Trino provides a distributed SQL query layer over the Apache Iceberg tables, decoupling analytical querying from the Spark processing engine. Apache Superset connects to Trino to provide interactive dashboards and analytical reporting.

The entire platform is deployed locally using Docker Compose, providing a reproducible and modular environment that can be recreated on any compatible machine.

## 3. Main Components

The platform is composed of the following main components:

- **Public APIs**

  - AEMET
  - Open-Meteo
  - REE/ESIOS

  These APIs provide the meteorological and energy data required by the platform.

- **Python**

  Responsible for data ingestion, API communication, configuration management, and auxiliary processing tasks.

- **Apache Spark (PySpark / Spark SQL)**

  The distributed processing engine used to validate, clean, transform, and integrate data before storing it in the Lakehouse. Spark SQL can also be used internally during transformation and processing operations.

- **Apache Iceberg**

  Provides the table format used by the Lakehouse, enabling efficient storage, schema evolution, partitioning, and analytical queries.

- **MinIO**

  S3-compatible object storage used to persist the Lakehouse data.

- **PostgreSQL**

  Provides relational metadata storage required by platform services and the Iceberg catalog.

- **Apache Airflow**

  Orchestrates and schedules the execution of ingestion, transformation, and update workflows.

- **Trino**

  Distributed SQL query engine that provides interactive analytical access to Apache Iceberg tables stored in the Lakehouse.

- **Apache Superset**

  Business Intelligence platform used to build dashboards and visualize analytical results. Superset uses Trino as the SQL query layer for Lakehouse analytics.

- **Docker Compose**

  Deploys and manages the complete platform as a reproducible local environment.

## 4. End-to-End Data Flow

The platform follows a sequential data processing workflow that covers the complete data lifecycle.

1. **Data Ingestion**

   Data is retrieved from the public APIs (AEMET, Open-Meteo, and REE/ESIOS) using Python-based connectors.

2. **Data Processing**

   Apache Spark validates, cleans, transforms, and standardizes the collected data.

3. **Data Storage**

   Data is stored using Apache Iceberg tables on MinIO object storage, following the Bronze, Silver, and Gold Lakehouse layers.

4. **Workflow Orchestration**

   Apache Airflow schedules and coordinates the execution of ingestion and processing pipelines.

5. **SQL Query Layer**

   Trino provides distributed SQL access to the Apache Iceberg tables and exposes the analytical datasets to downstream consumers.

6. **Data Analytics and Visualization**

   Apache Superset queries the Lakehouse through Trino to build dashboards and interactive visualizations.

## 5. Architectural Decisions

The following architectural decisions have been established during the design and infrastructure phases of the project:

- The platform follows a Lakehouse architecture based entirely on Open Source technologies.
- All infrastructure services are deployed locally using Docker Compose.
- Data ingestion is implemented in Python.
- Apache Spark (PySpark / Spark SQL) is the distributed data processing engine.
- Apache Iceberg is used as the Lakehouse table format.
- MinIO provides S3-compatible object storage.
- PostgreSQL provides metadata storage for platform services and the Iceberg catalog.
- Apache Airflow orchestrates the data pipelines.
- Trino provides the distributed SQL query layer over Apache Iceberg.
- Apache Superset provides the Business Intelligence and visualization layer through Trino.
- Processing and interactive analytical querying are intentionally separated between Spark and Trino.
- The platform performs an initial historical data load followed by periodic incremental updates.
- The analytical model is designed to integrate meteorological and energy data at Autonomous Community level.