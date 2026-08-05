# Architecture Overview

## 1. Project Objective

The objective of this project is to design and implement an Open Source Lakehouse platform for the integration, processing, storage, and analysis of public meteorological and energy data from Spain.

The platform will ingest data from AEMET, Open-Meteo, and REE/ESIOS, perform an initial historical load, and subsequently execute periodic incremental updates.

The solution will be deployed locally using Docker Compose and will rely entirely on Open Source technologies. The platform will process data with Python and Apache Spark, store it in Apache Iceberg tables on MinIO, orchestrate workflows with Apache Airflow, and provide analytical access through Spark SQL and Apache Superset.

The main analytical objective is to enable the study of relationships between meteorological conditions, electricity generation, energy demand, and electricity prices at autonomous community level.

## 2. High-Level Architecture

The platform follows a Lakehouse architecture to support the complete data lifecycle, from data ingestion to analytical visualization.

Data is collected from multiple public APIs, processed using Apache Spark, and stored in Apache Iceberg tables on MinIO object storage. Apache Airflow orchestrates the execution of the different data pipelines, while PostgreSQL provides metadata storage for the platform services. Finally, Apache Superset enables interactive dashboards and analytical reporting through Spark SQL.

The entire platform is deployed locally using Docker Compose, providing a reproducible and modular environment that can be easily recreated on any compatible machine.

## 3. Main Components

The platform is composed of the following main components:

- **Public APIs**
  - AEMET
  - Open-Meteo
  - REE/ESIOS

  These APIs provide the meteorological and energy data required by the platform.

- **Python**
  
  Responsible for data ingestion, API communication, configuration management, and auxiliary processing tasks.

- **Apache Spark (PySpark)**
  
  The distributed processing engine used to validate, clean, transform, and integrate data before storing it in the Lakehouse.

- **Apache Iceberg**
  
  Provides the table format used by the Lakehouse, enabling efficient storage, schema evolution, partitioning, and analytical queries.

- **MinIO**
  
  S3-compatible object storage used to persist all Lakehouse data.

- **PostgreSQL**
  
  Stores metadata required by the platform services, including Airflow metadata and the Iceberg catalog.

- **Apache Airflow**
  
  Orchestrates and schedules the execution of ingestion, transformation, and update workflows.

- **Spark SQL**
  
  Provides SQL-based analytical access to the Lakehouse data.

- **Apache Superset**
  
  Business Intelligence platform used to build dashboards and visualize analytical results.

- **Docker Compose**
  
  Deploys and manages the complete platform as a reproducible local environment.

## 4. End-to-End Data Flow

The platform follows a sequential data processing workflow that covers the complete data lifecycle.

1. **Data Ingestion**
   
   Data is retrieved from the public APIs (AEMET, Open-Meteo, and REE/ESIOS) using Python-based connectors.

2. **Data Processing**
   
   Apache Spark validates, cleans, transforms, and standardizes the collected data before storing it in the Lakehouse.

3. **Data Storage**
   
   Processed data is stored as Apache Iceberg tables on MinIO object storage, following a multi-layer Lakehouse architecture.

4. **Workflow Orchestration**
   
   Apache Airflow schedules and coordinates the execution of ingestion and processing pipelines.

5. **Data Analytics**
   
   Spark SQL provides analytical access to the Lakehouse, allowing Apache Superset to build dashboards and interactive visualizations.

## 5. Architectural Decisions

The following architectural decisions have been established during the design phase of the project:

- The platform follows a Lakehouse architecture based entirely on Open Source technologies.
- All services are deployed locally using Docker Compose.
- Data ingestion is implemented in Python.
- Apache Spark (PySpark) is the distributed processing engine.
- Apache Iceberg is used as the Lakehouse table format.
- MinIO provides S3-compatible object storage.
- PostgreSQL stores platform metadata and the Iceberg catalog.
- Apache Airflow orchestrates all data pipelines.
- Spark SQL provides analytical access to the Lakehouse.
- Apache Superset is the Business Intelligence platform.
- The platform performs an initial historical data load followed by periodic incremental updates.
- The analytical model is designed to integrate meteorological and energy data at Autonomous Community level.