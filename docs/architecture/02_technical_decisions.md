# Technical Decisions

## 1. Design Principles

The architecture of the platform has been designed according to the following principles:

- **Open Source first**  
  The platform is based entirely on Open Source technologies, avoiding dependency on proprietary cloud services or commercial licenses.

- **Reproducibility**  
  The complete environment must be deployable locally using Docker Compose, allowing the platform to be recreated consistently on another compatible machine.

- **Modularity**  
  Each component has a clearly defined responsibility, making it possible to replace, update, or scale individual services without redesigning the entire platform.

- **Scalability**  
  Although the initial deployment is local, the selected technologies support future execution in distributed or cloud environments.

- **Separation of responsibilities**  
  Data ingestion, processing, storage, orchestration, metadata management, querying, and visualization are handled by specialized components.

- **Maintainability**  
  Configuration, source code, infrastructure definitions, and technical documentation are version-controlled in Git.

- **Incremental development**  
  The platform is implemented progressively, starting with historical data ingestion and later supporting periodic incremental updates.

## 2. Technology Stack

### Python

Python has been selected as the primary programming language due to its extensive ecosystem for data engineering and its excellent support for API integration, automation, and data processing.

Within this project, Python is responsible for:

- Connecting to public APIs.
- Managing configuration and authentication.
- Performing historical and incremental data ingestion.
- Supporting the orchestration workflows.
- Executing auxiliary tasks required by the platform.

Python also provides seamless integration with Apache Spark through PySpark, allowing the project to combine traditional programming with distributed data processing.

### Apache Spark

Apache Spark has been selected as the distributed data processing engine of the platform.

Spark provides the scalability and performance required to process large datasets efficiently while supporting complex data transformation workflows. Although the initial volume of data is expected to be moderate, using Spark aligns the project with modern Data Engineering architectures and enables future scalability without requiring significant architectural changes.

The project uses PySpark to integrate Spark's distributed processing capabilities with the Python ecosystem.

Within the platform, Apache Spark is responsible for:

- Reading raw data from the Lakehouse.
- Validating and cleaning datasets.
- Performing data transformations.
- Standardizing data from different sources.
- Integrating meteorological and energy datasets.
- Writing Apache Iceberg tables.
- Providing SQL-based analytical access through Spark SQL.

### Apache Iceberg

Apache Iceberg has been selected as the Lakehouse table format for the platform.

Iceberg provides advanced table management capabilities that overcome many of the limitations of traditional data lakes. Features such as schema evolution, partition evolution, ACID transactions, and time travel make it well suited for analytical workloads and modern Data Engineering architectures.

Using Iceberg also separates the logical representation of the data from the underlying storage, allowing the platform to manage datasets efficiently while maintaining consistency and scalability.

Within the project, Apache Iceberg is responsible for:

- Storing structured datasets in the Lakehouse.
- Managing table metadata.
- Supporting schema evolution.
- Enabling efficient partition management.
- Providing reliable and consistent analytical tables.

### MinIO

MinIO has been selected as the object storage solution for the Lakehouse.

As an S3-compatible object storage system, MinIO provides a lightweight and efficient platform for storing analytical datasets while maintaining full compatibility with modern Data Lake and Lakehouse technologies. Its compatibility with the Amazon S3 API allows seamless integration with Apache Spark and Apache Iceberg.

Using MinIO also enables the entire platform to run locally without relying on external cloud storage services, making the project fully reproducible and independent of proprietary infrastructure.

Within the platform, MinIO is responsible for:

- Storing all Lakehouse data.
- Providing S3-compatible object storage.
- Persisting Apache Iceberg data files and metadata.
- Serving as the central storage layer for analytical datasets.

### PostgreSQL

PostgreSQL has been selected as the relational database management system for storing platform metadata.

Rather than storing analytical data, PostgreSQL supports the internal operation of the platform by managing metadata required by different services. It provides a reliable, lightweight, and widely adopted solution that integrates seamlessly with Apache Airflow and Apache Iceberg.

Its robustness, Open Source nature, and broad ecosystem make it an appropriate choice for local deployments while remaining suitable for production environments.

Within the platform, PostgreSQL is responsible for:

- Storing Apache Airflow metadata.
- Managing the Apache Iceberg catalog.
- Providing persistent metadata storage for platform services.

### Apache Airflow

Apache Airflow has been selected as the workflow orchestration platform for the project.

Airflow enables the automation, scheduling, and monitoring of the complete data pipeline, ensuring that ingestion, transformation, and loading tasks are executed in the correct order. Its Directed Acyclic Graph (DAG) approach provides a clear and maintainable way to define data workflows while supporting dependency management, retries, logging, and error handling.

Using Airflow allows the platform to execute both historical and incremental data loading processes in a reliable and reproducible manner.

Within the platform, Apache Airflow is responsible for:

- Scheduling data ingestion workflows.
- Orchestrating data processing tasks.
- Managing task dependencies.
- Handling retries and execution failures.
- Logging workflow execution.
- Monitoring pipeline status.

### Spark SQL

Spark SQL has been selected as the analytical query engine for the platform.

It provides a standard SQL interface to query Apache Iceberg tables stored in the Lakehouse, allowing analytical workloads to be executed efficiently without requiring additional query engines. Since Spark is already responsible for data processing, using Spark SQL simplifies the overall architecture while maintaining a consistent technology stack.

Spark SQL also integrates seamlessly with Apache Superset, enabling the creation of interactive dashboards and analytical reports directly from the Lakehouse.

Within the platform, Spark SQL is responsible for:

- Querying Apache Iceberg tables.
- Providing SQL-based access to analytical datasets.
- Serving as the data source for Apache Superset.
- Supporting interactive analytical queries.

### Apache Superset

Apache Superset has been selected as the Business Intelligence platform for the project.

Superset provides a web-based environment for exploring data, building interactive dashboards, and creating visualizations without requiring proprietary software. As an Open Source solution, it aligns with the project's objective of building a complete Lakehouse platform using freely available technologies.

Through its integration with Spark SQL, Superset enables users to analyze the processed datasets and explore relationships between meteorological and energy variables using charts, maps, filters, and time-series visualizations.

Within the platform, Apache Superset is responsible for:

- Creating interactive dashboards.
- Visualizing analytical datasets.
- Supporting exploratory data analysis.
- Presenting Key Performance Indicators (KPIs).
- Providing web-based access to analytical results.

### Docker Compose

Docker Compose has been selected as the deployment and service orchestration mechanism for the local platform.

It allows all infrastructure components to be defined and managed from a single declarative configuration file, making the complete environment reproducible and easier to deploy, stop, restart, and maintain.

Using Docker Compose also isolates the different platform services while providing a shared network and persistent storage configuration. This approach simplifies local development and avoids the need to install and configure each technology directly on the host operating system.

Within the platform, Docker Compose is responsible for:

- Defining all platform services.
- Managing service dependencies.
- Creating the shared Docker network.
- Configuring persistent volumes.
- Exposing the required service ports.
- Injecting environment variables and configuration values.
- Reproducing the complete platform on another compatible machine.

## 3. Overall Architecture Rationale

The selected technology stack provides a complete Open Source solution for designing and implementing a modern Lakehouse platform.

Each component has a well-defined responsibility within the architecture: Python handles data ingestion, Apache Spark performs distributed data processing, Apache Iceberg manages analytical tables, MinIO provides object storage, PostgreSQL stores platform metadata, Apache Airflow orchestrates workflows, Spark SQL enables analytical queries, and Apache Superset delivers data visualization.

This separation of responsibilities results in a modular, maintainable, and scalable architecture while keeping the platform simple enough to be deployed locally using Docker Compose. The selected technologies are widely adopted in modern Data Engineering and provide a solid foundation for future extensions without requiring significant architectural changes.