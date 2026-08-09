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
- Supporting SQL-based transformations and processing through Spark SQL.

### Spark SQL

Spark SQL is used as part of the Apache Spark processing layer.

It provides SQL capabilities within Spark jobs and allows transformations to be expressed using SQL when appropriate. This complements the PySpark DataFrame API and provides flexibility when implementing transformations between the Bronze, Silver, and Gold layers.

Unlike the initial architecture design, Spark SQL is not used as the primary interactive analytical query layer. This responsibility is assigned to Trino, allowing processing workloads and analytical query workloads to remain separated.

Within the platform, Spark SQL is responsible for:

- Supporting SQL-based data transformations.
- Querying intermediate datasets during processing.
- Complementing PySpark processing workflows.
- Working with Apache Iceberg tables from the Spark processing layer.

### Apache Iceberg

Apache Iceberg has been selected as the Lakehouse table format for the platform.

Iceberg provides advanced table management capabilities that overcome many of the limitations of traditional data lakes. Features such as schema evolution, partition evolution, ACID transactions, and time travel make it well suited for analytical workloads and modern Data Engineering architectures.

Using Iceberg also separates the logical representation of the data from the underlying storage, allowing the platform to manage datasets efficiently while maintaining consistency and scalability.

The use of Iceberg also enables different processing and query engines, such as Apache Spark and Trino, to operate over the same Lakehouse tables.

Within the project, Apache Iceberg is responsible for:

- Storing structured datasets in the Lakehouse.
- Managing table metadata.
- Supporting schema evolution.
- Enabling efficient partition management.
- Providing reliable and consistent analytical tables.
- Providing a common table layer accessible from Spark and Trino.

### MinIO

MinIO has been selected as the object storage solution for the Lakehouse.

As an S3-compatible object storage system, MinIO provides a lightweight and efficient platform for storing analytical datasets while maintaining compatibility with modern Data Lake and Lakehouse technologies. Its compatibility with the Amazon S3 API allows integration with Apache Spark, Apache Iceberg, and Trino.

Using MinIO also enables the entire platform to run locally without relying on external cloud storage services, making the project fully reproducible and independent of proprietary infrastructure.

Within the platform, MinIO is responsible for:

- Storing all Lakehouse data.
- Providing S3-compatible object storage.
- Persisting Apache Iceberg data files and metadata.
- Serving as the central storage layer for analytical datasets.

### PostgreSQL

PostgreSQL has been selected as the relational database management system for storing platform metadata.

Rather than storing the main analytical datasets, PostgreSQL supports the internal operation of the platform by managing metadata required by different services. It provides a reliable, lightweight, and widely adopted solution that integrates with the components of the architecture.

Its robustness, Open Source nature, and broad ecosystem make it an appropriate choice for local deployments while remaining suitable for production environments.

Within the platform, PostgreSQL is responsible for:

- Storing Apache Airflow metadata.
- Supporting the Apache Iceberg JDBC catalog.
- Providing persistent metadata storage for platform services.
- Supporting metadata required by Apache Superset.

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

### Trino

Trino has been selected as the distributed SQL query engine for the analytical layer of the platform.

The incorporation of Trino introduces a dedicated query layer between the Lakehouse storage and the visualization layer. This separates data processing workloads from interactive analytical workloads.

Apache Spark remains responsible for distributed data processing and transformations, while Trino provides SQL access to the analytical datasets stored as Apache Iceberg tables.

This separation of responsibilities improves the modularity of the architecture and prevents the Business Intelligence layer from depending directly on the Spark processing engine.

Trino also provides a standard SQL interface that can be consumed by analytical tools such as Apache Superset.

Within the platform, Trino is responsible for:

- Providing distributed SQL access to Apache Iceberg tables.
- Querying analytical datasets stored in the Lakehouse.
- Serving as the SQL access layer between the Lakehouse and Apache Superset.
- Supporting interactive analytical queries.
- Decoupling analytical query workloads from Spark processing workloads.

### Apache Superset

Apache Superset has been selected as the Business Intelligence platform for the project.

Superset provides a web-based environment for exploring data, building interactive dashboards, and creating visualizations without requiring proprietary software. As an Open Source solution, it aligns with the project's objective of building a complete Lakehouse platform using freely available technologies.

Through its integration with Trino, Superset enables users to analyze the processed datasets and explore relationships between meteorological and energy variables using charts, maps, filters, and time-series visualizations.

This architecture prevents Superset from querying the Spark processing engine directly and provides a dedicated analytical SQL layer through Trino.

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

Custom Docker images are used where additional dependencies or configuration are required, including Apache Spark, Apache Airflow, and Apache Superset.

Within the platform, Docker Compose is responsible for:

- Defining all platform services.
- Managing service dependencies.
- Creating the shared Docker network.
- Configuring persistent volumes.
- Exposing the required service ports.
- Injecting environment variables and configuration values.
- Building the custom platform images.
- Reproducing the complete platform on another compatible machine.

## 3. Overall Architecture Rationale

The selected technology stack provides a complete Open Source solution for designing and implementing a modern Lakehouse platform.

Each component has a well-defined responsibility within the architecture:

- Python handles data ingestion and API integration.
- Apache Spark performs distributed data processing.
- Spark SQL supports SQL-based transformations within the Spark processing layer.
- Apache Iceberg provides the Lakehouse table format.
- MinIO provides S3-compatible object storage.
- PostgreSQL stores platform metadata and supports the Iceberg catalog.
- Apache Airflow orchestrates data workflows.
- Trino provides distributed SQL access to the Lakehouse.
- Apache Superset provides analytical visualization and Business Intelligence.
- Docker Compose provides reproducible local infrastructure deployment.

A key architectural decision is the separation between **data processing** and **interactive analytical querying**.

Apache Spark and Spark SQL are responsible for processing and transforming the datasets across the Bronze, Silver, and Gold layers. Trino is responsible for exposing the resulting analytical datasets through SQL, while Apache Superset consumes this query layer to provide dashboards and visualizations.

This separation of responsibilities results in a modular, maintainable, and scalable architecture while keeping the platform deployable locally using Docker Compose.

The selected technologies are widely adopted in modern Data Engineering environments and provide a solid foundation for future extensions without requiring significant architectural changes.