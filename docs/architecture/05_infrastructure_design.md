# Infrastructure Design

## 1. Infrastructure Overview

The platform is deployed as a fully containerized environment using Docker Compose.

Each infrastructure component runs as an independent container with a dedicated responsibility, allowing the platform to remain modular, reproducible, and easy to maintain.

The infrastructure has been designed to support the complete data lifecycle, including data ingestion, distributed processing, metadata management, workflow orchestration, object storage, analytical querying, and data visualization.

All services communicate through a shared Docker network while maintaining persistent storage for data and metadata. This architecture enables the entire platform to be recreated consistently on any compatible machine without requiring manual installation of individual components.

## 2. Platform Components

The infrastructure is composed of a set of specialized services, each responsible for a specific function within the platform.

The main infrastructure components are:

- **Apache Spark**
  
  Executes distributed data processing, transformations, and analytical queries.

- **MinIO**
  
  Provides S3-compatible object storage for the Lakehouse datasets.

- **Apache Iceberg**
  
  Manages the analytical table format used by the Lakehouse.

- **PostgreSQL**
  
  Stores platform metadata and the Apache Iceberg catalog.

- **Apache Airflow**
  
  Orchestrates and schedules the execution of the complete data pipeline.

- **Apache Superset**
  
  Provides web-based dashboards and analytical visualizations.

- **Docker Compose**
  
  Deploys and manages all infrastructure services as a single reproducible environment.

## 3. Network Architecture

All platform services communicate through a shared Docker network.

The shared network allows containers to resolve and access each other using their service names, avoiding dependencies on host-specific IP addresses and simplifying service configuration.

Only the services that require direct user access expose ports to the host machine. Internal services remain accessible exclusively within the Docker network whenever possible.

This approach improves portability, reduces configuration complexity, and limits unnecessary external exposure of infrastructure components.

## 4. Persistent Storage

Persistent Docker volumes are used to preserve platform data and metadata independently of the lifecycle of individual containers.

This ensures that restarting, recreating, or updating a service does not cause the loss of Lakehouse data, workflow metadata, configuration, or analytical content.

Persistent storage is required for the following components:

- **MinIO**
  
  Stores Apache Iceberg data files and table metadata.

- **PostgreSQL**
  
  Stores metadata used by Apache Airflow and the Apache Iceberg catalog.

- **Apache Airflow**
  
  Preserves workflow logs and execution-related files when required.

- **Apache Superset**
  
  Preserves application metadata, dashboard definitions, charts, and configuration data.

The exact volume names, mount paths, and storage structure will be defined during the infrastructure implementation phase.

## 5. Service Communication

Platform services communicate through well-defined interfaces, with each component interacting only with the services required to perform its responsibilities.

This approach reduces coupling between components, improves maintainability, and simplifies future extensions of the platform.

The main communication paths are:

- Python connectors retrieve data from the public APIs.
- Apache Spark reads from and writes to the Lakehouse stored in MinIO.
- Apache Spark accesses the Apache Iceberg catalog through PostgreSQL.
- Apache Airflow triggers and monitors Spark processing workflows.
- Spark SQL provides analytical access to the Lakehouse datasets.
- Apache Superset connects to Spark SQL to query analytical data and build dashboards.

This communication model establishes a clear separation between data ingestion, processing, storage, orchestration, and analytical consumption while maintaining a modular architecture.

## 6. Deployment Strategy

The platform is designed to be deployed as a self-contained local environment using Docker Compose.

All infrastructure services are started from a single configuration, ensuring a consistent deployment process across different development environments. This approach minimizes manual configuration and simplifies installation, maintenance, and reproducibility.

The deployment strategy follows these principles:

- Infrastructure as Code through Docker Compose.
- Container isolation for all platform services.
- Persistent storage using Docker volumes.
- Shared networking for inter-service communication.
- Environment-based configuration.
- Modular service deployment.

This deployment model provides a lightweight and reproducible environment suitable for development, testing, and demonstration purposes while remaining extensible for future production-oriented deployments.