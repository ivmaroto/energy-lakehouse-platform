# Infrastructure Design

## 1. Infrastructure Overview

The platform is deployed as a fully containerized local environment using Docker Compose.

Each infrastructure component runs as an independent container with a dedicated responsibility, allowing the platform to remain modular, reproducible, and maintainable.

The infrastructure supports the complete data lifecycle, including data ingestion, distributed processing, metadata management, workflow orchestration, object storage, analytical querying, and data visualization.

All services communicate through a shared Docker network while persistent Docker volumes are used where required to preserve data and metadata independently of the container lifecycle.

The infrastructure has been implemented and validated locally using Docker Compose, allowing the complete platform to be started, stopped, and recreated from a single declarative configuration.

## 2. Platform Components

The infrastructure is composed of specialized services, each responsible for a specific function within the platform.

The main infrastructure components are:

- **Apache Spark Master**

  Coordinates the distributed Spark processing cluster and manages the execution of processing workloads.

- **Apache Spark Worker**

  Provides processing resources to the Spark cluster and executes distributed data transformation tasks.

- **MinIO**

  Provides S3-compatible object storage for the Lakehouse datasets.

- **Apache Iceberg**

  Provides the analytical table format used by the Lakehouse and enables Spark and Trino to operate over the same datasets.

- **PostgreSQL**

  Provides relational metadata storage for platform services and supports the Apache Iceberg JDBC catalog.

- **Trino**

  Provides the distributed SQL query layer used to access Apache Iceberg analytical datasets.

- **Apache Airflow**

  Orchestrates and schedules the execution of ingestion and processing pipelines.

- **Apache Superset**

  Provides web-based dashboards, analytical exploration, and data visualization.

- **Docker Compose**

  Defines, builds, deploys, and manages the complete local infrastructure as a reproducible environment.

## 3. Container Architecture

The local platform is composed of multiple Docker services coordinated through Docker Compose.

The deployed infrastructure includes:

```text
Docker Compose
│
├── PostgreSQL
│
├── MinIO
│
├── Spark Master
│
├── Spark Worker
│
├── Trino
│
├── Airflow Init
│
├── Airflow Webserver
│
├── Airflow Scheduler
│
├── Superset Init
│
└── Superset
```

The initialization containers have a different lifecycle from the long-running services.

`airflow-init` and `superset-init` execute the initialization procedures required by their respective applications and terminate successfully once those operations are completed.

Therefore, the expected final state for these containers is:

```text
Exited (0)
```

The remaining infrastructure services continue running while the platform is active.

## 4. Network Architecture

All platform services communicate through a shared Docker network.

The network used by the platform is:

```text
lakehouse-network
```

The shared network allows containers to resolve and access each other using Docker service names instead of host-specific IP addresses.

Examples of internal service communication include:

```text
postgres:5432
minio:9000
spark-master:7077
```

Only services requiring access from the host expose their corresponding ports.

This approach improves portability, reduces configuration complexity, and prevents dependencies on dynamically assigned container IP addresses.

## 5. Service Ports

The local infrastructure exposes the following main ports:

| Service | Port | Purpose |
|---|---:|---|
| PostgreSQL | 5432 | Relational metadata storage |
| Spark Master | 7077 | Spark cluster communication |
| Spark Master UI | 8080 | Spark Master monitoring interface |
| Spark Worker UI | 8081 | Spark Worker monitoring interface |
| Trino | 8082 | Distributed SQL query engine |
| Airflow | 8083 | Airflow web interface |
| Superset | 8088 | Superset web interface |
| MinIO API | 9000 | S3-compatible storage API |
| MinIO Console | 9001 | MinIO administration interface |

Port mappings provide access to the required administration and visualization interfaces while service-to-service communication is performed through the internal Docker network.

## 6. Persistent Storage

Persistent Docker volumes are used to preserve platform data and metadata independently of the lifecycle of individual containers.

The infrastructure defines persistent storage for the components that require durable state.

### MinIO

MinIO uses persistent storage for the Lakehouse object data.

This storage contains the data files and metadata associated with the Apache Iceberg tables.

### PostgreSQL

PostgreSQL uses a persistent Docker volume for relational metadata.

This allows database information to survive container recreation and normal platform shutdowns.

### Trino

Trino has persistent storage available for local runtime data required by its configuration.

### Airflow

Airflow logs and configuration resources are mounted from the project structure where required.

Airflow application metadata is stored in PostgreSQL.

### Superset

Superset application metadata is persisted through its database configuration, while its Docker configuration is maintained as part of the version-controlled project infrastructure.

Persistent storage allows the platform to be stopped using:

```bash
docker compose down
```

without deleting the stored data.

A complete reset can be performed using:

```bash
docker compose down -v
```

which removes the persistent Docker volumes and should therefore only be used when a complete environment reset is required.

## 7. Custom Docker Images

Some platform components require custom Docker images to provide additional dependencies and project-specific configuration.

Custom images are used for:

- Apache Spark.
- Apache Airflow.
- Apache Superset.

### Apache Spark

A custom Spark image is used to provide the dependencies required to interact with the Lakehouse storage and Apache Iceberg environment.

The Spark deployment consists of a Master node and a Worker node.

### Apache Airflow

A custom Airflow image provides the Python dependencies required by the orchestration layer and future data pipelines.

### Apache Superset

A custom Superset image provides the dependencies and configuration required by the Business Intelligence layer.

These images are built through Docker Compose as part of the local infrastructure deployment.

## 8. Service Communication

Platform services communicate through well-defined interfaces, with each component interacting only with the services required to perform its responsibilities.

The main communication paths are:

- Python connectors retrieve data from AEMET, Open-Meteo, and REE/ESIOS.
- Apache Spark reads and writes Lakehouse datasets stored through MinIO.
- Apache Spark works with Apache Iceberg tables during data processing.
- PostgreSQL provides persistent metadata storage and supports the Iceberg JDBC catalog.
- Apache Airflow schedules and orchestrates ingestion and Spark processing workflows.
- Trino queries Apache Iceberg datasets stored in the Lakehouse.
- Apache Superset connects to Trino for analytical SQL access and dashboard generation.

The main analytical path is:

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
     │
     ▼
Apache Superset
```

This communication model establishes a clear separation between data ingestion, processing, storage, orchestration, analytical querying, and visualization.

## 9. Processing and Analytical Query Separation

The infrastructure deliberately separates distributed data processing from interactive analytical querying.

Apache Spark and Spark SQL are responsible for processing operations such as:

- Data validation.
- Data cleansing.
- Dataset standardization.
- Data integration.
- Bronze-to-Silver transformations.
- Silver-to-Gold transformations.

Trino is responsible for:

- Distributed SQL access to the Lakehouse.
- Interactive analytical queries.
- Exposing curated datasets to Apache Superset.

This design prevents the visualization layer from depending directly on the Spark processing cluster and provides a dedicated SQL access layer for analytical workloads.

## 10. Environment Configuration

Environment-specific values and credentials are managed through environment variables.

The repository provides:

```text
.env.example
```

as a template containing the variables required by the local infrastructure.

The actual local configuration is stored in:

```text
.env
```

The `.env` file is excluded from version control to prevent local credentials and secrets from being committed to the repository.

Environment variables include configuration for:

- PostgreSQL.
- MinIO.
- Apache Airflow.
- Apache Superset.

This approach separates infrastructure configuration from the Docker Compose definition and improves portability between environments.

## 11. Deployment Strategy

The platform is deployed as a self-contained local environment using Docker Compose.

Docker Compose provides Infrastructure as Code through the version-controlled:

```text
docker-compose.yml
```

The deployment strategy follows these principles:

- Infrastructure as Code through Docker Compose.
- Container isolation for platform services.
- Persistent storage using Docker volumes.
- Shared networking for inter-service communication.
- Environment-based configuration.
- Custom Docker images where required.
- Modular service deployment.
- Reproducible local infrastructure.

The Docker Compose configuration can be validated using:

```bash
docker compose config
```

Custom images can be built using:

```bash
docker compose build
```

The complete platform can be started using:

```bash
docker compose up -d
```

The infrastructure status can be inspected using:

```bash
docker compose ps -a
```

The platform can be stopped while preserving persistent data using:

```bash
docker compose down
```

## 12. Deployment Validation

During the infrastructure implementation phase, the platform services were successfully deployed locally using Docker Compose.

The following components were started and manually validated:

- PostgreSQL.
- MinIO.
- Apache Spark Master.
- Apache Spark Worker.
- Trino.
- Apache Airflow Webserver.
- Apache Airflow Scheduler.
- Apache Superset.

The Airflow and Superset web interfaces were successfully accessed from the host environment.

The platform was also stopped and restarted through Docker Compose, confirming that the main services can be managed from the centralized infrastructure definition.

Initialization services for Airflow and Superset completed successfully with exit code `0`.

A final clean-environment reproducibility test after deleting the Docker volumes remains pending. This validation will confirm that the complete platform can initialize automatically from an empty local Docker environment.

## 13. Infrastructure Design Principles

The infrastructure has been designed according to the following principles:

- **Reproducibility**

  The complete platform can be deployed from version-controlled infrastructure definitions.

- **Modularity**

  Each infrastructure component has a clearly defined responsibility.

- **Persistence**

  Platform data and metadata survive normal container recreation and shutdown.

- **Isolation**

  Services execute in independent containers.

- **Portability**

  Docker Compose reduces dependencies on host-specific software installations.

- **Separation of responsibilities**

  Processing, storage, metadata management, orchestration, querying, and visualization are handled by dedicated components.

- **Open Source first**

  The complete infrastructure is based on Open Source technologies.

- **Local independence**

  The platform can operate without depending on proprietary cloud infrastructure.

This deployment model provides a reproducible environment suitable for development, testing, demonstration, and the implementation of the complete Energy Lakehouse Platform.