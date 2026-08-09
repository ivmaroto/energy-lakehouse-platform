# Airflow Design

## 1. Overview

Apache Airflow is the workflow orchestration platform responsible for automating and coordinating the execution of the data pipelines within the Lakehouse.

Rather than performing data processing itself, Airflow manages the execution order of the different processing stages, ensuring that tasks are executed only after their dependencies have been successfully completed.

The orchestration layer supports both the initial historical data ingestion and subsequent incremental updates, providing a reliable and reproducible mechanism for executing the complete data lifecycle.

By separating orchestration from data processing, the platform maintains a modular architecture where Python handles data ingestion, Apache Spark and Spark SQL perform data transformations, and Airflow coordinates workflow execution.

Once analytical datasets have been generated in the Gold layer, Trino provides SQL access to them and Apache Superset consumes those datasets for visualization.

## 2. Workflow Orchestration

The platform adopts a workflow-based orchestration model, where the complete data lifecycle is divided into a sequence of independent but connected processing stages.

Each workflow is responsible for a specific objective and is executed only when all required upstream tasks have been successfully completed. This dependency-based execution model ensures data consistency while preventing incomplete or inconsistent datasets from propagating through the Lakehouse.

The orchestration process coordinates the execution of:

- Historical data ingestion.
- Incremental data ingestion.
- Storage of source data in the Bronze layer.
- Data processing across the Bronze, Silver, and Gold layers.
- Data quality validation.
- Generation and publication of analytical datasets.
- Verification that Gold datasets are available for analytical consumption.

This modular workflow design simplifies maintenance, improves fault isolation, and allows individual processes to evolve independently without affecting the overall architecture.

## 3. Pipeline Structure

The data pipeline is organized as a sequence of logical stages that reflect the complete lifecycle of the data within the Lakehouse.

Each stage has a clearly defined responsibility, ensuring a modular and maintainable processing architecture.

The pipeline is structured as follows:

1. Data ingestion from public APIs using Python connectors.
2. Storage of raw datasets in the Bronze layer.
3. Data validation, cleansing, and standardization using Apache Spark and Spark SQL.
4. Publication of validated datasets in the Silver layer.
5. Data integration, aggregation, and analytical model generation.
6. Publication of business-ready datasets in the Gold layer.
7. Validation of Gold datasets.
8. Availability of Gold datasets for analytical querying through Trino.
9. Consumption of analytical datasets by Apache Superset.

The general workflow can be represented as:

```text
Public APIs
    │
    ▼
Data Ingestion
    │
    ▼
  Bronze
    │
    ▼
Validation
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
Quality Checks
    │
    ▼
Available through Trino
    │
    ▼
Apache Superset
```

Apache Airflow coordinates the ingestion, processing, validation, and publication stages. Trino and Apache Superset form the downstream analytical consumption path once the Gold datasets are available.

## 4. DAG Strategy

Airflow workflows are implemented using Directed Acyclic Graphs (DAGs).

DAGs define the dependencies between the different tasks required to execute the data pipelines. This provides a clear representation of the workflow and allows Airflow to control execution order, retries, scheduling, and failure management.

The orchestration design allows workflows to be separated according to their responsibility.

The platform is designed to support DAGs for:

- Historical ingestion.
- Incremental ingestion.
- Bronze-to-Silver processing.
- Silver-to-Gold processing.
- Data quality validation.
- End-to-end pipeline execution.

This separation prevents the complete platform from becoming dependent on a single monolithic workflow and allows individual processes to be executed or maintained independently.

The exact DAG implementation can evolve during the ingestion and processing phases while preserving this orchestration model.

## 5. Historical Ingestion Workflow

The historical ingestion workflow is responsible for the initial population of the Lakehouse.

Its purpose is to retrieve the historical information available from the selected public APIs and populate the Bronze layer before executing the downstream transformation processes.

A conceptual historical workflow is:

```text
Start
  │
  ├──► AEMET Historical Ingestion
  │
  ├──► Open-Meteo Historical Ingestion
  │
  └──► REE/ESIOS Historical Ingestion
             │
             ▼
      Bronze Validation
             │
             ▼
      Bronze → Silver
             │
             ▼
       Silver → Gold
             │
             ▼
        Gold Validation
             │
             ▼
            End
```

The historical workflow is primarily required during the initial population of the platform but can also be executed when a complete reload or reprocessing operation is required.

## 6. Incremental Ingestion Workflow

After the initial historical load, Airflow coordinates periodic incremental ingestion workflows.

Incremental ingestion retrieves newly available data from each public source and processes only the information required to update the Lakehouse.

A conceptual incremental workflow is:

```text
Start
  │
  ▼
Determine Incremental Window
  │
  ▼
Retrieve New API Data
  │
  ▼
Store in Bronze
  │
  ▼
Validate Data
  │
  ▼
Bronze → Silver
  │
  ▼
Silver → Gold
  │
  ▼
Validate Gold
  │
  ▼
End
```

The exact incremental strategy may vary between AEMET, Open-Meteo, and REE/ESIOS according to the publication frequency and capabilities of each API.

## 7. Scheduling Strategy

The platform supports two complementary scheduling strategies: an initial historical data load and periodic incremental updates.

The historical ingestion workflow is executed during the initial population of the platform or whenever a complete data reload is required.

After the historical load, incremental workflows can be scheduled periodically to ingest newly available meteorological and energy data.

The initial design targets hourly orchestration where appropriate, although the final scheduling frequency for each connector will be aligned with the actual publication frequency and limitations of its source API.

Apache Airflow manages execution schedules independently from processing logic, allowing the frequency of individual workflows to be adjusted without modifying the underlying ingestion or transformation components.

This approach avoids unnecessary API requests and processing when a particular source publishes information at a lower frequency.

## 8. Task Dependencies

Airflow task dependencies ensure that downstream processing is executed only when the required upstream datasets are available and valid.

A simplified dependency chain is:

```text
Ingestion
    │
    ▼
Bronze Storage
    │
    ▼
Bronze Validation
    │
    ▼
Bronze → Silver
    │
    ▼
Silver Validation
    │
    ▼
Silver → Gold
    │
    ▼
Gold Validation
    │
    ▼
Analytical Dataset Available
```

If a critical upstream task fails, downstream tasks are not executed until the failure has been resolved or the corresponding retry succeeds.

This protects the Silver and Gold layers from incomplete or invalid upstream data.

## 9. Error Handling

The orchestration layer includes error handling mechanisms to ensure that pipeline failures are detected, isolated, and managed without compromising previously processed data.

Apache Airflow controls task execution status and prevents downstream tasks from running when an upstream dependency has failed.

Failed tasks can be retried automatically according to configurable retry policies.

The error handling strategy includes:

- Automatic retries for temporary failures.
- Clear task failure states.
- Prevention of downstream execution after critical errors.
- Preservation of successfully processed data.
- Isolation of failures within the affected workflow stage.
- Recording of error details in execution logs.
- Source-specific handling where required.

Retry limits, retry delays, timeouts, and source-specific failure policies will be configured during pipeline implementation according to the characteristics of each connector and processing task.

## 10. Monitoring and Logging

Monitoring and logging are essential to ensure the reliability and traceability of the orchestration layer.

Apache Airflow provides built-in monitoring capabilities that allow workflow executions to be tracked through its web interface.

Each workflow execution records task status, execution time, dependencies, and error information, enabling rapid identification and diagnosis of issues.

The monitoring strategy includes:

- Workflow execution monitoring.
- Task execution status tracking.
- Execution time measurement.
- Centralized execution logs.
- Error reporting.
- Historical execution records.
- Retry monitoring.

These capabilities support operational visibility, facilitate troubleshooting, and provide execution traceability throughout the complete data pipeline.

Airflow logs are generated as runtime artifacts and are excluded from Git version control.

## 11. Relationship with Spark

Airflow and Apache Spark have separate responsibilities within the platform.

Apache Airflow is responsible for:

- Scheduling workflows.
- Managing task dependencies.
- Controlling execution order.
- Handling retries and failures.
- Monitoring pipeline execution.

Apache Spark and Spark SQL are responsible for:

- Data validation.
- Data cleansing.
- Data standardization.
- Data integration.
- Bronze-to-Silver transformations.
- Silver-to-Gold transformations.
- Writing processed Lakehouse datasets.

This separation ensures that Airflow remains an orchestration platform rather than becoming responsible for implementing data processing logic.

## 12. Relationship with Trino and Superset

Trino and Apache Superset are downstream consumers of the datasets generated by the orchestrated data pipelines.

Airflow does not replace or directly perform analytical querying.

Once a workflow has successfully generated and validated the required Gold datasets, those datasets become available through Trino.

The analytical path is:

```text
Airflow-orchestrated pipeline
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

Trino provides the SQL query layer, while Apache Superset provides visualization and interactive analytical capabilities.

This architecture maintains a clear separation between orchestration, processing, querying, and visualization.

## 13. Design Principles

The orchestration layer has been designed according to the following principles:

- **Automation**

  Data pipelines are designed to execute automatically according to their configured schedules.

- **Reliability**

  Workflow dependencies ensure that each processing stage is executed only after the successful completion of its required upstream tasks.

- **Fault Tolerance**

  Temporary failures can be managed through retry mechanisms while preventing inconsistent data from propagating through the platform.

- **Modularity**

  Each workflow performs a specific function and can be maintained or extended independently.

- **Scalability**

  New workflows and data sources can be incorporated without redesigning the existing orchestration architecture.

- **Traceability**

  Workflow execution is monitored through execution logs, task status, and historical records.

- **Maintainability**

  Orchestration logic remains independent from data processing logic, simplifying future maintenance and evolution of the platform.

- **Separation of responsibilities**

  Airflow orchestrates workflows, Spark processes data, Trino provides analytical SQL access, and Superset provides visualization.

- **Reprocessability**

  Historical and transformation workflows can be executed again when data needs to be rebuilt or processing logic changes.

- **Source-aware scheduling**

  Incremental scheduling can be adapted to the publication frequency and limitations of each external API.