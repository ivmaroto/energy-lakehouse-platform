# Airflow Design

## 1. Overview

Apache Airflow is the workflow orchestration platform responsible for automating and coordinating the execution of the data pipelines within the Lakehouse.

Rather than performing data processing itself, Airflow manages the execution order of the different processing stages, ensuring that tasks are executed only after their dependencies have been successfully completed.

The orchestration layer supports both the initial historical data ingestion and the subsequent incremental updates, providing a reliable and reproducible mechanism for executing the complete data lifecycle.

By separating orchestration from data processing, the platform maintains a modular architecture where Apache Spark focuses on data transformation while Airflow manages workflow execution.

## 2. Workflow Orchestration

The platform adopts a workflow-based orchestration model, where the complete data lifecycle is divided into a sequence of independent but connected processing stages.

Each workflow is responsible for a specific objective and is executed only when all required upstream tasks have been successfully completed. This dependency-based execution model ensures data consistency while preventing incomplete or inconsistent datasets from propagating through the Lakehouse.

The orchestration process coordinates the execution of:

- Historical data ingestion.
- Incremental data ingestion.
- Data processing across the Bronze, Silver, and Gold layers.
- Data quality validation.
- Publication of analytical datasets.

This modular workflow design simplifies maintenance, improves fault isolation, and allows individual processes to evolve independently without affecting the overall architecture.

## 3. Pipeline Structure

The data pipeline is organized as a sequence of logical stages that reflect the complete lifecycle of the data within the Lakehouse.

Each stage has a clearly defined responsibility and exchanges data only with the adjacent stage, ensuring a modular and maintainable processing architecture.

The pipeline is structured as follows:

1. Data ingestion from public APIs.
2. Storage of raw datasets in the Bronze layer.
3. Data validation, cleansing, and standardization in the Silver layer.
4. Data integration and analytical model generation in the Gold layer.
5. Analytical querying through Spark SQL.
6. Data visualization in Apache Superset.

This staged approach simplifies pipeline management, facilitates debugging, and preserves full data lineage throughout the platform.

## 4. Scheduling Strategy

The platform supports two complementary scheduling strategies: an initial historical data load and periodic incremental updates.

The historical ingestion workflow is executed during the initial deployment of the platform or whenever a complete data reload is required.

After the historical load, incremental workflows are scheduled on an hourly basis to ingest newly available meteorological and energy data. This frequency is aligned with the expected temporal resolution of the datasets used by the platform and enables the Lakehouse to remain continuously updated.

Apache Airflow manages the execution schedule independently from the processing logic, allowing the frequency of individual workflows to be adjusted if the publication characteristics of a specific API require a different configuration.

## 5. Error Handling

The orchestration layer includes error handling mechanisms to ensure that pipeline failures are detected, isolated, and managed without compromising previously processed data.

Apache Airflow controls task execution status and prevents downstream tasks from running when an upstream dependency has failed. Failed tasks can be retried automatically according to configurable retry policies.

The error handling strategy includes:

- Automatic retries for temporary failures.
- Clear task failure states.
- Prevention of downstream execution after critical errors.
- Preservation of successfully processed data.
- Isolation of failures within the affected workflow stage.
- Recording of error details in execution logs.

Retry limits, retry delays, timeouts, and source-specific failure policies will be configured during pipeline implementation.

## 6. Monitoring and Logging

Monitoring and logging are essential to ensure the reliability and traceability of the orchestration layer.

Apache Airflow provides built-in monitoring capabilities that allow workflow executions to be tracked in real time. Each workflow execution records task status, execution time, dependencies, and error information, enabling rapid identification and diagnosis of issues.

The monitoring strategy includes:

- Workflow execution monitoring.
- Task execution status tracking.
- Execution time measurement.
- Centralized execution logs.
- Error reporting.
- Historical execution records.

These capabilities support operational visibility, facilitate troubleshooting, and provide execution traceability throughout the complete data pipeline.

## 7. Design Principles

The orchestration layer has been designed according to the following principles:

- **Automation**
  
  All data pipelines are executed automatically without manual intervention.

- **Reliability**
  
  Workflow dependencies ensure that each processing stage is executed only after the successful completion of the previous stage.

- **Fault Tolerance**
  
  Temporary failures are managed through retry mechanisms while preventing inconsistent data from propagating through the platform.

- **Modularity**
  
  Each workflow performs a specific function and can be maintained or extended independently.

- **Scalability**
  
  New workflows and data sources can be incorporated without modifying the existing orchestration architecture.

- **Traceability**
  
  Workflow execution is fully monitored through execution logs, task status, and historical records.

- **Maintainability**
  
  The orchestration logic remains independent from the data processing logic, simplifying future maintenance and evolution of the platform.