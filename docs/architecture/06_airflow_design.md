# Airflow Design

## 1. Overview

Apache Airflow is the workflow-orchestration component of the Energy Lakehouse
Platform.

Airflow coordinates the execution of ingestion and Lakehouse-processing tasks
but does not implement the underlying data-transformation logic.

The separation of responsibilities is:

```text
Python
→ source ingestion

Apache Spark / PySpark
→ Bronze-to-Silver processing
→ Silver-to-Gold processing

Apache Airflow
→ execution coordination
→ dependencies
→ scheduling
→ retries
→ execution monitoring

Trino
→ analytical SQL access

Apache Superset
→ visualization
```

The orchestration layer is designed to support both historical executions and
subsequent recurrent or incremental executions.

The core data-processing path itself has already been validated independently
from Airflow:

```text
External sources
      │
      ▼
Bronze / MinIO
      │
      ▼
Silver / Spark / Iceberg
      │
      ▼
Gold / Spark / Iceberg
      │
      ▼
Trino
```

Final runtime validation of this complete chain when launched and coordinated
directly by Airflow remains part of the orchestration closure.

---

## 2. Role of Airflow

Airflow is responsible for coordinating the data pipeline.

Its responsibilities include:

- executing ingestion workflows;
- controlling task dependencies;
- coordinating Bronze, Silver and Gold stages;
- scheduling recurring processes;
- providing retry mechanisms;
- recording task execution states;
- exposing execution logs;
- providing operational visibility through the Airflow interface.

Airflow is not responsible for:

- implementing API connector logic;
- transforming Bronze data;
- performing geographical normalization;
- calculating Gold metrics;
- executing interactive analytical queries.

Those responsibilities remain inside their corresponding platform components.

---

## 3. Orchestration Architecture

The intended orchestration architecture is:

```text
                    Apache Airflow
                          │
          ┌───────────────┼────────────────┐
          │               │                │
          ▼               ▼                ▼
   Python Ingestion   Spark Silver     Spark Gold
          │               │                │
          ▼               ▼                ▼
       Bronze          Silver           Gold
       MinIO           Iceberg          Iceberg
                                           │
                                           ▼
                                         Trino
                                           │
                                           ▼
                                        Superset
```

Airflow coordinates execution while each processing component remains
independently executable and testable.

This separation allows ingestion and Spark jobs to be validated outside Airflow
before they are incorporated into an automated workflow.

---

## 4. Airflow Infrastructure

The local Docker Compose environment includes:

```text
airflow-init
airflow-webserver
airflow-scheduler
```

The initialization service prepares the Airflow environment and terminates once
initialization has completed.

The long-running services are:

```text
airflow-webserver
airflow-scheduler
```

Airflow application metadata is persisted in PostgreSQL.

DAG files are mounted from:

```text
airflow/dags/
```

Execution logs are maintained as runtime artifacts and are not intended for
source-control persistence.

---

## 5. Validated Airflow Infrastructure

The following infrastructure elements have been validated:

```text
Airflow Webserver
Airflow Scheduler
PostgreSQL metadata connectivity
DAG discovery
Airflow web interface
```

Existing source-ingestion DAGs were previously discovered and executed during
ingestion validation.

This proves that Airflow can execute project Python ingestion code inside the
containerized environment.

It does not by itself prove that the final complete:

```text
Bronze
→ Silver
→ Gold
```

workflow has been executed end to end from Airflow.

That distinction is preserved explicitly in this document.

---

## 6. Existing Ingestion Workflows

The project originally implemented source- and frequency-oriented DAGs during
the ingestion phase.

These DAGs demonstrated that:

- Airflow can discover project workflows;
- Airflow can execute ingestion code;
- ingestion tasks can communicate with external APIs;
- ingestion tasks can persist Bronze data in MinIO;
- retries and task states can be managed through Airflow.

Some of those early workflows correspond to ingestion experiments or dataset
families that are no longer part of the final physical analytical scope.

The final current data scope is defined by the active Bronze, Silver and Gold
models rather than by retaining every earlier experimental ingestion path.

The active analytical source scope is:

```text
AEMET
  stations
  current_observations

Open-Meteo
  weather_hourly
  weather_15min

REE / ESIOS
  11 hourly generation indicators
  9 monthly installed-capacity indicators

CNIG / IGN
  provinces
  municipalities
```

---

## 7. Historical Reload Workflow

The project includes an end-to-end historical orchestration DAG:

```text
airflow/dags/historical_reload.py
```

Its objective is to coordinate a historical execution through the complete
Lakehouse processing chain.

The logical workflow is:

```text
Start
  │
  ├──► Master/reference ingestion
  │
  ├──► Historical Open-Meteo ingestion
  │
  ├──► Historical ESIOS ingestion
  │
  └──► AEMET current acquisition
             │
             ▼
         Bronze ready
             │
             ▼
      Create Silver tables
             │
             ▼
        Write Silver
             │
             ▼
       Create Gold tables
             │
             ▼
         Write Gold
             │
             ▼
            End
```

The workflow reuses the same Python ingestion and Spark-processing
implementations that have already been validated independently.

It does not duplicate transformation logic inside the DAG.

---

## 8. Historical Parameters

The historical orchestration workflow supports an explicit requested temporal
interval.

The principal temporal parameters are:

```text
start_date
end_date
```

These parameters determine the requested historical interval for source
connectors that support historical acquisition.

Different sources retain their own acquisition semantics.

For example:

```text
Open-Meteo
→ historical meteorological interval

ESIOS
→ historical energy interval

AEMET stations
→ master/reference acquisition

AEMET current observations
→ recent/current acquisition
```

AEMET current observations are therefore not reinterpreted as arbitrary
historical observations.

---

## 9. Source-Specific Historical Behaviour

### Open-Meteo

Historical meteorological acquisition operates over the AEMET station
catalogue.

The current validated catalogue contains:

```text
926 locations
```

The source strategy is:

```text
Hourly historical data
→ Open-Meteo Archive API

15-minute historical data
→ Open-Meteo Historical Forecast API
```

Large station batches include source-specific retry, backoff, pacing and
coverage-validation mechanisms.

---

### REE / ESIOS

The active historical ESIOS scope contains:

```text
11 hourly generation indicators
9 monthly installed-capacity indicators
```

Indicator configuration is externalized in:

```text
config/esios_indicators.json
```

ESIOS responses are technically validated before successful Bronze persistence.

In particular, an indicator response with:

```text
values = []
```

is not considered a valid completed source acquisition by the current ingestion
implementation.

---

### CNIG / IGN

CNIG provides the territorial master required by Silver geographical
normalization.

The master data is independent from the analytical historical date interval.

---

### AEMET

AEMET provides:

```text
stations
current_observations
```

The station catalogue is used as a meteorological location master.

Current observations remain a recent/current data source.

---

## 10. Silver Orchestration

Airflow does not contain the Bronze-to-Silver transformation logic.

The actual Spark processing is maintained under:

```text
spark/jobs/silver/
```

The principal entry points are:

```text
spark/jobs/silver/create_tables.py
spark/jobs/silver/write_silver.py
```

The resulting physical Silver model contains exactly:

```text
9 Apache Iceberg tables
```

These are:

```text
silver_aemet_stations
silver_aemet_current_observations

silver_open_meteo_hourly
silver_open_meteo_15min

silver_cnig_provinces
silver_cnig_autonomous_communities
silver_cnig_municipalities

silver_esios_energy_hourly
silver_esios_installed_capacity_monthly
```

Airflow's responsibility is only to invoke the appropriate processing stages
after the required Bronze ingestion tasks have completed successfully.

---

## 11. Gold Orchestration

Gold processing is also implemented outside Airflow.

The principal Spark entry points are:

```text
spark/jobs/gold/create_tables.py
spark/jobs/gold/write_gold.py
```

The current Gold physical model contains exactly:

```text
4 Apache Iceberg tables
```

These are:

```text
gold_fact_province_hourly
gold_fact_installed_capacity_monthly
gold_dim_geography
gold_dim_time
```

The main analytical fact operates at:

```text
Province × hour
```

and integrates meteorological and hourly electricity-generation data.

The monthly installed-capacity fact operates at:

```text
Autonomous Community × month
```

Airflow coordinates execution but does not replicate this analytical logic.

---

## 12. Task Dependencies

The final orchestration must preserve the following dependency relationship:

```text
Required Bronze ingestion
          │
          ▼
   Bronze available
          │
          ▼
Create Silver tables
          │
          ▼
     Write Silver
          │
          ▼
Create Gold tables
          │
          ▼
      Write Gold
          │
          ▼
 Gold available
```

A downstream processing stage must not execute successfully before its required
upstream stage has completed.

This dependency strategy prevents an incomplete upstream load from being
silently promoted through the Lakehouse.

---

## 13. Failure Behaviour

Airflow provides task-level execution states and retry mechanisms.

The orchestration strategy follows a fail-closed approach for critical
processing stages.

Conceptually:

```text
Ingestion failure
       │
       └──► dependent Silver processing does not continue successfully
```

```text
Silver failure
       │
       └──► Gold processing does not continue successfully
```

```text
Gold failure
       │
       └──► workflow is not considered successfully completed
```

Source-specific retry behaviour can also exist below Airflow.

For example, Open-Meteo batch ingestion implements connector-level retries,
backoff and resumable location processing.

Airflow therefore complements rather than replaces source-specific reliability
logic.

---

## 14. Retry Strategy

Retries can occur at different architectural levels.

### HTTP / connector level

Used for temporary external-source failures such as:

```text
timeouts
temporary HTTP errors
rate limitations
```

### Source-specific batch level

Open-Meteo includes:

```text
retry
backoff
pacing
coverage validation
resume support
```

### Airflow task level

Airflow can retry a failed orchestration task according to its configured task
policy.

This layered strategy prevents all transient failures from being handled by a
single component.

---

## 15. Monitoring and Logging

Apache Airflow provides operational visibility over workflow executions.

Available information includes:

- DAG execution status;
- task execution status;
- dependency state;
- execution timestamps;
- retry attempts;
- task logs;
- failure information;
- historical DAG runs.

The Airflow web interface is therefore the operational control point for
orchestrated executions.

Application-specific ingestion and Spark logs remain generated by their
respective components and are visible through the execution environment.

---

## 16. Incremental Orchestration

The architecture is designed to support subsequent incremental or recent-data
executions.

The general intended model is:

```text
Requested execution window
          │
          ▼
      Source ingestion
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

The exact source window cannot be assumed to behave identically for all
providers.

External sources may differ in:

- publication latency;
- available latest timestamp;
- temporal granularity;
- historical availability.

Therefore, the orchestration layer must respect the real data availability of
each source rather than fabricate observations up to the requested end time.

---

## 17. Checkpoint Status

A persistent business-level checkpoint system containing fields such as:

```text
dataset_name
last_successful_timestamp
status
updated_at
```

was considered during orchestration design.

However, such a persistent checkpoint table is **not part of the currently
validated implementation**.

Current Bronze metadata already records information such as:

```text
ingestion_timestamp
requested_start_date
requested_end_date
ingestion_mode
```

Airflow itself also records DAG and task execution metadata.

These mechanisms must not be described as equivalent to a persistent
dataset-level business checkpoint.

A dedicated checkpoint subsystem can be considered as future operational
enhancement if required.

---

## 18. Processing and Orchestration Separation

The project deliberately avoids embedding transformation code inside DAG
definitions.

The separation is:

```text
airflow/dags/
→ workflow definitions
→ dependencies
→ parameters
→ scheduling

ingestion/
→ API acquisition
→ Bronze persistence

spark/jobs/silver/
→ Bronze-to-Silver logic

spark/jobs/gold/
→ Silver-to-Gold logic
```

This improves:

- testability;
- maintainability;
- reuse;
- separation of concerns;
- independent execution of each processing component.

---

## 19. Relationship with Trino

Trino is not an orchestration engine.

It provides the SQL query layer after Spark has persisted the required Apache
Iceberg tables.

The relationship is:

```text
Airflow
   │
   ▼
Spark processing
   │
   ▼
Apache Iceberg Gold
   │
   ▼
Trino
```

Trino can then be used to verify that the final analytical datasets are present
and queryable.

The independently executed E2E validation has already demonstrated that the
four Gold tables can be queried successfully through Trino.

---

## 20. Relationship with Superset

Apache Superset is a downstream analytical consumer.

The intended final path is:

```text
Airflow-orchestrated pipeline
          │
          ▼
         Gold
          │
          ▼
        Trino
          │
          ▼
       Superset
```

Airflow does not execute dashboard queries or implement visualization logic.

The Superset visualization stage remains separate from orchestration.

---

## 21. Independent E2E Processing Validation

Before final Airflow runtime validation, the complete processing path was
executed independently using real source data.

The validated historical interval was:

```text
2026-01-10 → 2026-01-15
```

Bronze ingestion completed with:

```text
AEMET stations          = 1 file
CNIG masters            = 2 files
ESIOS hourly            = 11 files
ESIOS monthly           = 9 files
Open-Meteo hourly       = 926 files
Open-Meteo 15-minute    = 926 files
AEMET current           = 1 file
```

The resulting Silver model contained:

```text
9 tables
```

with relevant counts:

```text
silver_open_meteo_hourly = 133344
silver_open_meteo_15min = 533376
silver_esios_energy_hourly = 38443
silver_esios_installed_capacity_monthly = 123
```

Gold persistence subsequently completed successfully.

Trino exposed exactly:

```text
gold_dim_geography
gold_dim_time
gold_fact_installed_capacity_monthly
gold_fact_province_hourly
```

with:

```text
gold_dim_geography = 71 rows
gold_dim_time = 158 rows
gold_fact_installed_capacity_monthly = 19 rows
gold_fact_province_hourly = 8147 rows
```

The principal Gold fact contained:

```text
8100 rows with weather
6768 rows with energy
6721 rows with weather and energy
0 duplicate Province × hour keys
```

This validates the processing components that Airflow must coordinate.

It does **not** replace final Airflow runtime validation.

---

## 22. Current Orchestration Status

The current status is:

```text
Airflow infrastructure
= VALIDATED

Airflow Webserver / Scheduler
= VALIDATED

Existing ingestion workflow execution
= VALIDATED

Historical reload DAG implementation
= IMPLEMENTED

Historical reload DAG structure / task definition
= VALIDATED

Bronze → Silver → Gold → Trino processing outside Airflow
= VALIDATED

Complete Airflow-triggered Bronze → Silver → Gold runtime execution
= PENDING FINAL VALIDATION
```

The final orchestration acceptance criterion is therefore a successful
Airflow-controlled execution of the required end-to-end processing path.

Until that execution is completed, the orchestration layer must not be
documented as fully closed.

---

## 23. Design Principles

The orchestration layer follows these principles.

### Separation of responsibilities

Airflow coordinates execution; it does not implement source or transformation
logic.

### Dependency safety

Downstream tasks depend on the successful completion of required upstream
tasks.

### Reusability

The same ingestion and Spark jobs can be run either independently or through
Airflow.

### Failure visibility

Failed tasks remain visible through Airflow states and logs.

### Source awareness

The orchestration layer respects the real temporal availability and behaviour
of each external source.

### No synthetic completion

A requested interval is not considered complete by inventing observations that
the external source does not provide.

### Modularity

Ingestion, Silver processing and Gold processing remain separate executable
components.

### Observability

Airflow provides execution state, logs and historical run information.

### Reprocessability

Historical workflows can be used to rebuild selected periods when required.

### Incremental extensibility

The same architecture can support recent or incremental windows without
redesigning the complete Lakehouse pipeline.

### Scope control

Operational enhancements such as a dedicated persistent dataset checkpoint
system are not presented as implemented until they have actually been built and
validated.