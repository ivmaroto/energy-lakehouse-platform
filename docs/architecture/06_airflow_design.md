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

The orchestration layer supports:

- an end-to-end historical reload workflow covering Bronze, Silver and Gold;
- recurrent source-ingestion workflows that persist data in Bronze;
- manual historical Open-Meteo 15-minute reconstruction.

The complete historical processing path has been validated both independently
from Airflow and under direct Airflow control:

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

The Airflow-controlled historical Bronze → Silver → Gold path and its validated
persistence policies are therefore part of the implemented platform.

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

Final DAG-import validation returned:

```text
No data found
```

from:

```text
airflow dags list-import-errors
```

which confirms that no DAG import errors were present.

The runtime DAG inventory contained exactly:

```text
historical_reload
hourly_ingestion
monthly_ingestion
open_meteo_15min
```

The `historical_reload` DAG has also been executed successfully through the
complete:

```text
Bronze
→ Silver
→ Gold
```

processing path.

Its three persistence behaviours were validated separately with real data:

```text
PRESERVE
RANGE OVERWRITE
FULL DELETE
```

Therefore, Airflow infrastructure, DAG discovery and the historical end-to-end
orchestration path are all validated.

---

## 6. Current Airflow Workflows

The current repository contains four Airflow DAGs.

### `historical_reload`

Role:

```text
historical end-to-end reload
Bronze → Silver → Gold
```

It is the principal workflow for controlled historical reconstruction and for
the persistence policies described later in this document.

### `hourly_ingestion`

Role:

```text
hourly Bronze ingestion
```

It coordinates:

- approved hourly ESIOS generation indicators;
- AEMET current observations;
- Open-Meteo hourly ingestion for the AEMET station master.

Its schedule is:

```text
0 * * * *
```

This DAG persists source data in Bronze. It does not itself execute the Silver
or Gold stages.

### `monthly_ingestion`

Role:

```text
monthly Bronze ingestion
```

It coordinates the approved ESIOS installed-capacity indicators.

Its schedule is:

```text
@monthly
```

This DAG also persists source data in Bronze and does not itself execute the
Silver or Gold stages.

### `open_meteo_15min`

Role:

```text
manual historical Open-Meteo 15-minute Bronze reconstruction
```

Its schedule is:

```text
None
```

The underlying 15-minute batch implementation is reserved for historical
reconstruction.

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
Lakehouse processing chain while applying an explicit persistence policy.

The validated workflow is:

```text
Start
  │
  ▼
Apply Bronze persistence policy
  │
  ▼
Select Silver / Gold deletion policy
  │
  ├── FULL
  │     ├─ drop / purge current Silver and Gold tables
  │     └─ remove residual warehouse/silver and warehouse/gold objects
  │
  ├── RANGE
  │     └─ delete only the requested Silver / Gold temporal range
  │
  └── PRESERVE
        └─ skip Silver / Gold deletion
             │
             ▼
      Persistence policy complete
             │
     ┌───────┼──────────────┐
     │       │              │
     ▼       ▼              ▼
 Masters   ESIOS hourly   ESIOS monthly
     │
     ▼
 Open-Meteo historical
     │
     └──────────┬───────────┘
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

AEMET current observations are deliberately excluded from this historical
workflow.

Master ingestion behaves as an ensure operation:

```text
master already exists
→ preserve it

master missing
→ ingest it
```

Therefore:

- PRESERVE keeps existing masters unchanged;
- RANGE OVERWRITE keeps existing masters unchanged;
- FULL DELETE removes the active Bronze layer, after which the missing masters
  are rebuilt.

The workflow reuses the same Python ingestion and Spark-processing
implementations that have already been validated independently.

It does not duplicate transformation logic inside the DAG.

---

## 8. Historical Parameters and Persistence Policies

The historical reload workflow exposes exactly four runtime parameters:

```text
fecha_inicio
fecha_fin
sobreescribir_datos
eliminar_historial_completo
```

`fecha_inicio` and `fecha_fin` define the requested historical interval and are
required.

The two boolean parameters select the persistence behaviour.

### PRESERVE

```text
sobreescribir_datos = false
eliminar_historial_completo = false
```

Behaviour:

- preserve existing Bronze data;
- preserve existing Silver and Gold rows;
- preserve existing masters;
- ingest missing source coverage;
- insert only natural keys that are not already persisted.

This behaviour was validated physically and logically. Existing Silver and Gold
Parquet files remained unchanged while new temporal coverage was added, and no
duplicate natural keys were produced.

### RANGE OVERWRITE

```text
sobreescribir_datos = true
eliminar_historial_completo = false
```

Behaviour:

- delete the requested Bronze historical interval;
- delete the requested Silver and Gold temporal interval;
- preserve data outside the requested interval;
- preserve existing masters;
- rebuild the requested interval.

Validation confirmed that active Iceberg files inside the overwritten range
were replaced, while files outside the range and master-object metadata remained
unchanged.

### FULL DELETE

```text
eliminar_historial_completo = true
```

Full deletion has priority over range overwrite.

Behaviour:

- delete the complete active Bronze layer, including masters;
- drop and purge the current 9 Silver tables;
- drop and purge the current 4 Gold tables;
- remove residual physical objects under `warehouse/silver/` and
  `warehouse/gold/`;
- rebuild missing masters;
- load only the requested historical interval;
- recreate Silver and Gold.

Validation confirmed that no data files from previous runs remained after the
full reset.

### Historical write policy

The historical DAG passes:

```text
LAKEHOUSE_WRITE_POLICY=insert-only
```

only to:

```text
silver_write
gold_write
```

This prevents existing natural keys from being physically rewritten during
PRESERVE.

RANGE OVERWRITE and FULL DELETE first remove the data that must be reconstructed,
so the rebuilt rows are subsequently inserted as new rows.

AEMET current observations are not reinterpreted as arbitrary historical
observations and are excluded from `historical_reload`.

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

Canonical daily objects are validated for expected temporal coverage rather
than being considered complete only because the object exists.

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

ESIOS responses are technically validated before Bronze persistence.

A valid ESIOS response with:

```text
values = []
```

is handled as a valid `NO_DATA` result. It returns no observations and does not
fabricate synthetic records.

---

### CNIG / IGN

CNIG provides the territorial master required by Silver geographical
normalization.

The master data is independent from the analytical historical date interval.

During PRESERVE and RANGE OVERWRITE, an existing master is preserved. After a
FULL DELETE or in a clean installation, a missing master is ingested again.

---

### AEMET

AEMET provides:

```text
stations
current_observations
```

The station catalogue is used as a meteorological location master.

The station master is ensured by the historical workflow, but AEMET current
observations are deliberately excluded from historical reconstruction.

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

The historical orchestration preserves the following dependency relationship:

```text
Start
  │
  ▼
Bronze persistence policy
  │
  ▼
Silver / Gold deletion branch
  │
  ▼
persistence_policy_complete
  │
  ├─► masters ─► Open-Meteo historical
  ├─► ESIOS hourly
  └─► ESIOS monthly
           │
           ▼
      bronze_complete
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

The convergence task after the deletion branch uses:

```text
TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
```

so the selected branch can continue while the non-selected branch tasks remain
skipped.

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

The current implementation distinguishes historical end-to-end orchestration
from recurrent Bronze ingestion.

The current workflows are:

```text
historical_reload
→ historical Bronze → Silver → Gold

hourly_ingestion
→ recurrent hourly Bronze ingestion

monthly_ingestion
→ recurrent monthly Bronze ingestion

open_meteo_15min
→ manual historical 15-minute Bronze reconstruction
```

Therefore, the current hourly and monthly DAGs must not be described as
end-to-end Bronze → Silver → Gold pipelines.

They acquire and persist source data in Bronze.

Automatic promotion of those recurrent Bronze acquisitions through Silver and
Gold is not part of the currently validated recurrent DAG implementation.

The historical workflow remains the validated Airflow-controlled path for
complete Bronze → Silver → Gold reconstruction.

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

This independent run predates the final historical persistence-policy
validation and included an AEMET current-observation acquisition. The final
`historical_reload` workflow deliberately excludes AEMET current observations.

Final Airflow-controlled runtime validation is recorded in Section 22.

---

## 22. Current Orchestration Status

The final validated status is:

```text
Airflow infrastructure
= VALIDATED

Airflow Webserver / Scheduler
= VALIDATED

DAG import validation
= VALIDATED / no import errors

DAG discovery
= VALIDATED / 4 DAGs registered

Historical reload DAG structure
= VALIDATED

Complete Airflow-triggered Bronze → Silver → Gold runtime execution
= VALIDATED

PRESERVE persistence policy
= VALIDATED

RANGE OVERWRITE persistence policy
= VALIDATED

FULL DELETE persistence policy
= VALIDATED

Master preservation during PRESERVE / RANGE
= VALIDATED

Master rebuild during FULL DELETE
= VALIDATED

Physical removal of previous-run Silver / Gold data files during FULL DELETE
= VALIDATED

Gold natural-key duplicates after orchestration validation
= 0
```

The final full-delete validation also confirmed:

```text
OLD_PREVIOUS_RUN_OBJECTS = 0
```

after reconstruction.

After the final orchestration and persistence changes, the complete automated
test suites passed:

```text
tests/ingestion = 84 passed
tests/silver    = 85 passed
tests/gold      = 72 passed
```

No failures remained in these suites.

The historical orchestration layer is therefore implemented and validated.

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