# Data Flow

## 1. Overview

The Energy Lakehouse Platform implements a structured data flow covering the
complete lifecycle of the information, from external source acquisition to
analytical SQL consumption.

The platform integrates:

- AEMET OpenData;
- Open-Meteo;
- REE / ESIOS;
- CNIG / IGN geographical reference data.

Python-based ingestion components acquire and technically validate source data.

Raw acquisitions are persisted in the Bronze layer in MinIO.

Apache Spark and PySpark subsequently transform Bronze data into normalized
Silver Apache Iceberg tables and then generate the final Gold analytical
products.

Trino provides SQL access to the Apache Iceberg tables.

Apache Superset represents the downstream visualization layer.

The general data flow is:

```text
External Sources
       │
       ▼
Python Ingestion
       │
       ▼
┌─────────────────┐
│     Bronze      │
│ Raw MinIO data  │
└────────┬────────┘
         │
         ▼
   Apache Spark
         │
         ▼
┌─────────────────┐
│     Silver      │
│ Apache Iceberg  │
└────────┬────────┘
         │
         ▼
   Apache Spark
         │
         ▼
┌─────────────────┐
│      Gold       │
│ Apache Iceberg  │
└────────┬────────┘
         │
         ▼
       Trino
         │
         ▼
 Apache Superset
```

Apache Airflow provides the orchestration layer responsible for coordinating
pipeline executions.

---

## 2. Source Acquisition

Source acquisition is implemented independently for each provider.

```text
AEMET ────────────┐
Open-Meteo ───────┤
REE / ESIOS ──────┼──► Python ingestion
CNIG / IGN ───────┘
```

Each source connector is responsible for:

1. constructing the source request;
2. applying authentication when required;
3. validating temporal or geographical parameters;
4. executing the external request;
5. validating the technical response;
6. generating ingestion metadata;
7. persisting the source information in Bronze.

Business transformations are intentionally excluded from this stage.

---

## 3. Historical Data Flow

Historical ingestion retrieves source information for an explicitly requested
temporal interval when the source supports historical access.

The general interface is based on:

```text
start_date
end_date
```

The processing path is:

```text
Requested interval
       │
       ▼
Source-specific ingestion
       │
       ▼
Technical validation
       │
       ▼
MinIO / Bronze
       │
       ▼
Apache Spark
       │
       ▼
Silver
       │
       ▼
Apache Spark
       │
       ▼
Gold
```

Large temporal intervals can be divided into smaller source-specific request
windows when required by API limitations or operational considerations.

The historical strategy differs according to the source.

### Open-Meteo

The current historical meteorological flow uses:

```text
Hourly data
→ Open-Meteo Archive API

15-minute data
→ Open-Meteo Historical Forecast API
```

The AEMET station catalogue supplies the locations and coordinates used for
Open-Meteo acquisition.

The current validated catalogue contains:

```text
926 locations
```

### REE / ESIOS

The historical energy flow uses:

```text
11 hourly generation indicators
9 monthly installed-capacity indicators
```

The selected indicators are externalized in:

```text
config/esios_indicators.json
```

### AEMET

AEMET provides:

```text
stations
current_observations
```

The station catalogue acts as a geographical point master.

`current_observations` provides recent observations and is not used to
reconstruct arbitrary historical periods.

### CNIG / IGN

CNIG provides the geographical master data required for downstream territorial
normalization.

The current source masters are:

```text
provinces
municipalities
```

Autonomous communities are derived in Silver from the canonical territorial
information.

---

## 4. Bronze Data Flow

Bronze is the first persistence layer.

It stores source acquisitions in MinIO with minimal modification.

Conceptually:

```text
External API
     │
     ▼
Python connector
     │
     ▼
Technical validation
     │
     ▼
Bronze object
     │
     ▼
MinIO
```

Bronze objects are organized logically as:

```text
bronze/
└── <source>/
    └── <dataset>/
        └── year=YYYY/
            └── month=MM/
                └── day=DD/
                    └── <object>
```

The physical `year/month/day` hierarchy represents the ingestion date.

The requested source interval is stored independently in object metadata.

Typical metadata includes:

```text
source
dataset
ingestion_mode
ingestion_timestamp
requested_start_date
requested_end_date
```

Bronze does not perform:

- geographical harmonization;
- analytical aggregations;
- cross-source joins;
- metric calculation;
- definitive business-level deduplication.

---

## 5. Bronze-to-Silver Flow

Apache Spark reads the Bronze datasets and generates the normalized Silver
layer.

The general process is:

```text
Bronze
   │
   ▼
Source parsing
   │
   ▼
Typing
   │
   ▼
Temporal normalization
   │
   ▼
Deduplication
   │
   ▼
Geographical normalization
   │
   ▼
Data-quality validation
   │
   ▼
Apache Iceberg Silver
```

Silver processing performs operations including:

- conversion to explicit data types;
- timestamp normalization;
- natural-key deduplication;
- coordinate validation;
- geographical normalization against CNIG when applicable;
- preservation of valid missing measurements;
- structural validation.

Missing observations are not synthetically generated.

A source `NULL` is not automatically replaced by zero.

---

## 6. Physical Silver Flow

The current physical Silver model contains exactly **9 Apache Iceberg tables**.

### AEMET

```text
silver_aemet_stations
silver_aemet_current_observations
```

### Open-Meteo

```text
silver_open_meteo_hourly
silver_open_meteo_15min
```

### CNIG

```text
silver_cnig_provinces
silver_cnig_autonomous_communities
silver_cnig_municipalities
```

### REE / ESIOS

```text
silver_esios_energy_hourly
silver_esios_installed_capacity_monthly
```

The data remains source-oriented in Silver.

Cross-domain analytical integration is deferred to Gold.

---

## 7. Geographical Data Flow

CNIG / IGN acts as the canonical geographical reference.

The geographical normalization process follows:

```text
Source geography
      │
      ▼
Deterministic normalization
      │
      ▼
Controlled alias resolution
      │
      ▼
CNIG province
      │
      ▼
Autonomous community
```

The validated canonical geographical structure includes:

```text
52 province-level entities
19 autonomous communities
8132 municipalities
```

Official geographical codes are retained as strings to preserve leading
zeroes.

The platform does not manufacture geographical information.

If a source only supports a higher geographical level, that real level is
preserved.

---

## 8. Silver-to-Gold Flow

Gold processing transforms the normalized Silver datasets into analytical
products.

The general flow is:

```text
Silver meteorology ─────┐
                        │
Silver energy ──────────┼──► Apache Spark
                        │
Silver geography ───────┤
                        │
Silver masters ─────────┘
                              │
                              ▼
                    Analytical aggregation
                              │
                              ▼
                     Source integration
                              │
                              ▼
                   Apache Iceberg Gold
```

Gold processing includes:

- temporal aggregation;
- spatial aggregation;
- analytical metric selection;
- metric-specific meteorological fallback;
- energy metric preparation;
- cross-source integration;
- construction of fact and dimension tables.

---

## 9. Main Hourly Analytical Flow

The principal analytical product is:

```text
gold_fact_province_hourly
```

Its grain is:

```text
Province × hour
```

The natural key is:

```text
province_code + gold_timestamp
```

The meteorological and energy flows are prepared independently before
integration.

### Meteorological block

```text
AEMET current observations ──┐
                             │
Open-Meteo hourly ───────────┼──► Province × hour weather
                             │
Open-Meteo 15-minute ────────┘
```

AEMET is the preferred source for:

```text
temperature
humidity
precipitation
```

when a valid metric is available.

Open-Meteo provides metric-level fallback.

Open-Meteo also supplies analytical variables including:

```text
wind_speed_80m
wind_direction_80m
wind_speed_120m
wind_direction_120m
solar_radiation
direct_normal_irradiance
```

### Energy block

The configured ESIOS hourly indicators are normalized into:

```text
silver_esios_energy_hourly
```

and transformed into a:

```text
Province × hour
```

energy block.

### Integration

Uniqueness is validated on both blocks before integration.

The join is performed on:

```text
province_code
gold_timestamp
```

using:

```text
FULL OUTER JOIN
```

This deliberately preserves valid source coverage.

```text
Weather + Energy
→ one integrated row
```

```text
Weather only
→ preserve row
→ energy metrics remain NULL
```

```text
Energy only
→ preserve row
→ weather metrics remain NULL
```

Missing information is not transformed into artificial zero values.

---

## 10. Installed-Capacity Flow

The second Gold fact is:

```text
gold_fact_installed_capacity_monthly
```

The processing path is:

```text
ESIOS monthly indicators
         │
         ▼
silver_esios_installed_capacity_monthly
         │
         ▼
Geographical normalization
         │
         ▼
Gold monthly pivot / preparation
         │
         ▼
gold_fact_installed_capacity_monthly
```

Its analytical grain is:

```text
Autonomous Community × month
```

Installed capacity remains expressed in:

```text
MW
```

It is not converted to MWh and is not artificially distributed to provinces.

---

## 11. Gold Physical Model

The current Gold layer contains exactly **4 Apache Iceberg tables**:

```text
gold_fact_province_hourly
gold_fact_installed_capacity_monthly
gold_dim_geography
gold_dim_time
```

The two dimensions provide reusable geographical and temporal attributes for
analytical consumption.

---

## 12. Data Validation Flow

Validation occurs at several points rather than only at one transition.

### Ingestion validation

Includes:

- valid requests;
- valid response structures;
- authentication;
- expected data presence where required;
- temporal coverage checks;
- external API error handling.

### Silver validation

Includes:

- natural-key validation;
- duplicate detection;
- timestamp validation;
- coordinate validation;
- geographical correspondence;
- expected schema validation;
- source granularity preservation.

### Gold validation

Includes:

- expected table existence;
- natural-key uniqueness;
- expected analytical grain;
- presence of meteorological metrics;
- presence of energy metrics;
- integrated weather-and-energy rows;
- installed-capacity values;
- SQL accessibility through Trino.

The data-quality flow can therefore be represented as:

```text
Source
  │
  ▼
Technical validation
  │
  ▼
Bronze
  │
  ▼
Normalization validation
  │
  ▼
Silver
  │
  ▼
Analytical validation
  │
  ▼
Gold
```

---

## 13. Incremental Data Flow

The platform is designed to support incremental acquisition after an initial
historical population.

The general intended flow is:

```text
Execution window
       │
       ▼
Source API
       │
       ▼
Newly available source data
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

The exact window and availability behaviour can differ by source because
publication schedules and API capabilities are not identical.

The current Bronze metadata records requested temporal boundaries and ingestion
timestamps.

A persistent business-level checkpoint table that automatically stores the last
successful timestamp per dataset is **not currently part of the validated
implementation**.

Execution state is therefore not documented as if such a checkpoint mechanism
already existed.

Checkpoint-based continuation remains a possible future extension of the
orchestration layer.

---

## 14. Workflow Orchestration

Apache Airflow provides the workflow orchestration component.

Its responsibility is to coordinate execution rather than implement data
transformation logic.

The intended end-to-end dependency chain is:

```text
Start
  │
  ▼
Source ingestion
  │
  ▼
Bronze available
  │
  ▼
Silver table creation / processing
  │
  ▼
Gold table creation / processing
  │
  ▼
Gold available through Trino
  │
  ▼
End
```

Existing source ingestion DAGs have previously been validated.

A historical reload DAG has been implemented to coordinate Bronze ingestion and
the downstream Spark Silver and Gold jobs.

Final runtime validation of the complete Airflow-orchestrated end-to-end
execution belongs to the orchestration closure phase.

Therefore, the data-processing path itself is validated, while final Airflow
runtime orchestration should not yet be documented as completed.

---

## 15. Processing and Query Separation

Processing and analytical consumption are intentionally separated.

```text
                PROCESSING

Bronze ──► Apache Spark ──► Silver

Silver ──► Apache Spark ──► Gold


             SQL CONSUMPTION

Gold ──► Trino ──► Apache Superset
```

Apache Spark is responsible for:

- distributed transformation;
- aggregation;
- normalization;
- joins;
- Iceberg writes.

Trino is responsible for:

- interactive SQL querying;
- validation of persisted tables;
- exposing Gold datasets to downstream analytical consumers.

Apache Superset is responsible for visualization.

---

## 16. Validated End-to-End Data Flow

The core Lakehouse processing flow has been validated with real source data.

A complete historical Bronze execution was performed for:

```text
2026-01-10 → 2026-01-15
```

The Bronze execution completed with:

```text
AEMET station master     = 1 file
CNIG masters             = 2 files
ESIOS hourly             = 11 files
ESIOS monthly            = 9 files
Open-Meteo hourly        = 926 files
Open-Meteo 15-minute     = 926 files
AEMET current            = 1 file
```

The resulting Silver namespace contained exactly:

```text
9 tables
```

with validated counts including:

```text
silver_aemet_stations = 926
silver_aemet_current_observations = 9786

silver_open_meteo_hourly = 133344
silver_open_meteo_15min = 533376

silver_cnig_provinces = 52
silver_cnig_autonomous_communities = 19
silver_cnig_municipalities = 8132

silver_esios_energy_hourly = 38443
silver_esios_installed_capacity_monthly = 123
```

The Gold processing subsequently completed successfully with exactly:

```text
4 tables
```

and the following validated row counts:

```text
gold_dim_geography = 71
gold_dim_time = 158
gold_fact_installed_capacity_monthly = 19
gold_fact_province_hourly = 8147
```

The principal Gold fact was validated with:

```text
rows with meteorological information = 8100
rows with energy information = 6768
rows with both domains = 6721

duplicate Province × hour keys = 0
```

The monthly installed-capacity fact was validated with:

```text
rows = 19
month = 2026-01
rows with capacity values = 19
duplicate Autonomous Community × month keys = 0
```

Real integrated rows containing meteorological and ESIOS energy metrics in the
same Province × hour record were successfully queried through Trino.

Therefore, the following core data-processing flow is technically validated:

```text
Real external sources
        │
        ▼
      Bronze
        │
        ▼
      Silver
        │
        ▼
       Gold
        │
        ▼
      Trino
```

---

## 17. Analytical Consumption

The curated analytical path is:

```text
Apache Iceberg Gold
        │
        ▼
      Trino
        │
        ▼
Apache Superset
        │
        ▼
Dashboards / Analysis
```

Gold is the intended Business Intelligence consumption layer.

This prevents visualization logic from depending directly on raw Bronze objects
or normalized Silver source tables.

The principal analytical datasets exposed downstream are:

```text
Province × hour
meteorology + electricity generation

Autonomous Community × month
installed capacity
```

---

## 18. Complete Data Flow

The final platform data flow can be summarized as:

```text
AEMET ─────────────┐
Open-Meteo ────────┤
REE / ESIOS ───────┼──► Python Ingestion
CNIG / IGN ────────┘          │
                              ▼
                        MinIO / Bronze
                              │
                              ▼
                         Apache Spark
                              │
                              ▼
                    Apache Iceberg Silver
                         9 tables
                              │
                              ▼
                         Apache Spark
                              │
                              ▼
                     Apache Iceberg Gold
                         4 tables
                              │
                              ▼
                            Trino
                              │
                              ▼
                       Apache Superset

          Apache Airflow coordinates pipeline execution
```

This design provides a clear separation between acquisition, raw persistence,
normalization, analytical transformation, SQL querying and visualization while
preserving source traceability and the actual temporal and geographical
capabilities of the integrated data sources.