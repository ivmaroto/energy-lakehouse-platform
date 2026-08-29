# Lakehouse Design

## 1. Lakehouse Architecture

The Energy Lakehouse Platform follows a Lakehouse architecture that combines
raw object storage with managed analytical tables.

The platform uses MinIO as the common S3-compatible storage layer, while the
logical data model follows the Medallion Architecture:

```text
Bronze
   ↓
Silver
   ↓
Gold
```

The three layers do not use the same physical storage model.

Bronze preserves source acquisitions as raw objects in MinIO.

Silver and Gold are implemented as Apache Iceberg tables stored in MinIO and
managed through the shared Iceberg catalog.

Apache Spark and PySpark provide the distributed processing capabilities
required to transform data between layers.

Trino provides the interactive SQL query layer over the structured Apache
Iceberg datasets.

Apache Superset consumes the curated Gold datasets through Trino.

The resulting architecture is:

```text
External Sources
       │
       ▼
Python Ingestion
       │
       ▼
┌─────────────────┐
│     Bronze      │
│ Raw objects     │
│     MinIO       │
└────────┬────────┘
         │
         ▼
   Apache Spark
         │
         ▼
┌─────────────────┐
│     Silver      │
│ Apache Iceberg  │
│     MinIO       │
└────────┬────────┘
         │
         ▼
   Apache Spark
         │
         ▼
┌─────────────────┐
│      Gold       │
│ Apache Iceberg  │
│     MinIO       │
└────────┬────────┘
         │
         ▼
       Trino
         │
         ▼
 Apache Superset
```

This architecture preserves source traceability while progressively refining
data into reusable and business-ready analytical products.

---

## 2. Medallion Architecture

The platform uses the Medallion Architecture pattern to separate data according
to its level of processing.

### Bronze

Bronze contains source acquisitions with minimal modification.

Its objective is to preserve the original source payload and technical
ingestion metadata.

### Silver

Silver contains normalized, typed, deduplicated and reusable datasets.

Its objective is to create a consistent representation of each source without
performing premature analytical integration.

### Gold

Gold contains the final analytical products.

Its objective is to integrate and aggregate the Silver datasets according to
the validated analytical use cases.

The general progression is:

```text
External Sources
       │
       ▼
     Bronze
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
       │
       ▼
     Trino
       │
       ▼
   Superset
```

---

## 3. Bronze Layer

The Bronze layer is the raw landing area of the platform.

It receives information acquired from:

```text
AEMET OpenData
Open-Meteo
REE / ESIOS
CNIG / IGN
```

Bronze data is persisted in MinIO as source objects rather than Apache Iceberg
tables.

The logical storage structure follows:

```text
bronze/
└── <source>/
    └── <dataset>/
        └── year=YYYY/
            └── month=MM/
                └── day=DD/
                    └── <object>
```

The temporal directory hierarchy represents the ingestion date.

The actual requested source interval is preserved independently inside the
Bronze ingestion metadata.

### Bronze responsibilities

Bronze is responsible for:

- preserving source payloads;
- recording ingestion metadata;
- separating providers and datasets;
- supporting historical acquisition;
- supporting incremental acquisition;
- retaining repeated acquisitions when applicable;
- providing the input for Silver processing.

Bronze does not perform:

- geographical harmonization;
- unit reinterpretation;
- cross-source joins;
- analytical aggregations;
- KPI calculation;
- definitive business-level deduplication.

Typical metadata includes:

```text
source
dataset
ingestion_mode
ingestion_timestamp
requested_start_date
requested_end_date
```

Additional source-specific traceability fields may also be stored.

---

## 4. Silver Layer

The Silver layer contains the normalized and reusable datasets derived from
Bronze.

Apache Spark and PySpark perform the Bronze-to-Silver transformations.

Silver is persisted as Apache Iceberg tables in MinIO.

### Silver responsibilities

The Silver layer performs:

- parsing of raw source payloads;
- explicit data typing;
- temporal normalization;
- natural-key deduplication;
- coordinate normalization;
- geographical normalization against CNIG when applicable;
- validation of mandatory fields;
- structural data-quality controls;
- preservation of valid missing values;
- preparation of datasets for downstream integration.

Silver does not fabricate missing observations.

A valid source `NULL` is not automatically interpreted as zero.

Silver also avoids changing the real geographical or temporal granularity of a
source unless the transformation is explicitly required by the normalized
dataset design.

---

## 5. Physical Silver Model

The current Silver implementation contains exactly **9 Apache Iceberg tables**.

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

The previous experimental Silver datasets for AEMET daily climatology,
Open-Meteo historical forecast as a separate physical table and ESIOS
5-minute power are not part of the current physical Silver model.

---

## 6. Geographical Normalization

CNIG / IGN is the canonical geographical reference used by the platform.

The validated territorial masters contain:

```text
52 province-level entities
19 autonomous communities
8132 municipalities
```

Official codes are preserved as strings so leading zeroes are retained.

When source data provides sufficient geographical information, Silver applies a
normalization process such as:

```text
source geographical value
        │
        ▼
deterministic normalization
        │
        ▼
controlled alias resolution when required
        │
        ▼
CNIG canonical province
        │
        ▼
canonical autonomous community
```

The normalized geographical attributes include, where applicable:

```text
province_code
province_name
autonomous_community_code
autonomous_community_name
```

The platform never manufactures geographical detail that is absent from the
source.

---

## 7. Meteorological Silver Model

The meteorological Silver layer preserves AEMET and Open-Meteo as separate
sources.

### AEMET

The active datasets are:

```text
silver_aemet_stations
silver_aemet_current_observations
```

The station catalogue provides the official meteorological point catalogue.

The current validated catalogue contains:

```text
926 stations
```

AEMET current observations provide recent official meteorological measurements.

They are not used as a generic historical source for arbitrary past periods.

### Open-Meteo

The active datasets are:

```text
silver_open_meteo_hourly
silver_open_meteo_15min
```

Open-Meteo supplies the reproducible historical meteorological data required by
the principal analytical flow.

Hourly and 15-minute observations remain separate in Silver.

Temporal aggregation required for analytical products is performed in Gold.

---

## 8. Energy Silver Model

REE / ESIOS provides the electricity-system information used by the project.

The final active source configuration contains:

```text
11 hourly electricity-generation indicators
9 monthly installed-capacity indicators
```

These datasets are normalized into:

```text
silver_esios_energy_hourly
silver_esios_installed_capacity_monthly
```

Silver preserves:

- indicator identity;
- source timestamp;
- source geography;
- numerical value;
- source traceability.

The previously evaluated 5-minute ESIOS flow is not part of the final physical
Silver model.

---

## 9. Gold Layer

The Gold layer contains the analytical products generated from Silver.

Gold is implemented using Apache Spark and persisted as Apache Iceberg tables.

The purpose of Gold is to centralize transformations required for analytical
consumption so that downstream tools do not need to reproduce business logic.

Gold performs operations such as:

- temporal aggregation;
- spatial aggregation;
- metric selection;
- cross-source integration;
- analytical dimensional modelling;
- metric-level source fallback;
- construction of reusable facts and dimensions.

Gold does not artificially fill unavailable source observations.

---

## 10. Physical Gold Model

The current Gold implementation contains exactly **4 Apache Iceberg tables**:

```text
gold_fact_province_hourly
gold_fact_installed_capacity_monthly
gold_dim_geography
gold_dim_time
```

No additional country-level 5-minute or 15-minute physical Gold fact tables are
part of the current implementation.

---

## 11. `gold_fact_province_hourly`

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

The table integrates meteorological information with hourly ESIOS
electricity-generation metrics.

Examples of weather metrics include:

```text
temperature
humidity
precipitation
wind_speed_80m
wind_direction_80m
wind_speed_120m
wind_direction_120m
solar_radiation
direct_normal_irradiance
```

Examples of electricity-generation metrics include:

```text
wind_generation_mwh
solar_photovoltaic_generation_mwh
solar_thermal_generation_mwh
hydraulic_generation_mwh
nuclear_generation_mwh
combined_cycle_generation_mwh
gas_natural_steam_turbine_generation_mwh
gas_natural_cogeneration_mwh
coal_generation_mwh
other_renewables_generation_mwh
total_generation_mwh
```

---

## 12. Weather Aggregation

Meteorological observations are aggregated to the Province × hour analytical
grain.

### AEMET variables

For the metrics where AEMET is applicable:

```text
temperature
humidity
precipitation
```

AEMET acts as the preferred source when a valid observation exists.

Open-Meteo provides the fallback for the individual metric when AEMET is not
available.

The fallback is metric-specific.

It does not replace the entire Province × hour weather record.

Gold retains source traceability using:

```text
temperature_source
humidity_source
precipitation_source
```

### Open-Meteo variables

Open-Meteo supplies metrics not available from the AEMET current-observation
flow used by the analytical model, including:

```text
wind at 80 m
wind at 120 m
solar radiation
direct normal irradiance
```

Open-Meteo 15-minute observations are aggregated to hourly values where
required.

Scalar values use arithmetic aggregation.

Wind direction requires circular averaging rather than ordinary arithmetic
averaging.

---

## 13. Weather and Energy Integration

The meteorological block and the hourly energy block are independently prepared
at:

```text
Province × hour
```

Before joining them, uniqueness is validated on:

```text
province_code
gold_timestamp
```

The final integration uses:

```text
FULL OUTER JOIN
```

on:

```text
province_code
gold_timestamp
```

This preserves valid observations from either source.

The resulting behaviour is:

```text
Weather available
Energy unavailable
→ keep the row
→ energy metrics remain NULL
```

```text
Energy available
Weather unavailable
→ keep the row
→ meteorological metrics remain NULL
```

```text
Weather available
Energy available
→ integrate both domains in the same row
```

The platform does not fabricate values to force artificial source coverage.

---

## 14. `gold_fact_installed_capacity_monthly`

Installed electricity-generation capacity is represented in:

```text
gold_fact_installed_capacity_monthly
```

Its grain is:

```text
Autonomous Community × month
```

The natural key is:

```text
autonomous_community_code + year_month
```

Installed capacity remains expressed in MW.

It is not converted to MWh because MW represents power rather than energy.

The table contains installed-capacity metrics for the selected ESIOS
technologies, including:

```text
hydraulic
wind
solar photovoltaic
solar thermal
renewable total
nuclear
coal
combined cycle
other renewables
```

Installed capacity is not artificially distributed from autonomous communities
to provinces.

---

## 15. Gold Dimensions

The Gold layer also contains two reusable dimensions.

### `gold_dim_geography`

Provides geographical analytical attributes used by the facts.

### `gold_dim_time`

Provides temporal analytical attributes used by the Gold model.

These dimensions support consistent filtering, grouping and downstream
visualization.

---

## 16. Geographical Analytical Strategy

The platform deliberately does not impose a single geographical level on all
data.

The approved rule is:

```text
Use Province when the validated source supports Province.

Otherwise preserve the actual available geographical level.
```

The principal Gold fact therefore operates at:

```text
Province × hour
```

while installed capacity remains at:

```text
Autonomous Community × month
```

The following concepts remain distinct:

```text
Province
Autonomous Community
Spain
Peninsula
```

Geographical levels are never treated as interchangeable.

---

## 17. Peninsula Scope

Where peninsular meteorological aggregation is required by analytical logic, the
validated Peninsula scope excludes the following province codes:

```text
07  Illes Balears
35  Las Palmas
38  Santa Cruz de Tenerife
51  Ceuta
52  Melilla
```

Peninsula weather is therefore derived from the eligible province-level
meteorological data.

Spain-wide data must not simply be relabelled as Peninsula data.

---

## 18. Apache Iceberg Storage Model

Apache Iceberg provides the structured table abstraction for Silver and Gold.

The physical relationship is:

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
```

Spark is primarily responsible for:

- creating tables;
- writing transformed data;
- updating analytical datasets.

Trino is primarily responsible for:

- interactive SQL querying;
- analytical inspection;
- downstream SQL access.

Both engines operate over the same Apache Iceberg tables.

---

## 19. Data Lifecycle

The complete data lifecycle is:

```text
AEMET ────────────┐
Open-Meteo ───────┤
REE / ESIOS ──────┼──► Python ingestion
CNIG / IGN ───────┘
                         │
                         ▼
                    MinIO / Bronze
                         │
                         ▼
                    Apache Spark
                         │
                         ▼
             Apache Iceberg / Silver
                         │
                         ▼
                    Apache Spark
                         │
                         ▼
              Apache Iceberg / Gold
                         │
                         ▼
                       Trino
                         │
                         ▼
                Apache Superset
```

Apache Airflow provides the orchestration layer that coordinates the execution
of the pipeline.

---

## 20. Processing and Query Separation

A fundamental architectural principle is the separation between distributed
processing and interactive analytical querying.

### Apache Spark

Apache Spark and PySpark are responsible for:

- Bronze-to-Silver processing;
- Silver-to-Gold processing;
- validation;
- normalization;
- deduplication;
- geographical mapping;
- temporal aggregation;
- cross-source integration;
- Iceberg persistence.

### Trino

Trino is responsible for:

- interactive SQL access;
- querying persisted Iceberg tables;
- analytical inspection;
- exposing Gold datasets to Apache Superset.

The analytical consumer therefore does not need to execute Spark jobs.

---

## 21. Validated Physical Implementation

The current Lakehouse implementation has been validated using real source data.

A complete historical execution for:

```text
2026-01-10 → 2026-01-15
```

successfully populated Bronze and produced the current Silver and Gold models.

### Silver validation

The final Silver namespace contains:

```text
9 tables
```

Relevant validated row counts include:

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

Open-Meteo counts correspond exactly to:

```text
926 × 144 = 133344 hourly rows

926 × 576 = 533376 fifteen-minute rows
```

### Gold validation

The final Gold namespace contains:

```text
4 tables
```

Validated row counts are:

```text
gold_dim_geography = 71
gold_dim_time = 158
gold_fact_installed_capacity_monthly = 19
gold_fact_province_hourly = 8147
```

For the hourly integrated fact:

```text
rows with weather = 8100
rows with energy = 6768
rows with weather and energy = 6721

duplicate Province × hour keys = 0
```

For installed capacity:

```text
rows = 19
distinct months = 1
duplicate Autonomous Community × month keys = 0
rows containing capacity values = 19
```

Real rows containing both meteorological and energy metrics were also queried
successfully through Trino.

This validates the processing path:

```text
Bronze
→ Silver
→ Gold
→ Trino
```

using real source data.

---

## 22. Design Principles

The final Lakehouse design follows these principles.

### Raw-data preservation

Bronze preserves source acquisitions before analytical transformation.

### Progressive refinement

Each layer performs only the transformations appropriate to its role.

### No synthetic source detail

Missing geographical, temporal or measurement information is not invented.

### Natural-key consistency

Silver and Gold datasets use explicit natural keys to avoid duplicated logical
records.

### Source traceability

Source values and relevant metadata are retained where necessary to understand
the origin of analytical information.

### Canonical geography

CNIG / IGN provides the territorial reference used for normalization.

### Analytical grain based on real source capabilities

The platform uses Province × hour where the sources support that grain and
preserves higher-level geographies where they do not.

### Processing and query separation

Spark performs distributed processing.

Trino performs interactive querying.

### Managed analytical tables

Silver and Gold use Apache Iceberg.

Bronze remains a raw object-storage layer.

### Governed analytical consumption

The final Business Intelligence layer consumes curated Gold datasets through
Trino rather than reproducing processing logic in dashboards.

### Open Source and reproducibility

The complete architecture remains based on Open Source technologies and is
deployable locally using Docker Compose.