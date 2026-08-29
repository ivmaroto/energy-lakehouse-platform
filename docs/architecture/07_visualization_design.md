# Visualization Design

## 1. Overview

The visualization layer represents the analytical consumption stage of the
Energy Lakehouse Platform.

Apache Superset has been selected as the Business Intelligence platform because
it is Open Source, web-based and integrates with SQL query engines such as
Trino.

The intended analytical access path is:

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

Apache Superset does not access Bronze data directly and does not depend on the
Apache Spark processing engine.

The visualization layer is designed around the final analytical products
already implemented in Gold.

The principal analytical grains are:

```text
Province × hour
```

for integrated meteorological and electricity-generation analysis, and:

```text
Autonomous Community × month
```

for installed-capacity analysis.

The Gold processing layer is implemented and queryable through Trino.

Final dashboard construction and visualization validation in Apache Superset
remain a downstream implementation stage.

---

## 2. Business Intelligence Architecture

The Business Intelligence architecture separates data processing from
interactive querying and visualization.

The responsibilities are:

```text
Apache Spark / PySpark
→ analytical transformation

Apache Iceberg
→ managed Gold tables

MinIO
→ physical object storage

Trino
→ analytical SQL access

Apache Superset
→ dashboards and visualization
```

The architecture is therefore:

```text
              PROCESSING

Silver
   │
   ▼
 Spark
   │
   ▼
 Gold
   │
   ▼
Apache Iceberg


              ANALYTICS

Apache Iceberg Gold
        │
        ▼
      Trino
        │
        ▼
Apache Superset
```

This design prevents interactive visualization workloads from being coupled
directly to Spark.

---

## 3. Gold Analytical Contract

Apache Superset is intended to consume the curated Gold model rather than
reconstruct analytical logic from Silver source datasets.

The current physical Gold model contains exactly four tables:

```text
gold_fact_province_hourly
gold_fact_installed_capacity_monthly
gold_dim_geography
gold_dim_time
```

These tables constitute the analytical contract between the Data Engineering
and Business Intelligence layers.

Transformation logic such as:

- geographical normalization;
- meteorological aggregation;
- energy preparation;
- source fallback;
- weather and energy integration;
- temporal normalization;

belongs to the Gold processing layer rather than to individual charts or
dashboard definitions.

---

## 4. Main Analytical Dataset

The principal dataset for visualization is:

```text
gold_fact_province_hourly
```

Its grain is:

```text
Province × hour
```

It combines meteorological information with hourly electricity-generation
metrics.

The geographical hierarchy available for analysis includes:

```text
Autonomous Community
    │
    ▼
Province
```

The table also provides temporal identifiers that can be associated with the
Gold time dimension.

---

## 5. Meteorological Analysis

The principal hourly Gold fact contains meteorological variables including:

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

The visualization layer can therefore support analyses such as:

- temperature evolution;
- precipitation evolution;
- humidity evolution;
- wind-speed comparison;
- solar-radiation evolution;
- geographical comparison between provinces;
- temporal comparison of meteorological conditions.

For selected variables, Gold also preserves the effective source used through:

```text
temperature_source
humidity_source
precipitation_source
```

This allows source provenance to remain visible if required during analytical
exploration.

---

## 6. Electricity-Generation Analysis

The hourly Gold fact contains electricity-generation metrics including:

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

Possible analytical views include:

- total generation evolution;
- generation by technology;
- renewable-generation evolution;
- comparison between provinces;
- technology mix by period;
- comparison between meteorological variables and related generation metrics.

The current analytical scope does **not** include electricity demand or market
prices.

---

## 7. Integrated Meteorological and Energy Analysis

One of the principal objectives of the project is to explore relationships
between weather conditions and electricity generation.

The integration is already performed in Gold rather than dynamically in
Superset.

The principal analytical model is:

```text
Province
+
Hour
+
Meteorological metrics
+
Electricity-generation metrics
```

This enables analyses such as:

```text
wind speed
vs.
wind generation
```

```text
solar radiation
vs.
solar photovoltaic generation
```

```text
temperature / precipitation
vs.
changes in generation mix
```

These visualizations represent analytical relationships and should not
automatically be interpreted as demonstrating causality.

---

## 8. Installed-Capacity Analysis

The second analytical fact is:

```text
gold_fact_installed_capacity_monthly
```

Its grain is:

```text
Autonomous Community × month
```

The table contains installed-capacity metrics expressed in MW, including:

```text
hydraulic_installed_capacity_mw
wind_installed_capacity_mw
solar_photovoltaic_installed_capacity_mw
solar_thermal_installed_capacity_mw
renewable_total_installed_capacity_mw
nuclear_installed_capacity_mw
coal_installed_capacity_mw
combined_cycle_installed_capacity_mw
other_renewables_installed_capacity_mw
```

Possible analytical views include:

- installed capacity by technology;
- comparison between Autonomous Communities;
- renewable versus non-renewable capacity;
- monthly capacity evolution when multiple months are available.

Installed capacity remains at Autonomous Community level.

It must not be visually presented as province-level information.

---

## 9. Proposed Dashboard Structure

The visualization layer can be organized into a small number of dashboards
aligned with the final Gold model.

### 9.1 Energy and Weather Overview

Main dataset:

```text
gold_fact_province_hourly
```

Possible contents:

- selected period;
- selected province;
- total electricity generation;
- average temperature;
- average wind speed;
- average solar radiation;
- generation evolution by technology;
- weather evolution over time.

---

### 9.2 Weather and Renewable Generation

Main dataset:

```text
gold_fact_province_hourly
```

Possible analyses:

- wind speed versus wind generation;
- solar radiation versus photovoltaic generation;
- province comparison;
- hourly temporal evolution;
- scatter plots for exploratory relationships.

This dashboard directly supports the principal analytical objective of the
project.

---

### 9.3 Installed Capacity

Main dataset:

```text
gold_fact_installed_capacity_monthly
```

Possible contents:

- installed capacity by Autonomous Community;
- installed capacity by technology;
- renewable installed capacity;
- comparison of generation technologies;
- monthly evolution when sufficient temporal coverage exists.

---

## 10. Filtering Strategy

The visualization layer should allow users to reduce the analytical scope
without reproducing business transformations.

Relevant filters include:

```text
date / time range
autonomous community
province
generation technology
```

The available geographical filter depends on the fact table being consumed.

For:

```text
gold_fact_province_hourly
```

both Autonomous Community and Province can be used.

For:

```text
gold_fact_installed_capacity_monthly
```

the valid analytical geography is Autonomous Community.

---

## 11. Temporal Analysis

The platform contains:

```text
gold_dim_time
```

to support consistent temporal analysis.

The visualization layer can use temporal attributes for:

- chronological filtering;
- hourly analysis;
- daily grouping;
- monthly grouping where analytically meaningful;
- trend analysis.

The timestamp of the principal hourly fact is:

```text
gold_timestamp
```

and the monthly installed-capacity fact uses:

```text
year_month
gold_month_timestamp
```

Aggregation performed by the visualization layer must respect the meaning and
unit of the underlying metrics.

---

## 12. Geographical Analysis

The geographical analytical dimension is:

```text
gold_dim_geography
```

The final model allows analysis using the hierarchy:

```text
Autonomous Community
        │
        ▼
      Province
```

for the Province × hour fact.

Potential visualizations include:

- maps by province;
- ranked province comparisons;
- Autonomous Community filters;
- province-level meteorological comparisons;
- province-level electricity-generation comparisons.

Installed capacity remains at Autonomous Community level and must not be
disaggregated visually into provinces unless an actual province-level source
exists.

---

## 13. KPI Strategy

KPIs should be calculated only from metrics already supported by the Gold
tables.

Potential KPIs include:

```text
Total generation
Wind generation
Solar photovoltaic generation
Hydraulic generation
Renewable generation metrics
Average temperature
Average wind speed
Average solar radiation
Installed renewable capacity
Installed capacity by technology
```

KPI definitions must preserve the distinction between:

```text
MW
```

and:

```text
MWh
```

Installed capacity is expressed in MW.

Hourly ESIOS generation metrics represented in the main analytical fact are
expressed in MWh according to their validated analytical interpretation.

No KPI for demand or electricity market price belongs to the current project
scope.

---

## 14. NULL Handling

The Gold analytical model intentionally preserves missing source information.

Because:

```text
gold_fact_province_hourly
```

uses a full outer integration between weather and energy blocks, a row may
contain:

```text
weather + energy
weather only
energy only
```

Apache Superset visualizations must therefore not automatically convert missing
values into zero unless zero has an explicit analytical meaning for the
specific metric.

For example:

```text
NULL energy value
≠
0 MWh generation
```

The same principle applies to meteorological metrics.

---

## 15. Data Quality and Visualization

The visualization layer should expose curated information but must not conceal
the real coverage characteristics of the source data.

Where appropriate, dashboard design should make it possible to distinguish:

- periods containing both weather and energy;
- periods containing only weather;
- periods containing only energy;
- missing source observations.

This is particularly important for exploratory analysis because source
availability can vary over time.

The Gold layer already preserves these differences rather than fabricating
complete coverage.

---

## 16. Trino as SQL Access Layer

Trino is the interface between Apache Superset and Apache Iceberg.

The analytical path is:

```text
Apache Superset
       │
       ▼
     Trino
       │
       ▼
Apache Iceberg
       │
       ▼
     MinIO
```

Trino has already been validated against the current Gold namespace.

The four Gold tables are queryable through the Iceberg catalog:

```text
gold_dim_geography
gold_dim_time
gold_fact_installed_capacity_monthly
gold_fact_province_hourly
```

This confirms that the required SQL consumption layer exists before the
dashboard implementation stage.

---

## 17. Processing and Visualization Separation

Analytical logic should remain centralized in the Data Engineering pipeline.

The separation is:

```text
Spark / Gold
→ integration and analytical transformations

Trino
→ SQL querying

Superset
→ visualization
```

Superset should not become responsible for:

- source normalization;
- geographical mapping;
- AEMET/Open-Meteo fallback;
- ESIOS normalization;
- cross-source joins;
- duplicate resolution.

Keeping these transformations upstream ensures that every visualization
consumes the same analytical definitions.

---

## 18. Validated Gold Data Available for Visualization

The current Gold namespace contains:

```text
gold_dim_geography = 71 rows
gold_dim_time = 158 rows
gold_fact_installed_capacity_monthly = 19 rows
gold_fact_province_hourly = 8147 rows
```

The Province × hour fact contains:

```text
8100 rows with weather information
6768 rows with energy information
6721 rows with both weather and energy
```

and:

```text
0 duplicate Province × hour keys
```

The installed-capacity fact contains:

```text
19 rows
0 duplicate Autonomous Community × month keys
```

Real integrated Gold records have been queried successfully through Trino.

The analytical datasets required by the visualization layer are therefore
available.

---

## 19. Current Visualization Status

The current implementation status is:

```text
Gold analytical model
= VALIDATED

Gold availability through Trino
= VALIDATED

Superset service infrastructure
= AVAILABLE

Final Superset datasets
= PENDING IMPLEMENTATION

Final charts and dashboards
= PENDING IMPLEMENTATION

Final visualization validation
= PENDING
```

Therefore, this document describes the validated analytical contract and the
intended visualization design.

It must not be interpreted as evidence that the final dashboards have already
been implemented.

---

## 20. Design Principles

The visualization layer follows these principles.

### Gold-only analytical consumption

Business Intelligence consumes curated Gold products rather than raw or
intermediate source datasets.

### Processing/query separation

Spark performs transformations.

Trino provides SQL access.

Superset performs visualization.

### Grain awareness

Province × hour and Autonomous Community × month are treated as different
analytical grains.

### Unit awareness

MW and MWh are not treated as equivalent units.

### NULL preservation

Missing source information is not silently converted into zero.

### Minimal dashboard logic

Complex transformations remain in Gold rather than being duplicated inside
Superset.

### Traceability

Analytical results remain connected to the curated Gold datasets and their
source semantics.

### Interactive exploration

Users can filter and compare time periods, provinces and Autonomous Communities
according to the grain of each analytical product.

### Open Source analytics

The visualization stack remains entirely based on Open Source technologies.

---

## 21. Expected Final Visualization Flow

The final visualization flow is:

```text
AEMET ─────────────┐
Open-Meteo ────────┤
REE / ESIOS ───────┼──► Bronze
CNIG / IGN ────────┘
                          │
                          ▼
                       Silver
                          │
                          ▼
                        Gold
                     4 tables
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

The visualization stage therefore completes the analytical path without
duplicating ingestion or Lakehouse-processing logic.