# Gold Layer Design

## 1. Purpose

This document defines the approved design of the **Gold layer** for the
**Energy Lakehouse Platform**.

The Gold layer is built exclusively from previously validated Silver
data. It must preserve the actual semantics of each source, the
available temporal and geographical granularities, the approved
aggregation rules, the distinction between missing observations and
explicit zero values, the original signs of energy measurements,
Silver-to-Gold traceability, and load idempotency.

Gold does not enforce a single universal grain. Different analytical
products are defined according to the temporal and geographical
granularity that can be supported by the validated source data.

------------------------------------------------------------------------

## 2. Approved Analytical Use Cases

  ----------------------------------------------------------------------------
  ID                      Analytical question          Scope
  ----------------------- ---------------------------- -----------------------
  CU01                    What relationship exists     Wind ↔ wind generation
                          between wind speed and wind  
                          power generation?            

  CU02                    Which wind height, 80 m or   Wind ↔ wind generation
                          120 m, shows a stronger      
                          relationship with wind power 
                          generation?                  

  CU03                    How does wind power          Wind ↔ wind generation
                          generation change across     
                          different wind-speed ranges? 

  CU04                    Is there a relationship      Wind direction ↔ wind
                          between wind direction and   generation
                          wind power generation?       

  CU05                    What relationship exists     Radiation ↔ solar
                          between solar radiation and  generation
                          solar photovoltaic           
                          generation?                  

  CU06                    What relationship exists     DNI ↔ solar generation
                          between                      
                          `direct_normal_irradiance`   
                          and solar photovoltaic       
                          generation?                  

  CU07                    How does solar photovoltaic  Radiation ↔ solar
                          generation change across     generation
                          different solar-radiation    
                          levels?                      

  CU08                    What relationship exists     Temperature ↔ demand
                          between temperature and      
                          electricity demand?          

  CU09                    How does electricity demand  Temperature ↔ demand
                          vary across different        
                          temperature ranges?          

  CU10                    What relationship exists     Precipitation ↔ hydro
                          between precipitation and    
                          hydroelectric generation?    

  CU11                    Are there territorial        Territorial analysis
                          differences in the           
                          relationship between weather 
                          conditions and renewable     
                          generation?                  

  CU12                    How does the electricity     Energy mix
                          generation mix by technology 
                          evolve over time?            

  CU13                    How does installed capacity  Installed capacity
                          by technology evolve by      
                          autonomous community?        

  CU14                    Which autonomous communities Territorial capacity
                          concentrate the highest      
                          installed capacity for each  
                          technology?                  

  CU15                    What relationship exists     Capacity ↔ generation
                          between installed capacity   
                          and observed generation for  
                          a technology?                
  ----------------------------------------------------------------------------

------------------------------------------------------------------------

## 3. Approved Gold Analytical Products

  -----------------------------------------------------------------------
  Product           Geography         Temporal grain    Content
  ----------------- ----------------- ----------------- -----------------
  Gold 1            Province          1 hour            Weather + hourly
                                                        ESIOS energy

  Gold 2            Autonomous        Month             Installed
                    Community                           capacity

  Gold 3            Spain/Peninsula   15 minutes        Weather + ESIOS
                    depending on                        high-frequency
                    indicator                           energy aggregated
                                                        to 15 minutes

  Gold 4            Spain/Peninsula   5 minutes         High-frequency
                    depending on                        ESIOS energy data
                    indicator                           
  -----------------------------------------------------------------------

### 3.1 Geographical rule

The preferred Gold geographical level is **Province whenever the source
can validly support province-level data**.

When a source is only available at a higher geographical level, its
actual geography is preserved without artificial disaggregation.

The following scopes remain distinct:

`Province ≠ Autonomous Community ≠ Spain ≠ Peninsula`

CNIG is the canonical geographical master for provinces and autonomous
communities.

### 3.2 Temporal rule

Gold does not have a single mandatory temporal grain.

The approved grains are:

-   5 minutes;
-   15 minutes;
-   1 hour;
-   month.

Finer-grained observations must not be fabricated from lower-frequency
sources.

------------------------------------------------------------------------

## 4. Physical Gold Table Inventory

The Gold layer consists of **6 physical tables**.

### Fact tables

1.  `gold_fact_province_hourly`
2.  `gold_fact_installed_capacity_monthly`
3.  `gold_fact_country_15min`
4.  `gold_fact_country_5min`

### Dimensions

5.  `gold_dim_time`
6.  `gold_dim_geography`

Energy technology is not materialized as a seventh physical dimension.
Technologies are represented by explicit metrics in the corresponding
fact tables, preserving the approved fact-table grains.

------------------------------------------------------------------------

# 5. `gold_fact_province_hourly`

## 5.1 Purpose

Main analytical table for integrated weather and electricity-generation
analysis at **Province × hour** grain.

It supports analyses such as:

-   wind conditions versus wind generation;
-   solar radiation versus photovoltaic generation;
-   DNI versus solar thermal generation;
-   precipitation versus hydroelectric generation;
-   territorial differences;
-   provincial electricity generation mix.

## 5.2 Silver Sources

-   `silver_aemet_current_observations`
-   `silver_open_meteo_hourly`
-   `silver_open_meteo_15min`
-   `silver_esios_energy_hourly`
-   Silver CNIG tables

## 5.3 Physical Schema

  Column                                       Type
  -------------------------------------------- -------------
  `gold_timestamp`                             `TIMESTAMP`
  `geography_key`                              `STRING`
  `province_code`                              `STRING`
  `province_name`                              `STRING`
  `autonomous_community_code`                  `STRING`
  `autonomous_community_name`                  `STRING`
  `temperature`                                `DOUBLE`
  `humidity`                                   `DOUBLE`
  `precipitation`                              `DOUBLE`
  `wind_speed_80m`                             `DOUBLE`
  `wind_direction_80m`                         `DOUBLE`
  `wind_speed_120m`                            `DOUBLE`
  `wind_direction_120m`                        `DOUBLE`
  `solar_radiation`                            `DOUBLE`
  `direct_normal_irradiance`                   `DOUBLE`
  `wind_generation_mwh`                        `DOUBLE`
  `solar_photovoltaic_generation_mwh`          `DOUBLE`
  `solar_thermal_generation_mwh`               `DOUBLE`
  `hydraulic_generation_mwh`                   `DOUBLE`
  `nuclear_generation_mwh`                     `DOUBLE`
  `combined_cycle_generation_mwh`              `DOUBLE`
  `gas_natural_steam_turbine_generation_mwh`   `DOUBLE`
  `gas_natural_cogeneration_mwh`               `DOUBLE`
  `coal_generation_mwh`                        `DOUBLE`
  `other_renewables_generation_mwh`            `DOUBLE`
  `total_generation_mwh`                       `DOUBLE`
  `temperature_source`                         `STRING`
  `humidity_source`                            `STRING`
  `precipitation_source`                       `STRING`
  `gold_created_at`                            `TIMESTAMP`

## 5.4 Natural Key

`(province_code, gold_timestamp)`

Exactly one row must exist for each province and hour.

## 5.5 Grain

-   Temporal: 1 hour.
-   Geographical: Province.

The autonomous community is retained as a hierarchical attribute and
does not change the physical grain.

## 5.6 Iceberg Partitioning

`day(gold_timestamp)`

Province is not added to the partition specification.

## 5.7 Weather Transformations

### Temperature

Primary source:

`AVG(AEMET.ta)` across the available stations in the province.

Fallback when that specific AEMET measurement is unavailable:

`AVG(Open-Meteo.temperature_2m)` across the available points in the
province.

### Humidity

Primary source:

`AVG(AEMET.hr)` across the available stations in the province.

Fallback to Open-Meteo when the AEMET humidity measurement is
unavailable.

### Precipitation

Primary source:

`AVG(AEMET.prec)` across the available stations in the province.

Fallback:

`AVG(Open-Meteo precipitation)` across the available points in the
province.

The unit is millimetres and the source values represent precipitation
accumulated during the hourly interval.

### Wind speed at 80 m and 120 m

Source: `silver_open_meteo_15min`.

Per point:

`4 × 15-minute observations → AVG → hourly value`

Then spatially:

`hourly point values → AVG → Province × hour`

This applies to:

-   `wind_speed_80m`
-   `wind_speed_120m`

### Wind direction at 80 m and 120 m

Arithmetic averages must not be used.

Per point:

`4 × 15-minute directions → circular mean → hourly direction`

Then spatially:

`hourly point directions → circular mean → Province × hour`

This applies to:

-   `wind_direction_80m`
-   `wind_direction_120m`

The result is expressed in degrees from 0 to 360.

### Solar radiation

Source:

`silver_open_meteo_hourly.shortwave_radiation`

Transformation:

`AVG(points available in the province)`

Gold metric:

`solar_radiation`

### Direct Normal Irradiance

Source:

`silver_open_meteo_hourly.direct_normal_irradiance`

Transformation:

`AVG(points available in the province)`

Gold metric:

`direct_normal_irradiance`

### Weather fallback rule

The AEMET → Open-Meteo fallback applies **only** to:

-   `temperature`
-   `humidity`
-   `precipitation`

It is applied independently for each metric, not to the entire row.

The selected source is retained in:

-   `temperature_source`
-   `humidity_source`
-   `precipitation_source`

## 5.8 Hourly Energy Transformations

    ESIOS indicator ID Gold metric
  -------------------- --------------------------------------------
                  1159 `wind_generation_mwh`
                  1161 `solar_photovoltaic_generation_mwh`
                  1162 `solar_thermal_generation_mwh`
                 10035 `hydraulic_generation_mwh`
                  1153 `nuclear_generation_mwh`
                  1156 `combined_cycle_generation_mwh`
                  1158 `gas_natural_steam_turbine_generation_mwh`
                  1164 `gas_natural_cogeneration_mwh`
                 10036 `coal_generation_mwh`
                 10041 `other_renewables_generation_mwh`
                 10043 `total_generation_mwh`

Excluded from Gold:

-   `10195`
-   `1193`
-   `10267`

For the hourly grain:

`metric_mwh = value`

No `AVG`, no `SUM`, and no unit conversion is used to construct the
hourly observation.

For periods greater than one hour:

`SUM(MWh)`

`10035` preserves positive, zero, and negative values.

`10043` is retained as the official ESIOS total generation value and is
not reconstructed by summing the selected technologies.

------------------------------------------------------------------------

# 6. `gold_fact_installed_capacity_monthly`

## 6.1 Purpose

Analyze installed capacity by technology and autonomous community while
preserving the actual monthly ESIOS grain.

## 6.2 Silver Sources

-   `silver_esios_installed_capacity_monthly`
-   Silver CNIG tables

## 6.3 Physical Schema

  Column                                       Type
  -------------------------------------------- -------------
  `year_month`                                 `STRING`
  `gold_month_timestamp`                       `TIMESTAMP`
  `source_timestamp`                           `TIMESTAMP`
  `geography_key`                              `STRING`
  `autonomous_community_code`                  `STRING`
  `autonomous_community_name`                  `STRING`
  `esios_geo_id`                               `BIGINT`
  `hydraulic_installed_capacity_mw`            `DOUBLE`
  `wind_installed_capacity_mw`                 `DOUBLE`
  `solar_photovoltaic_installed_capacity_mw`   `DOUBLE`
  `solar_thermal_installed_capacity_mw`        `DOUBLE`
  `renewable_total_installed_capacity_mw`      `DOUBLE`
  `nuclear_installed_capacity_mw`              `DOUBLE`
  `coal_installed_capacity_mw`                 `DOUBLE`
  `combined_cycle_installed_capacity_mw`       `DOUBLE`
  `other_renewables_installed_capacity_mw`     `DOUBLE`
  `gold_created_at`                            `TIMESTAMP`

## 6.4 Natural Key

`(autonomous_community_code, year_month)`

Exactly one row per autonomous community and month.

## 6.5 Grain

-   Temporal: Month.
-   Geographical: Autonomous Community.

Installed capacity must not be artificially disaggregated to province
level.

## 6.6 Iceberg Partitioning

`year_month`

No additional partitioning by autonomous community.

## 6.7 ESIOS Transformations

    ESIOS indicator ID Gold metric
  -------------------- --------------------------------------------
                  1475 `hydraulic_installed_capacity_mw`
                  1485 `wind_installed_capacity_mw`
                  1486 `solar_photovoltaic_installed_capacity_mw`
                  1487 `solar_thermal_installed_capacity_mw`
                 10302 `renewable_total_installed_capacity_mw`
                  1477 `nuclear_installed_capacity_mw`
                  1478 `coal_installed_capacity_mw`
                  1483 `combined_cycle_installed_capacity_mw`
                  1488 `other_renewables_installed_capacity_mw`

For all metrics:

`installed_capacity_mw = value`

Rules:

-   installed capacity remains in MW;
-   no conversion to MWh;
-   no temporal `SUM(MW)` across months;
-   `year_month` is the analytical temporal key;
-   the original ESIOS timestamp is retained for traceability;
-   `esios_geo_id` is retained;
-   autonomous communities are normalized against CNIG;
-   the configurable +1-hour ESIOS gap is not automatically applied to
    monthly installed capacity.

### Official renewable total

Indicator `10302` is used directly as:

`renewable_total_installed_capacity_mw`

It is the official ESIOS renewable total used by this design.

It is not reconstructed by summing hydraulic, wind, solar photovoltaic,
solar thermal, and other renewables.

------------------------------------------------------------------------

# 7. `gold_fact_country_15min`

## 7.1 Purpose

Integrated high-frequency Gold product for analyzing national weather
and energy at 15-minute intervals while preserving the distinction
between Spain and Peninsula scopes.

## 7.2 Silver Sources

-   `silver_open_meteo_15min`
-   `silver_esios_power_5min`
-   Silver CNIG tables

Weather:

`point → province → Spain`

Energy:

`ESIOS 5 min → MW-to-MWh interval conversion → three-interval aggregation → 15 min`

## 7.3 Physical Schema

  Column                                             Type
  -------------------------------------------------- -------------
  `gold_timestamp`                                   `TIMESTAMP`
  `geography_key`                                    `STRING`
  `geography_level`                                  `STRING`
  `geography_name`                                   `STRING`
  `temperature`                                      `DOUBLE`
  `humidity`                                         `DOUBLE`
  `precipitation`                                    `DOUBLE`
  `wind_speed_80m`                                   `DOUBLE`
  `wind_direction_80m`                               `DOUBLE`
  `wind_speed_120m`                                  `DOUBLE`
  `wind_direction_120m`                              `DOUBLE`
  `solar_radiation`                                  `DOUBLE`
  `direct_normal_irradiance`                         `DOUBLE`
  `real_demand_energy_mwh_15min`                     `DOUBLE`
  `wind_generation_energy_mwh_15min`                 `DOUBLE`
  `nuclear_generation_energy_mwh_15min`              `DOUBLE`
  `coal_generation_energy_mwh_15min`                 `DOUBLE`
  `combined_cycle_generation_energy_mwh_15min`       `DOUBLE`
  `hydraulic_generation_energy_mwh_15min`            `DOUBLE`
  `solar_photovoltaic_generation_energy_mwh_15min`   `DOUBLE`
  `solar_thermal_generation_energy_mwh_15min`        `DOUBLE`
  `renewable_thermal_generation_energy_mwh_15min`    `DOUBLE`
  `cogeneration_waste_generation_energy_mwh_15min`   `DOUBLE`
  `pumping_consumption_energy_mwh_15min`             `DOUBLE`
  `gold_created_at`                                  `TIMESTAMP`

## 7.4 Natural Key

`(geography_key, gold_timestamp)`

Exactly one row per compatible geographical scope and 15-minute
interval.

## 7.5 Grain

-   Temporal: 15 minutes.
-   Geographical: Spain/Peninsula according to the actual indicator
    scope.

Spain and Peninsula are not treated as equivalent.

## 7.6 Iceberg Partitioning

`day(gold_timestamp)`

No additional geographical partition.

## 7.7 National Weather Aggregation

Open-Meteo already provides the required 15-minute temporal grain. No
additional temporal aggregation is performed.

For scalar variables:

`points → AVG by province → AVG across provinces → Spain × 15 min`

For wind directions:

`points → circular mean by province → circular mean across provinces → Spain × 15 min`

A direct national average across all points must not replace this
approved two-stage spatial aggregation.

## 7.8 Energy Transformation: 5 min → 15 min

Selected ESIOS indicators:

-   `1293`
-   `2038`
-   `2039`
-   `2040`
-   `2041`
-   `2042`
-   `2044`
-   `2045`
-   `2046`
-   `2051`
-   `2065`

Indicator `10004` remains excluded.

The original 5-minute observation represents power:

`power_mw = value`

Energy for each real 5-minute interval is:

`energy_mwh_5min = power_mw × (5 / 60)`

Energy for the 15-minute interval is:

`energy_mwh_15min = SUM(three energy_mwh_5min intervals)`

`SUM(power_mw)` is prohibited.

Original ESIOS signs are preserved.

------------------------------------------------------------------------

# 8. `gold_fact_country_5min`

## 8.1 Purpose

Preserve the maximum validated energy frequency: **Spain/Peninsula × 5
minutes**.

No weather source participates in this product.

## 8.2 Silver Source

-   `silver_esios_power_5min`

## 8.3 Physical Schema

  Column                                            Type
  ------------------------------------------------- -------------
  `gold_timestamp`                                  `TIMESTAMP`
  `geography_key`                                   `STRING`
  `geography_level`                                 `STRING`
  `geography_name`                                  `STRING`
  `esios_geo_id`                                    `BIGINT`
  `real_demand_mw`                                  `DOUBLE`
  `wind_generation_power_mw`                        `DOUBLE`
  `nuclear_generation_power_mw`                     `DOUBLE`
  `coal_generation_power_mw`                        `DOUBLE`
  `combined_cycle_generation_power_mw`              `DOUBLE`
  `hydraulic_generation_power_mw`                   `DOUBLE`
  `solar_photovoltaic_generation_power_mw`          `DOUBLE`
  `solar_thermal_generation_power_mw`               `DOUBLE`
  `renewable_thermal_generation_power_mw`           `DOUBLE`
  `cogeneration_waste_generation_power_mw`          `DOUBLE`
  `pumping_consumption_power_mw`                    `DOUBLE`
  `real_demand_energy_mwh_5min`                     `DOUBLE`
  `wind_generation_energy_mwh_5min`                 `DOUBLE`
  `nuclear_generation_energy_mwh_5min`              `DOUBLE`
  `coal_generation_energy_mwh_5min`                 `DOUBLE`
  `combined_cycle_generation_energy_mwh_5min`       `DOUBLE`
  `hydraulic_generation_energy_mwh_5min`            `DOUBLE`
  `solar_photovoltaic_generation_energy_mwh_5min`   `DOUBLE`
  `solar_thermal_generation_energy_mwh_5min`        `DOUBLE`
  `renewable_thermal_generation_energy_mwh_5min`    `DOUBLE`
  `cogeneration_waste_generation_energy_mwh_5min`   `DOUBLE`
  `pumping_consumption_energy_mwh_5min`             `DOUBLE`
  `gold_created_at`                                 `TIMESTAMP`

## 8.4 Natural Key

`(geography_key, gold_timestamp)`

Exactly one row per geographical scope and 5-minute timestamp.

## 8.5 Grain

-   Temporal: 5 minutes.
-   Geographical: Spain/Peninsula.

## 8.6 Iceberg Partitioning

`day(gold_timestamp)`

## 8.7 Selected Indicators

    ESIOS indicator ID Geography   Gold power metric
  -------------------- ----------- ------------------------------------------
                  1293 Peninsula   `real_demand_mw`
                  2038 Spain       `wind_generation_power_mw`
                  2039 Spain       `nuclear_generation_power_mw`
                  2040 Spain       `coal_generation_power_mw`
                  2041 Spain       `combined_cycle_generation_power_mw`
                  2042 Spain       `hydraulic_generation_power_mw`
                  2044 Spain       `solar_photovoltaic_generation_power_mw`
                  2045 Spain       `solar_thermal_generation_power_mw`
                  2046 Spain       `renewable_thermal_generation_power_mw`
                  2051 Spain       `cogeneration_waste_generation_power_mw`
                  2065 Spain       `pumping_consumption_power_mw`

Indicator `10004` remains excluded.

The observations already exist at 5-minute frequency:

`power_mw = value`

No transformation is used to create the 5-minute frequency.

Energy associated with each interval is derived as:

`energy_mwh_5min = power_mw × (5 / 60)`

equivalently:

`energy_mwh_5min = power_mw / 12`

This derivation does not change the temporal grain.

### Sign preservation

Original ESIOS signs are retained, particularly for:

-   `2042` --- hydraulic generation;
-   `2065` --- pumping consumption.

The following transformations are prohibited:

-   `ABS(value)`;
-   artificial sign inversion;
-   automatic compensation between hydraulic generation and pumping
    consumption.

Derived MWh values retain the sign of their source power value.

------------------------------------------------------------------------

# 9. `gold_dim_time`

## 9.1 Purpose

Common temporal dimension supporting all four approved Gold temporal
grains:

-   `FIVE_MINUTES`
-   `FIFTEEN_MINUTES`
-   `HOUR`
-   `MONTH`

A single physical dimension is used rather than separate time dimensions
per grain.

## 9.2 Physical Schema

  Column              Type
  ------------------- -------------
  `time_key`          `STRING`
  `time_grain`        `STRING`
  `gold_timestamp`    `TIMESTAMP`
  `date`              `DATE`
  `year`              `INT`
  `month`             `INT`
  `year_month`        `STRING`
  `day`               `INT`
  `day_of_week`       `INT`
  `hour`              `INT`
  `minute`            `INT`
  `gold_created_at`   `TIMESTAMP`

For monthly members, non-applicable hourly attributes may remain `NULL`.

No artificial hour is created to represent a month.

## 9.3 Keys

`time_key` must be:

-   deterministic;
-   stable;
-   reproducible;
-   unique;
-   independent of auto-incrementing IDs.

Conceptually:

`time_grain + temporal value`

Natural keys:

-   `(time_grain, gold_timestamp)` for submonthly grains;
-   `(time_grain, year_month)` for `MONTH`.

The literal serialization format may be fixed during implementation
while preserving these properties.

## 9.4 Partitioning

No Iceberg partitioning.

## 9.5 Attribute Derivation

For submonthly records:

`gold_timestamp → date, year, month, year_month, day, day_of_week, hour, minute`

For monthly records:

`year_month → year, month`

The dimension does not apply the ESIOS temporal gap itself.

------------------------------------------------------------------------

# 10. `gold_dim_geography`

## 10.1 Purpose

Common geographical dimension representing only:

-   `PROVINCE`
-   `AUTONOMOUS_COMMUNITY`
-   `COUNTRY`
-   `PENINSULA`

## 10.2 Sources

For Province and Autonomous Community:

Silver CNIG is the canonical geographical master.

For national scopes:

-   Spain;
-   Peninsula.

No artificial geography is created.

## 10.3 Physical Schema

  Column                        Type
  ----------------------------- -------------
  `geography_key`               `STRING`
  `geography_level`             `STRING`
  `geography_code`              `STRING`
  `geography_name`              `STRING`
  `province_code`               `STRING`
  `province_name`               `STRING`
  `autonomous_community_code`   `STRING`
  `autonomous_community_name`   `STRING`
  `country_code`                `STRING`
  `country_name`                `STRING`
  `esios_geo_id`                `BIGINT`
  `gold_created_at`             `TIMESTAMP`

## 10.4 Keys

`geography_key` must be deterministic, stable, reproducible, and unique.

Conceptually:

`geography_level + geography_code`

Natural key:

`(geography_level, geography_code)`

## 10.5 Territorial Hierarchy

Where applicable:

`Spain → Autonomous Community → Province`

For `PROVINCE`, province, autonomous community, and country attributes
may be populated.

For `AUTONOMOUS_COMMUNITY`, province attributes remain `NULL`.

For `COUNTRY`, only applicable national attributes are populated.

For `PENINSULA`, only attributes applicable to the Peninsula scope are
populated.

Lower geographical levels must not be filled artificially.

## 10.6 Spain and Peninsula

They are materialized as distinct members:

-   `COUNTRY → Spain`
-   `PENINSULA → Peninsula`

Therefore:

`Spain ≠ Peninsula`

Validated ESIOS geography IDs include:

-   Spain → `esios_geo_id = 3`
-   Peninsula → `esios_geo_id = 8741`

When no real ESIOS ID exists:

`esios_geo_id = NULL`

An ID must never be fabricated.

## 10.7 Partitioning

No Iceberg partitioning.

------------------------------------------------------------------------

# 11. ESIOS Temporal Alignment

For the ESIOS time series to which the approved alignment applies:

`gold_timestamp = observation_timestamp + configurable_gap`

Initial approved value:

`configurable_gap = +1 hour`

Requirements:

-   stored in JSON under `config/`;
-   not hardcoded;
-   applied before the corresponding temporal aggregation and
    integration;
-   not automatically applied to monthly installed capacity;
-   not applied by `gold_dim_time` itself.

The approved initial offset originates from the validated alignment
analysis in which indicator `1161` showed improved correlation with
Open-Meteo using the selected offset across 47/47 provinces.

------------------------------------------------------------------------

# 12. Missing Observations, NULLs, Zeros, and Gaps

The common Gold rule is:

`existing observation with published value = 0 → 0`

`missing observation → NULL / absence`

Therefore:

**NULL ≠ 0**

Gold must not automatically:

-   interpolate gaps;
-   fabricate rows;
-   impute missing observations;
-   replace missing metrics with zero;
-   apply a general `COALESCE(metric, 0)`.

The approved AEMET → Open-Meteo fallback for temperature, humidity, and
precipitation is a specific integration rule and is not arbitrary
imputation.

------------------------------------------------------------------------

# 13. Integration Rules

The logical Silver → Gold integration order is:

1.  Read Silver.
2.  Normalize geography.
3.  Apply ESIOS temporal alignment where applicable.
4.  Apply required temporal aggregations.
5.  Apply required spatial aggregations.
6.  Resolve weather fallback per metric.
7.  Produce intermediate datasets at the target Gold grain.
8.  Validate key uniqueness.
9.  Join only compatible datasets.
10. Persist Gold.

## 13.1 Join protection rule

**Every source must be aggregated to the target Gold grain before
integration.**

For `gold_fact_province_hourly`:

`AEMET stations → Province × hour`

`Open-Meteo points → Province × hour`

`ESIOS → Province × hour`

Only then are the prepared datasets joined by province and hour.

This prevents artificial multiplication such as:

`3 weather stations × 4 Open-Meteo points × 1 ESIOS value`

Individual station and point observations remain in Silver.

------------------------------------------------------------------------

# 14. Dimensions, Relationships, and Cardinalities

The physical Gold dimensions are:

-   `gold_dim_time`
-   `gold_dim_geography`

Relationships follow:

`dimension 1 → N fact`

There are no direct fact-to-fact relationships.

Each fact table must preserve the uniqueness of its own grain:

-   Province × hour;
-   Autonomous Community × month;
-   compatible geographical scope × 15 minutes;
-   Spain/Peninsula × 5 minutes.

Energy technology remains an analytical concept/catalogue represented
through explicit fact-table metrics rather than a physical relationship
that would alter these grains.

------------------------------------------------------------------------

# 15. Logical Model

``` mermaid
flowchart TB
    DT["gold_dim_time"]
    DG["gold_dim_geography"]

    F1["gold_fact_province_hourly<br/>Province × hour<br/>Weather + energy"]
    F2["gold_fact_installed_capacity_monthly<br/>Autonomous Community × month<br/>Installed capacity"]
    F3["gold_fact_country_15min<br/>Spain/Peninsula × 15 min<br/>Weather + energy"]
    F4["gold_fact_country_5min<br/>Spain/Peninsula × 5 min<br/>Energy"]

    DT -->|1:N| F1
    DT -->|1:N| F2
    DT -->|1:N| F3
    DT -->|1:N| F4

    DG -->|1:N| F1
    DG -->|1:N| F2
    DG -->|1:N| F3
    DG -->|1:N| F4
```

------------------------------------------------------------------------

# 16. Gold Load Strategy

## 16.1 Initial Load

All four fact tables perform an initial full build using the complete
Silver range available at execution time.

Flow:

`available Silver → Gold transformations → grain validation → initial full load`

## 16.2 `gold_dim_geography`

Initial complete load from:

`Silver CNIG + Spain + Peninsula`

After validation, the dimension is not rebuilt during normal Gold
executions.

It is updated only through an explicit operation if the official
geographical master is updated in the future.

## 16.3 `gold_dim_time`

Initial generation covers the required Gold temporal range.

Subsequent executions add only new required temporal members.

## 16.4 Incremental Fact Loads

After the initial build, all fact tables use `MERGE` by natural key.

  ------------------------------------------------------------------------------------
  Table                                    MERGE natural key
  ---------------------------------------- -------------------------------------------
  `gold_fact_province_hourly`              `(province_code, gold_timestamp)`

  `gold_fact_installed_capacity_monthly`   `(autonomous_community_code, year_month)`

  `gold_fact_country_15min`                `(geography_key, gold_timestamp)`

  `gold_fact_country_5min`                 `(geography_key, gold_timestamp)`
  ------------------------------------------------------------------------------------

Behavior:

`MATCH → UPDATE`

`NOT MATCH → INSERT`

Blind append is prohibited.

No execution frequency is defined here because no such frequency has
been approved as part of this design.

------------------------------------------------------------------------

# 17. Idempotency, Reprocessing, and Backfills

Idempotency is mandatory.

The same Silver input processed with the same transformation rules must
produce the same logical Gold state:

`same input + same transformations + same natural key = same Gold result`

Reprocessing a period must not create duplicate rows.

Historical ranges can be recalculated and merged without rebuilding the
complete Gold history.

For backfills:

`Silver range → Gold recalculation → validation → MERGE`

The process must ensure that:

-   existing keys are updated;
-   new keys are inserted;
-   duplicates are not introduced;
-   data outside the reprocessed range is not modified accidentally;
-   missing observations are not converted to zero.

------------------------------------------------------------------------

# 18. Gold Data Quality Controls

Gold quality controls must validate both physical integrity and the
semantic correctness of the Silver → Gold transformation.

Core principles:

-   do not invent data;
-   do not convert absence to zero;
-   do not automatically interpolate gaps;
-   do not alter original signs;
-   do not fabricate geographies;
-   do not mix incompatible granularities;
-   do not duplicate measures during joins;
-   do not lose valid Silver coverage because of transformation errors.

## 18.1 Natural Keys

Required duplicate count by natural key:

`0`

Natural keys:

  ------------------------------------------------------------------------------------
  Table                                    Natural key
  ---------------------------------------- -------------------------------------------
  `gold_fact_province_hourly`              `(province_code, gold_timestamp)`

  `gold_fact_installed_capacity_monthly`   `(autonomous_community_code, year_month)`

  `gold_fact_country_15min`                `(geography_key, gold_timestamp)`

  `gold_fact_country_5min`                 `(geography_key, gold_timestamp)`

  `gold_dim_time`                          `time_key`

  `gold_dim_geography`                     `geography_key`
  ------------------------------------------------------------------------------------

Additional dimension uniqueness:

-   `gold_dim_time`: `(time_grain, gold_timestamp)` for submonthly
    grains and `(time_grain, year_month)` for `MONTH`;
-   `gold_dim_geography`: `(geography_level, geography_code)`.

## 18.2 Structural NULL Controls

Required non-null columns include:

### `gold_fact_province_hourly`

-   `province_code`
-   `gold_timestamp`

### `gold_fact_installed_capacity_monthly`

-   `autonomous_community_code`
-   `year_month`

### `gold_fact_country_15min`

-   `geography_key`
-   `gold_timestamp`

### `gold_fact_country_5min`

-   `geography_key`
-   `gold_timestamp`

### `gold_dim_time`

-   `time_key`
-   `time_grain`

### `gold_dim_geography`

-   `geography_key`
-   `geography_level`
-   `geography_code`
-   `geography_name`

Metric NULLs are allowed when they represent genuine source coverage
limitations.

## 18.3 Timestamp Controls

### Hourly fact

`minute = 0`

### 15-minute fact

`minute ∈ {0, 15, 30, 45}`

### 5-minute fact

`minute MOD 5 = 0`

### Monthly fact

`year_month` must be coherent with the represented monthly period and
associated temporal attributes.

The original ESIOS monthly timestamp remains available for traceability.

## 18.4 ESIOS Gap Controls

The configurable ESIOS gap must:

-   come from external configuration;
-   not be hardcoded;
-   be applied only to the applicable time series;
-   be applied before the relevant aggregation/integration;
-   not be automatically applied to monthly installed capacity.

## 18.5 Geography Controls

Valid geographical levels by table:

  ----------------------------------------------------------------------------
  Table                                    Valid geography
  ---------------------------------------- -----------------------------------
  `gold_fact_province_hourly`              `PROVINCE`

  `gold_fact_installed_capacity_monthly`   `AUTONOMOUS_COMMUNITY`

  `gold_fact_country_15min`                `COUNTRY` / `PENINSULA` according
                                           to the data

  `gold_fact_country_5min`                 `COUNTRY` / `PENINSULA` according
                                           to the indicator

  `gold_dim_geography`                     `PROVINCE`, `AUTONOMOUS_COMMUNITY`,
                                           `COUNTRY`, `PENINSULA`
  ----------------------------------------------------------------------------

Province and autonomous-community mappings must use CNIG.

Artificial conversions are prohibited when the source does not support
them, including:

-   Peninsula → Spain;
-   Spain → Peninsula;
-   Spain → Autonomous Community;
-   Spain → Province;
-   Autonomous Community → Province.

## 18.6 Grain Controls

  ----------------------------------------------------------------------------------------
  Table                                    Temporal grain          Geographical grain
  ---------------------------------------- ----------------------- -----------------------
  `gold_fact_province_hourly`              Hour                    Province

  `gold_fact_installed_capacity_monthly`   Month                   Autonomous Community

  `gold_fact_country_15min`                15 minutes              Spain/Peninsula
                                                                   compatible scope

  `gold_fact_country_5min`                 5 minutes               Spain/Peninsula
  ----------------------------------------------------------------------------------------

A real source gap is not the same as an incorrect grain.

Missing timestamps are not fabricated merely to create a complete
calendar series.

## 18.7 Coverage Controls

Coverage must be measurable by:

`period + geography + metric/technology`

Gold must not lose valid Silver observations because of:

-   transformation errors;
-   joins;
-   filters;
-   pivots;
-   geographical normalization.

Real source coverage differences are not automatically treated as
technical errors.

## 18.8 Hourly Energy Metric Controls

For `gold_fact_province_hourly`:

`Gold metric_mwh = corresponding Silver value`

after the approved alignment and integration rules.

The hourly observation is not built using:

-   `AVG`;
-   `SUM`;
-   MW-to-MWh conversion.

## 18.9 Official Total Generation

`total_generation_mwh` from indicator `10043` is retained as the
official ESIOS total.

It is not required to equal a reconstructed sum of the selected
technology metrics.

## 18.10 Sign Controls

Original ESIOS signs must be preserved.

The following are prohibited:

-   `ABS(value)`;
-   artificial sign inversion;
-   unapproved automatic compensation between hydraulic generation and
    pumping consumption.

Derived MWh metrics preserve the source sign.

## 18.11 Installed-Capacity Controls

Installed-capacity metrics remain in MW.

They must not be:

-   automatically converted to MWh;
-   temporally summed across months.

Indicator `10302` is preserved as the official renewable
installed-capacity total.

## 18.12 Wind Aggregation Controls

Wind speed:

`4 × 15 min → AVG per point → AVG across provincial points`

Wind direction:

`4 × 15 min → circular mean per point → circular mean across provincial points`

Arithmetic `AVG(direction)` is not valid.

## 18.13 National Weather Aggregation Controls

For `gold_fact_country_15min`:

`point → province → Spain`

Scalar variables:

`AVG(points by province) → AVG(provinces)`

Directions:

`circular mean(points by province) → circular mean(provinces)`

## 18.14 Energy Integrity: 5 min → 15 min

For each 5-minute observation:

`energy_mwh_5min = power_mw × (5 / 60)`

For each 15-minute interval:

`energy_mwh_15min = SUM(three energy_mwh_5min intervals)`

The implementation must not use:

`SUM(power_mw)`

to represent 15-minute energy.

## 18.15 `gold_fact_country_5min` Integrity

The table preserves both:

`power_mw = Silver value`

and:

`energy_mwh_5min = power_mw × (5 / 60)`

Deriving interval energy does not change the 5-minute grain.

## 18.16 Weather Fallback Controls

For temperature, humidity, and precipitation:

`AEMET available → AEMET`

`AEMET metric unavailable → Open-Meteo`

The fallback is applied independently per metric.

Source traceability must be available through:

-   `temperature_source`
-   `humidity_source`
-   `precipitation_source`

## 18.17 `gold_dim_time` Integrity

Required:

-   unique `time_key`;
-   only `FIVE_MINUTES`, `FIFTEEN_MINUTES`, `HOUR`, `MONTH`;
-   coherent calendar attributes;
-   `month ∈ 1..12`;
-   `hour ∈ 0..23` where applicable;
-   `minute ∈ 0..59` where applicable;
-   no artificial hourly timestamp for monthly members.

## 18.18 `gold_dim_geography` Integrity

Required:

-   unique `geography_key`;
-   unique `(geography_level, geography_code)`;
-   only the four approved geographical levels;
-   valid Province → Autonomous Community hierarchy;
-   Autonomous Community → Spain hierarchy;
-   CNIG-based Province and Autonomous Community data;
-   Spain and Peninsula remain distinct;
-   `esios_geo_id` only when a real mapping exists.

## 18.19 Dimension-to-Fact Integrity

Each fact row must resolve to exactly one compatible member of the
corresponding dimensions.

Conceptually:

`fact.time_key → gold_dim_time.time_key`

`fact.geography_key → gold_dim_geography.geography_key`

A fact must never reference a geography incompatible with its grain.

## 18.20 Idempotency Controls

Re-running exactly the same input must produce:

-   the same logical row count;
-   the same natural keys;
-   the same values;
-   no duplication.

## 18.21 Backfill Controls

Historical reprocessing must verify that:

-   existing keys are updated;
-   new keys are inserted;
-   no duplicates appear;
-   data outside the requested range remains unchanged;
-   missing observations are not converted to zero;
-   the same quality rules apply to normal and backfill loads.

------------------------------------------------------------------------

# 19. Critical Load-Acceptance Controls

A Gold load must not be considered valid if any critical structural
control fails.

At minimum, the following conditions are critical failures:

-   `NULL` natural-key component;
-   duplicate natural keys;
-   geography incompatible with the product;
-   timestamp incompatible with the approved grain;
-   incorrect alteration of ESIOS signs;
-   use of `SUM(MW)` where prohibited;
-   loss of uniqueness during joins;
-   invalid dimension reference.

The following are not automatically failures:

-   a metric containing `NULL` because of genuine missing source
    coverage;
-   a genuine source gap;
-   partial territorial coverage already present in Silver.

Gold quality controls must distinguish transformation errors from real
source limitations.

------------------------------------------------------------------------

# 20. Data Not Promoted to Gold

The following data remain available in Silver but are not promoted to
the current Gold analytical products.

## 20.1 Weather Variables

-   atmospheric pressure;
-   cloud cover;
-   dew point;
-   10 m wind speed;
-   10 m wind direction;
-   wind gusts;
-   direct radiation;
-   diffuse radiation;
-   sunshine duration.

## 20.2 ESIOS Indicators

-   `10195`
-   `1193`
-   `10267`
-   `10004`

------------------------------------------------------------------------

# 21. Design Status

The Gold design is approved with:

-   4 analytical products;
-   6 physical tables;
-   defined schemas;
-   defined physical types;
-   defined natural keys;
-   defined temporal grains;
-   defined geographical grains;
-   defined Iceberg partitioning;
-   defined transformations;
-   defined integration rules;
-   defined quality controls;
-   defined load strategy;
-   defined idempotency requirements.

The physical Gold implementation must follow this document.

New metrics, indicators, geographical levels, temporal grains,
transformations, or integration rules must not be introduced without
explicit approval.