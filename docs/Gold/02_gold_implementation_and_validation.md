# Gold Layer — Implementation and Validation

## 1. Purpose

This document records the implementation and technical validation of the final
Gold layer of the Energy Lakehouse Platform.

It complements:

```text
docs/Gold/01_gold_design.md
```

The design document defines the final analytical model and transformation
rules.

This document records implementation evidence obtained from real Silver data,
Apache Spark execution, Apache Iceberg persistence, Trino queries and the final
Airflow-controlled historical workflow.

The validated Gold processing path is:

```text
Apache Iceberg Silver
        │
        ▼
Apache Spark / PySpark
        │
        ▼
Gold transformations
        │
        ▼
Apache Iceberg Gold
        │
        ▼
       MinIO
        │
        ▼
       Trino
```

The final physical Gold model contains exactly:

```text
4 tables
```

and no longer contains the previously evaluated national 5-minute or
15-minute Gold facts.

---

# 2. Final Gold Inventory

The final Gold namespace contains:

```text
gold_dim_geography
gold_dim_time
gold_fact_installed_capacity_monthly
gold_fact_province_hourly
```

The physical model therefore contains:

```text
2 fact tables
2 dimensions
```

The following previous tables are not part of the final model:

```text
gold_fact_country_15min
gold_fact_country_5min
```

---

# 3. Final Analytical Grains

The two implemented fact tables use different analytical grains.

## Province-hour fact

```text
gold_fact_province_hourly
```

Grain:

```text
Province × hour
```

Content:

```text
meteorological information
+
hourly electricity-generation information
```

## Installed-capacity fact

```text
gold_fact_installed_capacity_monthly
```

Grain:

```text
Autonomous Community × month
```

Content:

```text
installed electricity-generation capacity by technology
```

Installed capacity is not artificially distributed to provinces.

---

# 4. Implementation Structure

Gold processing is implemented under:

```text
spark/jobs/gold/
```

Gold tests are maintained under:

```text
tests/gold/
```

The principal persistence entry points include:

```text
spark/jobs/gold/create_tables.py
spark/jobs/gold/write_gold.py
```

Shared Gold transformation functionality is maintained under the same Gold job
package.

The implementation separates:

```text
transformation logic
table creation
persistence
validation
```

rather than embedding analytical processing inside Airflow DAG definitions.

---

# 5. Gold Configuration

Gold-specific configuration is externalized in:

```text
config/gold_config.json
```

The validated configuration includes:

```json
{
  "esios_time_gap_hours": 1,
  "peninsula_excluded_province_codes": [
    "07",
    "35",
    "38",
    "51",
    "52"
  ]
}
```

The hourly ESIOS timestamp rule is therefore:

```text
gold_timestamp
=
observation_timestamp
+
1 hour
```

through configuration rather than a hardcoded transformation constant.

The hourly offset is not automatically applied to monthly installed-capacity
data.

The excluded province codes define the validated Peninsula scope used by Gold
geographical logic.

---

# 6. Final Silver Inputs

The Gold implementation consumes the final nine-table Silver model.

Relevant inputs include:

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

Gold therefore operates on the current final Silver model rather than the
earlier twelve-table implementation.

AEMET current observations remain part of the reusable Silver model, but the
final historical Airflow reconstruction deliberately excludes new AEMET current
acquisition from arbitrary historical reloads.

---

# 7. Province × Hour Weather Preparation

Meteorological source observations are prepared before being integrated with
energy data.

The target intermediate grain is:

```text
Province × hour
```

The general transformation model is:

```text
AEMET station observations
        │
        ▼
Province × hour
```

and:

```text
Open-Meteo point observations
        │
        ▼
Province × hour
```

This prevents direct station-to-energy joins from multiplying analytical rows.

---

# 8. AEMET Weather Processing

AEMET current observations provide recent official meteorological
measurements.

The AEMET station catalogue is used to resolve station geography.

Relevant metrics used in the Gold fallback process include:

```text
temperature
humidity
precipitation
```

Valid station observations are aggregated to:

```text
Province × hour
```

An AEMET observation without a valid station-to-province resolution must not be
assigned an invented province.

AEMET current observations are a recent/current source.

They are deliberately excluded from arbitrary historical reconstruction by the
final:

```text
historical_reload
```

workflow.

Historical meteorological reconstruction is therefore supplied by Open-Meteo.

---

# 9. Open-Meteo Hourly Processing

Open-Meteo hourly data is aggregated from point/station level to:

```text
Province × hour
```

Relevant analytical variables include:

```text
temperature_2m
relative_humidity_2m
precipitation
shortwave_radiation
direct_normal_irradiance
```

They feed Gold metrics including:

```text
temperature
humidity
precipitation
solar_radiation
direct_normal_irradiance
```

Historical Open-Meteo hourly Bronze objects are organized as canonical daily
station objects.

A complete daily object is validated against:

```text
24 hourly timestamps
```

rather than object existence alone.

---

# 10. Open-Meteo 15-Minute Wind Processing

The table:

```text
silver_open_meteo_15min
```

provides elevated-wind variables at 15-minute resolution.

Gold derives hourly values for:

```text
wind_speed_80m
wind_direction_80m
wind_speed_120m
wind_direction_120m
```

Historical Open-Meteo 15-minute Bronze objects are considered complete only
when the expected daily temporal axis contains:

```text
96 timestamps
```

for a complete UTC day.

## 10.1 Wind Speed

Temporal aggregation:

```text
15-minute observations
→ hourly average per point
```

Spatial aggregation:

```text
hourly point values
→ provincial average
```

The final grain is:

```text
Province × hour
```

## 10.2 Wind Direction

Wind direction uses circular rather than arithmetic averaging.

Temporal aggregation:

```text
15-minute directions
→ circular hourly mean per point
```

Spatial aggregation:

```text
hourly point directions
→ circular provincial mean
```

This prevents invalid results around the 0° / 360° boundary.

---

# 11. Meteorological Fallback

The approved fallback policy applies only to:

```text
temperature
humidity
precipitation
```

For each:

```text
Province × hour × metric
```

the rule is:

```text
valid AEMET value exists
→ use AEMET
```

otherwise:

```text
use available Open-Meteo value
```

Fallback is applied independently for each metric.

The selected source is preserved in:

```text
temperature_source
humidity_source
precipitation_source
```

This is controlled source integration rather than arbitrary imputation.

In historical reloads, AEMET current observations are not used to reconstruct
arbitrary past dates.

---

# 12. Hourly ESIOS Generation Processing

The energy input is:

```text
silver_esios_energy_hourly
```

The final Gold mapping contains 11 indicators.

| Indicator ID | Gold metric |
|---:|---|
| 1159 | `wind_generation_mwh` |
| 1161 | `solar_photovoltaic_generation_mwh` |
| 1162 | `solar_thermal_generation_mwh` |
| 10035 | `hydraulic_generation_mwh` |
| 1153 | `nuclear_generation_mwh` |
| 1156 | `combined_cycle_generation_mwh` |
| 1158 | `gas_natural_steam_turbine_generation_mwh` |
| 1164 | `gas_natural_cogeneration_mwh` |
| 10036 | `coal_generation_mwh` |
| 10041 | `other_renewables_generation_mwh` |
| 10043 | `total_generation_mwh` |

The hourly source values are retained as the corresponding hourly energy
metrics.

---

# 13. Hourly Energy Semantics

The implemented hourly rule is:

```text
Gold hourly metric
=
corresponding ESIOS hourly value
```

No synthetic 5-minute conversion is part of the final Gold model.

Hourly generation metrics are represented as:

```text
MWh
```

The source sign is preserved.

Therefore:

```text
negative source value
→ negative Gold value
```

```text
published zero
→ zero
```

```text
missing observation
→ NULL
```

A structurally valid ESIOS response with:

```text
values = []
```

is handled upstream as valid:

```text
NO_DATA
```

and does not create synthetic zero-valued Gold observations.

---

# 14. Official Total Generation

Indicator:

```text
10043
```

is mapped directly to:

```text
total_generation_mwh
```

It is the official ESIOS total generation value used by the final analytical
model.

It is not reconstructed by summing the selected generation technologies.

---

# 15. Province-Hour Energy Preparation

ESIOS observations are normalized to the target analytical grain before the
weather-energy join.

The intermediate energy block is:

```text
Province × hour
```

with the configured temporal alignment already applied.

Before integration, uniqueness is validated on:

```text
province_code
+
gold_timestamp
```

Duplicate energy keys are treated as errors rather than silently discarded.

---

# 16. Province-Hour Integration

The principal Gold integration rule is:

```text
Meteorological Province × hour
             FULL OUTER JOIN
Energy Province × hour
```

on:

```text
province_code
gold_timestamp
```

Both input blocks must be unique on that grain before the join.

## 16.1 Valid Row Types

The FULL OUTER JOIN intentionally allows:

```text
weather + energy
```

```text
weather only
```

```text
energy only
```

A missing source results in NULL metrics for that source.

The valid source information from the other side is retained.

## 16.2 Join Protection

The implementation must not perform joins at station or point grain against
provincial energy observations.

Incorrect:

```text
weather station
×
Open-Meteo point
×
ESIOS province
```

Correct:

```text
weather
→ Province × hour

energy
→ Province × hour

then join
```

This prevents artificial row multiplication.

---

# 17. `gold_fact_province_hourly` Physical Validation

The persisted physical schema was inspected through Trino.

Validated columns are:

```text
gold_timestamp
time_key
geography_key

province_code
province_name
autonomous_community_code
autonomous_community_name

temperature
humidity
precipitation

wind_speed_80m
wind_direction_80m
wind_speed_120m
wind_direction_120m

solar_radiation
direct_normal_irradiance

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

temperature_source
humidity_source
precipitation_source

gold_created_at
```

Timestamp columns are exposed by Trino as:

```text
timestamp(6) with time zone
```

The natural key is:

```text
province_code
+
gold_timestamp
```

---

# 18. Installed-Capacity Processing

The Silver source is:

```text
silver_esios_installed_capacity_monthly
```

The final Gold fact is:

```text
gold_fact_installed_capacity_monthly
```

Its grain is:

```text
Autonomous Community × month
```

Installed capacity remains at the geographical level supplied by the validated
source and is not artificially distributed to provinces.

---

# 19. Installed-Capacity Indicator Mapping

The final mapping contains 9 indicators.

| Indicator ID | Gold metric |
|---:|---|
| 1475 | `hydraulic_installed_capacity_mw` |
| 1485 | `wind_installed_capacity_mw` |
| 1486 | `solar_photovoltaic_installed_capacity_mw` |
| 1487 | `solar_thermal_installed_capacity_mw` |
| 10302 | `renewable_total_installed_capacity_mw` |
| 1477 | `nuclear_installed_capacity_mw` |
| 1478 | `coal_installed_capacity_mw` |
| 1483 | `combined_cycle_installed_capacity_mw` |
| 1488 | `other_renewables_installed_capacity_mw` |

Installed-capacity values remain:

```text
MW
```

and are not converted to MWh.

---

# 20. Installed-Capacity Physical Validation

The persisted physical schema inspected through Trino contains:

```text
year_month
time_key
gold_month_timestamp
source_timestamp

geography_key
autonomous_community_code
autonomous_community_name
esios_geo_id

hydraulic_installed_capacity_mw
wind_installed_capacity_mw
solar_photovoltaic_installed_capacity_mw
solar_thermal_installed_capacity_mw
renewable_total_installed_capacity_mw
nuclear_installed_capacity_mw
coal_installed_capacity_mw
combined_cycle_installed_capacity_mw
other_renewables_installed_capacity_mw

gold_created_at
```

Timestamp columns are exposed as:

```text
timestamp(6) with time zone
```

The natural key is:

```text
autonomous_community_code
+
year_month
```

---

# 21. Official Renewable Capacity Total

Indicator:

```text
10302
```

is retained directly as:

```text
renewable_total_installed_capacity_mw
```

It is not reconstructed by summing the individual renewable-capacity metrics.

---

# 22. Gold Geographical Dimension

The final geographical dimension is:

```text
gold_dim_geography
```

The validated final dimension contains:

```text
PROVINCE               = 52
AUTONOMOUS_COMMUNITY   = 19
COUNTRY                = 1
PENINSULA              = 1
```

Therefore:

```text
gold_dim_geography rows
= 73
```

The physical fact grains remain:

```text
PROVINCE
AUTONOMOUS_COMMUNITY
```

COUNTRY and PENINSULA are reusable analytical members of the dimension and are
not separate physical fact grains.

CNIG remains the canonical geographical source.

Validated ESIOS hourly geography maps to:

```text
47 provinces
```

with no unmatched ESIOS province records.

The five remaining canonical province-level entities do not require fabricated
ESIOS identifiers.

---

# 23. Gold Time Dimension

The final temporal dimension is:

```text
gold_dim_time
```

The final fact tables physically contain:

```text
time_key
```

and therefore reference the temporal analytical dimension using this key.

The implemented physical fact grains require temporal members compatible with:

```text
HOUR
MONTH
```

No 5-minute or 15-minute Gold fact is part of the final physical model.

The number of rows in `gold_dim_time` depends on the temporal coverage loaded
into Gold.

Historical row counts are execution evidence and must not be treated as a
permanent dimensional cardinality.

---

# 24. Peninsula Geographical Rule

The validated Peninsular meteorological scope excludes:

```text
07 — Illes Balears
35 — Las Palmas
38 — Santa Cruz de Tenerife
51 — Ceuta
52 — Melilla
```

The Peninsula definition is constructed from province-level canonical
geography.

Spain and Peninsula are distinct analytical scopes.

The final physical model contains:

```text
no dedicated Peninsula fact
```

but:

```text
gold_dim_geography
```

includes a distinct:

```text
PENINSULA
```

member.

This member must not be interpreted as an additional physical fact grain.

---

# 25. NULL and Zero Validation

The Gold implementation follows:

```text
published zero
→ 0
```

```text
source observation missing
→ NULL
```

Therefore:

```text
NULL ≠ 0
```

The Gold model must not globally apply:

```text
COALESCE(metric, 0)
```

to missing source information.

The FULL OUTER JOIN preserves legitimate partial source coverage.

A valid ESIOS `NO_DATA` response is also distinct from a published zero.

---

# 26. Duplicate Protection

Duplicate Gold natural keys are considered invalid.

The critical fact controls are:

### Province-hour

```text
province_code
+
gold_timestamp
```

### Installed capacity

```text
autonomous_community_code
+
year_month
```

The implementation validates uniqueness instead of hiding duplicate analytical
rows with an uncontrolled post-join deduplication.

---

# 27. Geography Protection

Gold must not fabricate unsupported geographical detail.

Prohibited transformations include:

```text
Autonomous Community
→ artificial Province values
```

and any other expansion not supported by the source.

The monthly installed-capacity fact therefore remains at:

```text
Autonomous Community
```

while hourly weather-energy integration uses:

```text
Province
```

where supported by the source.

COUNTRY and PENINSULA remain distinct reusable dimension members rather than
manufactured fact-level detail.

---

# 28. Unit Validation

The final Gold implementation preserves the distinction between:

```text
MW
```

and:

```text
MWh
```

## Electricity generation

```text
MWh
```

## Installed capacity

```text
MW
```

These physical quantities must not be treated as interchangeable.

For an interval of exactly one hour, a mean power expressed in MW may be
numerically equal to the energy in MWh over that hour, but the physical
magnitudes and units remain conceptually different.

---

# 29. Automated Gold Tests

The Gold implementation includes automated tests covering functionality such
as:

- geographical normalization;
- temporal alignment;
- meteorological aggregation;
- elevated-wind aggregation;
- circular wind-direction calculation;
- ESIOS metric mapping;
- weather fallback;
- Province × hour integration;
- natural-key protection;
- Gold construction and persistence behaviour.

The latest validated Gold automated suite completed successfully with:

```text
72 passed
```

No failing Gold tests remained.

After the final orchestration and persistence changes, the complete regression
status was:

```text
tests/ingestion = 84 passed
tests/silver    = 85 passed
tests/gold      = 72 passed
```

**Status: VALIDATED**

---

# 30. Independent Historical E2E Validation

An independent real-data Lakehouse execution was performed for:

```text
2026-01-10 → 2026-01-15
```

The selected interval had actual data available for the configured final ESIOS
indicator families.

Bronze acquisition completed successfully.

The data was then processed through:

```text
Bronze
→ Silver
→ Gold
→ Trino
```

This execution predates the final Airflow historical persistence policy.

It also included AEMET current observations in the analytical state.

Those details are retained as historical validation evidence and must not be
confused with the final `historical_reload` semantics.

---

# 31. Silver Input Validation

The independent historical execution produced exactly nine Silver tables.

Validated counts were:

```text
silver_aemet_stations
= 926

silver_aemet_current_observations
= 9786

silver_open_meteo_hourly
= 133344

silver_open_meteo_15min
= 533376

silver_cnig_provinces
= 52

silver_cnig_autonomous_communities
= 19

silver_cnig_municipalities
= 8132

silver_esios_energy_hourly
= 38443

silver_esios_installed_capacity_monthly
= 123
```

These counts belong to that specific independent execution.

They are retained as validation evidence and must not be interpreted as
permanent Silver table sizes.

---

# 32. Gold Persistence Validation

The Gold write process completed successfully.

The execution reported:

```text
GOLD PERSISTENCE COMPLETED
```

The resulting Gold namespace contained exactly:

```text
gold_dim_geography
gold_dim_time
gold_fact_installed_capacity_monthly
gold_fact_province_hourly
```

No obsolete Gold fact was present.

The Gold writer supports:

```text
upsert
insert-only
```

The default writer behaviour remains:

```text
upsert
```

The final historical Airflow workflow explicitly uses:

```text
LAKEHOUSE_WRITE_POLICY=insert-only
```

for historical Silver and Gold writes.

This prevents PRESERVE executions from rewriting existing active natural keys.

**Status: VALIDATED**

---

# 33. Historical Gold Row Counts

For the independent historical execution described above, Trino returned:

```text
gold_dim_geography
= 71

gold_dim_time
= 158

gold_fact_province_hourly
= 8147

gold_fact_installed_capacity_monthly
= 19
```

These counts represent that specific validation state.

They are not permanent model cardinalities.

In particular, the final geographical dimension was subsequently expanded and
validated with:

```text
PROVINCE               = 52
AUTONOMOUS_COMMUNITY   = 19
COUNTRY                = 1
PENINSULA              = 1
```

for a final structural cardinality of:

```text
73
```

members.

---

# 34. Province-Hour Key Validation

For the independent historical validation state, the persisted fact contained:

```text
province_codes
= 52
```

and:

```text
duplicate_province_hour_keys
= 0
```

Therefore, that execution preserved the required:

```text
Province × hour
```

natural-key uniqueness.

Subsequent Airflow persistence validation also returned zero duplicate
Province × hour keys.

---

# 35. Installed-Capacity Key Validation

For the independent historical validation state, the persisted monthly fact
contained:

```text
rows
= 19

distinct months
= 1

year_month
= 2026-01

duplicate_capacity_keys
= 0
```

All:

```text
19
```

rows contained installed-capacity values.

This confirms:

```text
Autonomous Community × month
```

uniqueness for that validation execution.

The row count is execution evidence rather than a permanent table cardinality.

---

# 36. Weather-Energy Coverage Validation

For the independent historical validation state, the Province-hour fact
contained:

```text
rows_with_weather
= 8100

rows_with_energy
= 6768

rows_with_weather_and_energy
= 6721
```

The FULL OUTER behaviour can be reconciled exactly.

Weather-only:

```text
8100 - 6721
= 1379
```

Energy-only:

```text
6768 - 6721
= 47
```

Rows containing both:

```text
6721
```

Therefore:

```text
1379
+
47
+
6721
=
8147
```

which is exactly the persisted fact row count for that execution.

This provides direct evidence that:

```text
FULL OUTER JOIN
```

preserves valid observations from either source block.

The value `8147` is historical execution evidence and is not a permanent Gold
table size.

**Status: VALIDATED**

---

# 37. Real Integrated Rows

Real Gold rows were queried through Trino.

Validated examples included provinces such as:

```text
Araba/Álava
Albacete
Alacant/Alicante
Almería
Ávila
```

with combinations of actual metrics such as:

```text
temperature
wind_speed_80m
solar_radiation
wind_generation_mwh
solar_photovoltaic_generation_mwh
total_generation_mwh
```

according to the coverage provided by the original sources.

This confirms actual cross-source integration rather than independent
population of unrelated weather and energy columns.

---

# 38. Timestamp Coverage and Historical Semantics

The independent historical validation requested:

```text
2026-01-10 → 2026-01-15
```

That earlier execution also contained AEMET current observations with their real
recent timestamps.

Because the Province-hour fact uses FULL OUTER integration, valid current
weather-only observations were preserved in that validation state.

This behaviour must not be confused with the final historical Airflow policy.

The final:

```text
historical_reload
```

workflow excludes:

```text
AEMET current_observations
```

from arbitrary historical reconstruction.

Therefore, later recent timestamps found in the earlier validation are evidence
of that previous execution context rather than a requirement of the final
historical workflow.

The configured ESIOS temporal rule:

```text
gold_timestamp
=
observation_timestamp
+
1 hour
```

can also produce a valid next-day boundary after the final source hour of a
requested interval.

That boundary is expected alignment behaviour and must not automatically be
treated as an out-of-range processing error.

---

# 39. Installed-Capacity Temporal Coverage

For the independent historical validation state, the persisted
installed-capacity fact contained:

```text
installed_capacity_min_month
= 2026-01

installed_capacity_max_month
= 2026-01

installed_capacity_months
= 1
```

This is consistent with that historical validation execution.

These values describe that execution only and are not permanent model
constraints.

---

# 40. Trino Catalog Validation

The final Gold tables were queried through:

```text
iceberg.gold
```

`SHOW TABLES` exposed exactly:

```text
gold_dim_geography
gold_dim_time
gold_fact_installed_capacity_monthly
gold_fact_province_hourly
```

`DESCRIBE` and analytical validation queries were successfully executed against
the final Gold model.

The persisted Iceberg data is therefore accessible independently from Spark.

**Status: VALIDATED**

---

# 41. End-to-End Lakehouse and Airflow Validation

The complete real processing path is:

```text
AEMET ─────────────┐
Open-Meteo ────────┤
REE / ESIOS ───────┼──► Bronze / MinIO
CNIG / IGN ────────┘
                          │
                          ▼
                     Apache Spark
                          │
                          ▼
                    Silver / Iceberg
                          │
                          ▼
                     Apache Spark
                          │
                          ▼
                     Gold / Iceberg
                          │
                          ▼
                         Trino
```

This path was executed with real source data.

The complete historical:

```text
Bronze
→ Silver
→ Gold
```

path was subsequently validated under direct Airflow control.

The final `historical_reload` workflow supports:

```text
PRESERVE
RANGE OVERWRITE
FULL DELETE
```

and all three persistence behaviours were validated with real data.

Therefore:

```text
Real APIs
→ Bronze
→ Silver
→ Gold
→ Trino
```

and the final historical orchestration path are technically validated.

---

# 42. Current Gold Quality Status

Validated controls include:

```text
Gold table inventory
= VALIDATED

Province × hour natural-key uniqueness
= VALIDATED

Autonomous Community × month uniqueness
= VALIDATED

FULL OUTER integration
= VALIDATED

Weather-only preservation
= VALIDATED

Energy-only preservation
= VALIDATED

Weather + energy integration
= VALIDATED

Gold physical schemas
= VALIDATED

Gold Iceberg persistence
= VALIDATED

Gold queryability through Trino
= VALIDATED

Historical PRESERVE
= VALIDATED

Historical RANGE OVERWRITE
= VALIDATED

Historical FULL DELETE
= VALIDATED

Historical insert-only Gold writes
= VALIDATED
```

Metric NULL values remain allowed when they represent genuine source coverage
limitations.

A valid ESIOS `NO_DATA` response remains distinct from a published zero.

---

# 43. Removed Previous Gold Scope

The final implementation deliberately removes the earlier high-frequency
physical Gold products.

Removed:

```text
gold_fact_country_15min
gold_fact_country_5min
```

The associated earlier analytical scope included:

```text
ESIOS 5-minute power
electricity demand
Spain/Peninsula high-frequency facts
```

These products are not part of the current final analytical model and must not
appear in the final Gold inventory, tests, row-count evidence or visualization
contract.

---

# 44. Silver Model Reconciliation

The final Gold implementation is based on:

```text
9 Silver tables
```

and not the previous twelve-table Silver model.

Removed upstream physical Silver tables include:

```text
silver_aemet_daily_climatology
silver_open_meteo_historical_forecast
silver_esios_power_5min
```

This reconciliation is important because Gold documentation must match the
actual current Lakehouse catalog rather than historical intermediate
implementations.

---

# 45. Visualization Boundary

The current downstream boundary validated by this document is:

```text
Trino
```

The analytical Gold tables are queryable and therefore ready to be exposed to
Apache Superset.

However:

```text
final Superset datasets
final charts
final dashboards
```

are not validated by this Gold implementation document.

They belong to the visualization stage.

---

# 46. Airflow Validation

The underlying data-processing path:

```text
Bronze
→ Silver
→ Gold
→ Trino
```

is validated.

The final historical Bronze → Silver → Gold runtime was also executed
successfully under direct Apache Airflow control.

The validated `historical_reload` parameters are:

```text
fecha_inicio
fecha_fin
sobreescribir_datos
eliminar_historial_completo
```

The validated policies are:

```text
PRESERVE

RANGE OVERWRITE

FULL DELETE
```

FULL DELETE has priority over RANGE OVERWRITE.

Validation confirmed:

```text
PRESERVE
→ existing active Silver/Gold files preserved
→ missing coverage added
→ duplicate natural keys = 0
```

```text
RANGE OVERWRITE
→ requested interval rebuilt
→ outside-range data preserved
→ masters preserved
→ duplicate natural keys = 0
```

```text
FULL DELETE
→ active Bronze removed
→ current Silver and Gold tables purged
→ active Silver/Gold warehouse prefixes physically cleaned
→ masters rebuilt
→ previous-run physical Silver/Gold objects = 0
```

**Status: VALIDATED**

---

# 47. Repository Status

This document does not claim that the current repository is clean, committed or
synchronized.

The documentation is currently being reconciled with the final implemented
Lakehouse model.

Git closure must therefore be validated separately after all documentation
changes have been reviewed and saved.

No commit hash is recorded here as evidence of the current documentation state
until that final repository verification is actually executed.

---

# 48. Final Gold Implementation Status

The current validated Gold implementation is:

```text
Final physical Gold tables
= 4

Fact tables
= 2

Dimensions
= 2

Main analytical grain
= Province × hour

Installed-capacity grain
= Autonomous Community × month

Hourly ESIOS generation indicators
= 11

Monthly ESIOS capacity indicators
= 9

AEMET / Open-Meteo fallback
= VALIDATED

ESIOS valid empty response
= NO_DATA

ESIOS hourly temporal alignment
= CONFIGURED AT +1 HOUR

Province-hour FULL OUTER integration
= VALIDATED

Province-hour duplicate keys
= 0

Installed-capacity duplicate keys
= 0

Final gold_dim_geography structure
= 73 members

Gold automated tests
= 72 PASSED

Complete regression suites
= 84 INGESTION / 85 SILVER / 72 GOLD PASSED

Gold Iceberg persistence
= VALIDATED

Gold Trino queryability
= VALIDATED

Historical Airflow PRESERVE
= VALIDATED

Historical Airflow RANGE OVERWRITE
= VALIDATED

Historical Airflow FULL DELETE
= VALIDATED

Historical Gold write policy
= INSERT-ONLY

Real APIs → Bronze → Silver → Gold → Trino
= VALIDATED

Airflow Bronze → Silver → Gold
= VALIDATED

Final Superset dashboards
= PENDING VISUALIZATION IMPLEMENTATION
```

Historical row counts such as:

```text
gold_fact_province_hourly = 8147
gold_fact_installed_capacity_monthly = 19
gold_dim_geography = 71
gold_dim_time = 158
```

are retained only as evidence of the independent historical validation state in
which they were measured.

They are not permanent cardinalities of the final model.

The Gold layer is implemented and technically validated for the final
Energy Lakehouse Platform scope.
