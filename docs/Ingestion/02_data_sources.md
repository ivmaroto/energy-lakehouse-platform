# Data Sources

## 1. Overview

The Energy Lakehouse Platform integrates public meteorological, geographical and
electricity-system data from four source domains:

- AEMET OpenData;
- Open-Meteo;
- REE / ESIOS;
- CNIG / IGN.

These sources provide the information required to build the final analytical
model of the platform.

The principal analytical use case combines:

```text
meteorological conditions
+
electricity generation
```

at:

```text
Province × hour
```

A complementary analytical product represents installed electricity-generation
capacity at:

```text
Autonomous Community × month
```

The ingestion layer retrieves and preserves the source information in Bronze.

Subsequent normalization, geographical harmonization, aggregation and
cross-source integration are performed in Silver and Gold.

---

## 2. AEMET OpenData

### 2.1 Description

AEMET (Agencia Estatal de Meteorología) is the Spanish national meteorological
agency.

AEMET OpenData provides programmatic access to official meteorological datasets
through its public API.

AEMET acts as the official meteorological reference source used by the
platform.

---

### 2.2 Authentication

Access requires an API key.

The credential is supplied through:

```text
AEMET_API_KEY
```

The real key must never be:

- embedded in source code;
- written in committed documentation;
- committed to Git.

---

### 2.3 Final active datasets

The current AEMET ingestion scope contains:

```text
stations
current_observations
```

Datasets previously evaluated during development but not retained in the final
scope include:

```text
daily climatology
radiation ingestion
```

---

### 2.4 Station catalogue

The AEMET station dataset acts as the official meteorological point catalogue
for the project.

The validated catalogue used by historical Open-Meteo acquisition contains:

```text
926 stations
```

Station identifiers and coordinates are subsequently normalized in Silver.

The same validated station catalogue supplies the geographical points used for
Open-Meteo acquisition.

---

### 2.5 Current observations

AEMET current observations provide recent official meteorological measurements.

The source is suitable for current or recent information but is not treated as
a mechanism for reconstructing arbitrary historical observation periods.

The final `historical_reload` workflow therefore excludes AEMET current
observations from arbitrary historical reconstruction.

Historical meteorological coverage required by the analytical model is supplied
by Open-Meteo.

---

### 2.6 Role in Gold

For meteorological variables supported by the current AEMET observations, the
Gold model uses AEMET as the preferred source when a valid value exists.

The current fallback-enabled variables are:

```text
temperature
humidity
precipitation
```

Open-Meteo provides the metric-specific fallback when the corresponding AEMET
value is unavailable.

This source-selection logic is performed in Gold rather than during ingestion.

---

## 3. Open-Meteo

### 3.1 Description

Open-Meteo provides meteorological information through HTTP APIs.

It supplies the historical and higher-frequency meteorological coverage used by
the principal analytical flow.

The project uses a configured Open-Meteo service plan.

Any runtime source-access configuration or credential required by that plan is
externalized from source code and must not be committed to Git.

---

### 3.2 Final active datasets

The current Open-Meteo scope contains:

```text
weather_hourly
weather_15min
```

The same validated set of:

```text
926 AEMET station locations
```

is used as the geographical point catalogue for both datasets.

Historical acquisition persists canonical daily Bronze objects per station.

---

## 4. Open-Meteo API Strategy

Different Open-Meteo services are used depending on the requested temporal
dataset.

### 4.1 Current / incremental API

```text
Forecast API
```

Used for current or incremental access where appropriate.

---

### 4.2 Historical hourly API

```text
Archive API
```

Used to retrieve historical hourly meteorological information.

---

### 4.3 Historical 15-minute API

```text
Historical Forecast API
```

Used for historical 15-minute meteorological acquisition.

The standard Forecast API is not used as a generic replacement for arbitrary
historical 15-minute periods.

This distinction was validated during implementation against the real
Open-Meteo service behaviour.

---

## 5. Open-Meteo Meteorological Variables

The analytical flow currently uses Open-Meteo variables including:

```text
temperature_2m
relative_humidity_2m
precipitation

wind_speed_80m
wind_direction_80m

wind_speed_120m
wind_direction_120m

shortwave_radiation
direct_normal_irradiance
```

These variables support analysis involving:

- temperature;
- humidity;
- precipitation;
- wind conditions at elevated heights;
- solar radiation.

Not every raw source field must necessarily be exposed in the final Gold model.

---

## 6. Open-Meteo Historical Coverage

Historical acquisition is performed over the validated:

```text
926 locations
```

The final implementation validates completeness at canonical daily-object
level.

For each complete UTC day:

### Hourly

```text
24 timestamps
```

### 15-minute

```text
96 timestamps
```

Object existence alone is not considered sufficient evidence of completeness.

A partial daily object is incomplete and must be reloaded or completed.

For the earlier independent historical validation interval:

```text
2026-01-10 → 2026-01-15
```

the expected temporal coverage per location was:

```text
6 × 24 = 144 hourly observations
```

and:

```text
6 × 24 × 4 = 576 fifteen-minute observations
```

That execution produced:

```text
926 / 926 hourly locations
926 / 926 15-minute locations
```

with resulting Silver counts:

```text
silver_open_meteo_hourly = 133344

926 × 144 = 133344
```

and:

```text
silver_open_meteo_15min = 533376

926 × 576 = 533376
```

These values remain valid evidence for that historical execution, while the
current completeness rule is evaluated per canonical daily object.

---

## 7. REE / ESIOS

### 7.1 Description

REE / ESIOS provides public information related to the operation of the Spanish
electricity system.

It is the principal electricity-system source used by the platform.

---

### 7.2 Authentication

Access to the selected API services requires an access credential.

The credential is provided through:

```text
ESIOS_API_KEY
```

The real credential must remain outside source control.

---

### 7.3 Final ESIOS scope

The final active ESIOS configuration contains:

```text
11 hourly electricity-generation indicators
9 monthly installed-capacity indicators
```

The selected indicators are maintained in:

```text
config/esios_indicators.json
```

The final project scope does not include:

```text
electricity demand
electricity market prices
ESIOS 5-minute power datasets
```

---

## 8. ESIOS Hourly Generation Indicators

The final hourly indicator catalogue is:

| Indicator ID | Dataset |
|---:|---|
| 1159 | `generacion_medida_eolica_terrestre` |
| 1161 | `generacion_medida_solar_fotovoltaica` |
| 1162 | `generacion_medida_solar_termica` |
| 10035 | `generacion_medida_hidraulica` |
| 1153 | `generacion_medida_nuclear` |
| 1156 | `generacion_medida_ciclo_combinado` |
| 1158 | `generacion_medida_gas_natural_turbina_vapor` |
| 1164 | `generacion_medida_gas_natural_cogeneracion` |
| 10036 | `generacion_medida_carbon` |
| 10041 | `generacion_medida_otras_renovables` |
| 10043 | `generacion_medida_total` |

These indicators are normalized downstream into:

```text
silver_esios_energy_hourly
```

and subsequently integrated into:

```text
gold_fact_province_hourly
```

---

## 9. ESIOS Installed-Capacity Indicators

The final monthly installed-capacity catalogue is:

| Indicator ID | Dataset |
|---:|---|
| 1475 | `potencia_instalada_hidraulica` |
| 1485 | `potencia_instalada_eolica` |
| 1486 | `potencia_instalada_solar_fotovoltaica` |
| 1487 | `potencia_instalada_solar_termica` |
| 10302 | `potencia_instalada_total_renovable` |
| 1477 | `potencia_instalada_nuclear` |
| 1478 | `potencia_instalada_carbon` |
| 1483 | `potencia_instalada_ciclo_combinado` |
| 1488 | `potencia_instalada_otras_renovables` |

These indicators are normalized into:

```text
silver_esios_installed_capacity_monthly
```

and subsequently transformed into:

```text
gold_fact_installed_capacity_monthly
```

---

## 10. ESIOS Units

The platform distinguishes strictly between:

```text
MW
```

and:

```text
MWh
```

### Installed capacity

Installed capacity is expressed in:

```text
MW
```

because it represents power capacity.

### Hourly generation

The hourly generation indicators used by the main Gold fact are represented as
energy metrics in:

```text
MWh
```

The numerical equality that can occur between average MW over exactly one hour
and MWh produced during that hour does not make the physical quantities
equivalent.

The units remain conceptually distinct throughout the analytical model.

---

## 11. ESIOS Response Availability

Successful HTTP transport does not necessarily imply that an ESIOS indicator
contains observations for a requested period.

The final ingestion implementation distinguishes:

```text
valid response with values
→ process the available observations
```

from:

```text
valid response with values = []
→ NO_DATA
```

A structurally valid empty response is not treated as an ingestion failure.

It also does not create:

```text
synthetic zero values
synthetic timestamps
synthetic source observations
```

Real API validation confirmed that all configured hourly and monthly indicators
returned observations for the independent historical validation interval:

```text
2026-01-10 → 2026-01-15
```

The same availability must not automatically be assumed for every interval.

---

## 12. CNIG / IGN

### 12.1 Description

CNIG / IGN provides the canonical geographical reference data used by the
Lakehouse.

This source was incorporated to avoid relying on geographical names or codes
from individual meteorological or energy providers as the platform-wide
territorial reference.

---

### 12.2 Active source masters

The current Bronze geographical masters are:

```text
provinces
municipalities
```

Silver processing subsequently generates the normalized autonomous-community
dimension.

---

### 12.3 Validated territorial cardinalities

The current normalized geographical model contains:

```text
52 province-level entities
19 autonomous communities
8132 municipalities
```

Official territorial codes are stored as strings so leading zeroes are
preserved.

---

### 12.4 Role in the platform

CNIG / IGN is used for:

- province normalization;
- autonomous-community derivation;
- municipality normalization;
- canonical geographical names;
- source geographical mapping.

Geographical harmonization occurs in Silver and Gold, not during Bronze
ingestion.

---

## 13. Geographic Scope

The final project scope is Spain, but different analytical products use
different validated geographical grains.

### Main hourly analytical product

```text
Province × hour
```

implemented as:

```text
gold_fact_province_hourly
```

### Installed-capacity product

```text
Autonomous Community × month
```

implemented as:

```text
gold_fact_installed_capacity_monthly
```

The platform therefore does not force all sources to Autonomous Community level.

The general principle is:

```text
Use Province when the validated source supports Province.

Otherwise preserve the real higher-level geography.
```

Geographical detail is never manufactured.

---

## 14. Peninsula Scope

Where a Peninsular meteorological aggregate is required, the validated scope
excludes:

```text
07  Illes Balears
35  Las Palmas
38  Santa Cruz de Tenerife
51  Ceuta
52  Melilla
```

The Peninsular aggregate is derived from valid province-level meteorological
information.

Spain-wide values must not be relabelled as Peninsular values.

The reusable `gold_dim_geography` dimension retains distinct COUNTRY and
PENINSULA members, while no dedicated Peninsula fact is part of the final
physical model.

---

## 15. Temporal Scope

The platform handles multiple temporal granularities.

### AEMET

```text
current / recent observations
```

### Open-Meteo

```text
hourly
15-minute
```

### ESIOS

```text
hourly generation
monthly installed capacity
```

### CNIG

```text
master / reference data
```

The source-specific temporal grain is preserved until a downstream
transformation explicitly aggregates it.

---

## 16. Historical Acquisition

Historical acquisition uses an explicit source interval:

```text
start_date
end_date
```

where supported by the source.

The final Airflow historical interface exposes the runtime interval as:

```text
fecha_inicio
fecha_fin
```

The principal historical observation datasets are:

```text
Open-Meteo hourly
Open-Meteo 15-minute
ESIOS hourly
ESIOS monthly
```

AEMET station and CNIG datasets are reference masters and are not tied to the
same historical observation window.

AEMET current observations are deliberately excluded from arbitrary historical
reconstruction in the final:

```text
historical_reload
```

workflow.

Historical reloads use ensure-style master handling:

```text
master exists
→ preserve it

master missing
→ ingest it
```

Therefore PRESERVE and RANGE OVERWRITE preserve existing masters, while FULL
DELETE rebuilds them after the active Bronze layer is removed.

---

## 17. Incremental / Recent Acquisition

The platform supports recurrent acquisition of newly available source data.

The exact latest available timestamp may differ between providers.

Therefore:

```text
requested end time
```

must not automatically be interpreted as:

```text
guaranteed data availability from every source
```

Source publication latency and API availability are preserved rather than
filled with synthetic observations.

The current Airflow runtime includes:

```text
hourly_ingestion
→ recurrent hourly Bronze ingestion

monthly_ingestion
→ recurrent monthly Bronze ingestion

open_meteo_15min
→ manual historical Open-Meteo 15-minute Bronze utility
```

The hourly and monthly DAGs are Bronze-only ingestion workflows.

They do not independently execute Silver or Gold promotion.

---

## 18. Source Comparison

| Source | Domain | Authentication / access configuration | Historical observations | Current / recent data | Main role |
|---|---|---|---|---|---|
| AEMET | Meteorology | `AEMET_API_KEY` | Not used for arbitrary historical reconstruction | Yes | Official stations and recent observations |
| Open-Meteo | Meteorology | Runtime access configuration externalized for the configured service plan | Yes | Yes | Historical and high-frequency weather |
| REE / ESIOS | Electricity system | `ESIOS_API_KEY` | Yes | Source-dependent | Generation and installed capacity |
| CNIG / IGN | Geography | Current master acquisition does not rely on a project API credential | Reference data | Reference data | Canonical territorial master |

No real credential or secret value belongs in this document.

---

## 19. Source Independence

Each source is handled independently.

```text
AEMET ─────────► AEMET connector ──────────┐
                                           │
Open-Meteo ───► Open-Meteo connector ──────┤
                                           ├──► Bronze / MinIO
REE / ESIOS ──► ESIOS connector ───────────┤
                                           │
CNIG / IGN ───► geographical ingestion ─────┘
```

Shared functionality is provided by common ingestion components for:

```text
configuration
HTTP communication
date handling
logging
storage
```

This design limits coupling between external providers.

---

## 20. Source Preservation

The ingestion layer preserves source information before business
transformation.

Bronze therefore does not perform:

- geographical harmonization;
- analytical aggregation;
- cross-source joining;
- metric fallback;
- KPI calculation;
- artificial null filling.

For analytical time-series datasets, Bronze storage is governed by source
observation time rather than ingestion time.

`ingestion_timestamp` remains audit metadata.

The processing path is:

```text
Source
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

Source absence remains distinct from a published numerical zero.

---

## 21. Validated Source Scope

The final implemented source scope is:

```text
AEMET
├── stations
└── current_observations

Open-Meteo
├── weather_hourly
└── weather_15min

REE / ESIOS
├── 11 hourly generation indicators
└── 9 monthly installed-capacity indicators

CNIG / IGN
├── provinces
└── municipalities
```

This scope is the source basis for the current:

```text
9 Silver tables
4 Gold tables
```

used by the Energy Lakehouse Platform.

The final historical Airflow workflow using these sources has been validated
through:

```text
PRESERVE
RANGE OVERWRITE
FULL DELETE
```

with the complete historical:

```text
Bronze → Silver → Gold
```

path executed successfully under Airflow control.
