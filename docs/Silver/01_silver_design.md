```
# Silver Layer Design

## 1. Purpose

The Silver layer transforms raw Bronze data into normalized, typed,
deduplicated and analytically usable datasets while preserving the original
granularity and traceability of each source.

Bronze remains immutable.

Silver does not perform analytical aggregations between temporal
granularities. Aggregations and analytical selections are deferred to Gold.

The main analytical objective of the platform is:

```text
Province × hour
Meteorological data + compatible ESIOS energy indicators
```

Additional analytical flows preserve higher-frequency and structural data:

```text
High frequency:
5 minutes / 15 minutes
according to the real geographical granularity of the source

Structural:
monthly
according to the real geographical granularity of the source

AEMET daily:
official climatological reference
not part of the main analytical flow
```

---

## 2. Validated Bronze Inventory

The physical Bronze inventory was validated directly against MinIO.

Validated result:

```text
TOTAL DATASETS = 43
TOTAL OBJECTS  = 3409
```

### AEMET

3 datasets:

- `stations`
- `daily_climatological_values`
- `current_observations`

Validated Bronze object counts:

| Dataset | Objects |
|---|---:|
| `stations` | 1 |
| `daily_climatological_values` | 607 |
| `current_observations` | 1 |

`current_observations` is included as the official incremental observation
source.

`daily_climatological_values` is retained as an official climatological
reference but is not part of the main analytical flow.

### Open-Meteo

3 datasets:

- `weather_hourly`
- `weather_historical_forecast`
- `weather_15min`

Validated Bronze object counts:

| Dataset | Objects |
|---|---:|
| `weather_hourly` | 921 |
| `weather_historical_forecast` | 921 |
| `weather_15min` | 921 |

### CNIG

2 datasets:

- `provinces`
- `municipalities`

Both datasets are used as official geographical reference data.

Validated CNIG physical evidence:

```text
provinces       = 52 records
municipalities  = 8132 records
autonomous communities derived from provinces = 19
CODES_WITH_MULTIPLE_NAMES = 0
```

The autonomous-community master is derived from the unique pairs:

```text
COD_CA + COMUNIDAD_AUTONOMA
```

Official geographical and INE codes are preserved as strings so that leading
zeroes are not lost.

The validated CSV samples are not UTF-8. Both `cp1252` and `latin-1`
successfully decode the inspected samples. The exact source encoding between
those two alternatives is **NOT VALIDATED**.

### ESIOS

35 configured Bronze datasets were physically validated in MinIO.

The Silver design keeps the configured ESIOS datasets and normalizes them
according to their actual temporal and geographical granularity.

Silver must not manufacture province-level information when the ESIOS
indicator does not provide province-level geography.

---

## 3. Analytical Granularities

The analytical granularities used by the platform are:

| Source / dataset family | Granularity |
|---|---|
| ESIOS high frequency | 5 minutes |
| Open-Meteo high frequency | 15 minutes |
| ESIOS hourly | 1 hour |
| Open-Meteo hourly | 1 hour |
| AEMET current observations | 1 hour |
| ESIOS installed capacity | Monthly |
| AEMET daily climatology | Daily reference |

Silver preserves the original granularity of every dataset.

Validated ESIOS temporal classification:

| ESIOS Silver family | Magnitude | Temporal granularity | Configured datasets |
|---|---|---|---:|
| Energy observations | Energy | 1 hour | 14 |
| High-frequency power | Power | 5 minutes | 12 |
| Installed capacity | Power | Monthly | 9 |
| **Total** | | | **35** |

No transformations such as:

```text
5 min -> 15 min
15 min -> hour
5 min -> hour
hour -> month
```

are performed in Silver.

These aggregations belong to the Gold layer.

---

## 4. Geographical Normalization

The common geographical hierarchy is based on official geographical
reference data.

The target hierarchy is:

```text
AEMET station
      ↓
official municipality
      ↓
official province
      ↓
autonomous community
```

CNIG is used as the official geographical reference for provinces and
municipalities.

Silver uses canonical geographical identifiers and normalized geographical
names.

Name inconsistencies between external sources must be resolved during
normalization rather than propagated to analytical tables.

Silver preserves the real geographical granularity supplied by each source.

In particular:

- province-level ESIOS data remains province-level;
- autonomous-community data remains autonomous-community-level;
- national or peninsular data is not artificially expanded to provinces.

---

## 5. Coordinate Normalization

AEMET station coordinates are normalized to decimal coordinates.

The coordinate conversion was validated successfully for:

```text
921 / 921 AEMET stations
```

Normalized coordinate ranges are:

```text
latitude  ∈ [-90, 90]
longitude ∈ [-180, 180]
```

Invalid coordinates are treated as data-quality incidents.

Raw source coordinate values may be preserved when required for traceability.

---

## 6. Temporal Normalization

Silver normalizes temporal fields while preserving the source granularity.

The source temporal granularities currently used are:

```text
ESIOS              5 minutes / 1 hour / monthly
Open-Meteo         15 minutes / 1 hour
AEMET observations 1 hour
AEMET daily        daily reference
```

Timestamps must be converted to normalized timestamp types.

Source temporal information required for traceability may also be preserved.

A missing or invalid mandatory timestamp is treated as a data-quality error.

No missing timestamps are generated or imputed.

---

## 7. Cleaning and Typing Principles

Bronze contains the raw source representation.

Silver is responsible for:

- converting numeric strings to appropriate numeric types;
- converting date/time fields to date or timestamp types;
- normalizing empty values;
- handling `NULL`;
- detecting structurally invalid values;
- preserving valid missing values when the source does not provide a metric;
- handling validated source-specific special values;
- avoiding artificial data imputation.

The following principle applies:

```text
NULL does not automatically mean ERROR.
```

A `NULL` value may represent a metric that was not available or not measured
by the source.

Mandatory fields and optional measurements must therefore be treated
separately.

Values are not replaced automatically with:

- zero;
- averages;
- previous observations;
- synthetic values.

Validated source-specific cases such as AEMET `Ip` must be handled explicitly
during transformation.

---

## 8. Deduplication and Idempotency

Silver processing must be idempotent.

Reprocessing the same Bronze information must not multiply the same logical
record.

The approved natural keys are:

| Dataset family | Natural key |
|---|---|
| AEMET stations | `station_id` |
| AEMET daily | `station_id + observation_date` |
| AEMET hourly | `station_id + observation_timestamp` |
| Open-Meteo | `station_id + observation_timestamp` |
| CNIG provinces | `province_code` |
| CNIG autonomous communities | `autonomous_community_code` |
| CNIG municipalities | `municipality_ine_code` |
| ESIOS | `indicator_id + esios_geo_id + observation_timestamp` |

Deduplication is especially relevant for AEMET `current_observations`,
because consecutive ingestion executions may contain overlapping observation
windows.

---

## 9. Meteorological Silver Design

Meteorological Silver data is built from AEMET and Open-Meteo without
prematurely merging their original datasets.

### AEMET

The following datasets are retained:

#### `stations`

Official station reference dataset.

The validated Bronze payload contained 921 stations.

#### `daily_climatological_values`

Official daily climatological dataset.

Granularity:

```text
daily
```

Role:

```text
official climatological reference
NOT part of the main analytical flow
```

#### `current_observations`

Official incremental conventional observations.

Granularity used in Silver:

```text
1 hour
```

Role:

```text
official hourly observation / contrast source
```

The validated Bronze object inspected during Silver design contained 9,688
records.

### Open-Meteo

The following datasets are retained independently:

#### `weather_hourly`

Granularity:

```text
1 hour
```

#### `weather_historical_forecast`

Granularity:

```text
1 hour
```

#### `weather_15min`

Granularity:

```text
15 minutes
```

Open-Meteo provides the reproducible meteorological data required for the
historical hourly analytical flow.

AEMET and Open-Meteo coexist in Silver.

Silver does not apply analytical precedence or aggregate one source into the
other.

For Open-Meteo, Silver preserves all validated meteorological variables
available in the source datasets. Variable selection for analytical products
is deferred to Gold.

Per-record technical traceability retained in Silver:

```text
source
ingestion_timestamp
```

The following extraction metadata is not replicated as columns of the Silver
observation fact:

```text
generationtime_ms
requested_start_date
requested_end_date
ingestion_mode
```

Timezone information is used during temporal normalization and is not
unnecessarily replicated after the observation timestamp has been normalized.

---

## 10. ESIOS Silver Design

All 35 configured ESIOS Bronze datasets share the same validated
`indicator` structure.

Validated `indicator` fields:

```text
composited
disaggregated
geos
id
magnitud
name
short_name
step_type
tiempo
values
values_updated_at
```

For 33 of the 35 datasets, the validated `values` records contain:

```text
datetime
datetime_utc
geo_id
geo_name
tz_time
value
```

Two datasets contained an empty `values` list in the validated Bronze load:

```text
demanda_en_consumo
demanda_medida_discriminacion_horaria_total
```

An empty `values` list must not be replaced with synthetic records.

Validated ESIOS examples demonstrate different combinations of temporal and
geographical granularity using the same internal structure.

Examples validated during design include:

- hourly province-level generation;
- five-minute national generation;
- five-minute peninsular demand;
- monthly autonomous-community installed capacity.

Silver preserves:

- indicator identity;
- timestamp;
- value;
- magnitude/unit information when supplied;
- real geographical information;
- original temporal granularity.

Selection of indicators for specific analytical products is deferred to Gold.

### Validated ESIOS Classification

The 35 configured datasets were classified from their real `magnitud` and
`tiempo` metadata:

```text
14 datasets -> Energía  / Hora
12 datasets -> Potencia / Cinco minutos
 9 datasets -> Potencia / Mes
--------------------------------
35 datasets
```

The three Silver ESIOS tables therefore contain one row per real ESIOS
observation according to those three validated families.

The common observation-level information retained in Silver is:

```text
indicator_id
dataset
indicator_name
indicator_short_name
magnitude_id
magnitude_name
time_id
time_name
observation_timestamp
source_datetime
tz_time
esios_geo_id
esios_geo_name
value
values_updated_at
source
ingestion_timestamp
```

The exact timestamp representation of the source-specific temporal fields is
resolved by the approved temporal-normalization rules during implementation.

The following indicator-level metadata remains preserved in Bronze and is not
replicated in every Silver observation row:

```text
composited
disaggregated
step_type
geos
```

The `geos` collection describes the indicator's available geographies; the
actual observation already carries its real `geo_id` and `geo_name`.

No additional ESIOS indicator-master table is introduced. The approved design
remains at 12 Silver tables.

---

## 11. Integration Rules

### 11.1 Meteorology to Meteorology

AEMET and Open-Meteo coexist as separate normalized sources in Silver.

Their roles are different:

```text
Open-Meteo
→ reproducible historical meteorological source

AEMET current observations
→ official hourly observation / contrast source

AEMET daily climatology
→ official daily climatological reference
```

Silver does not perform premature analytical aggregation or source
replacement.

### 11.2 Meteorology to Energy

The principal analytical target is:

```text
Province × hour
```

Only ESIOS indicators whose real temporal and geographical granularity is
compatible with that product may participate directly in that Gold analysis.

Other flows remain independent:

```text
5-minute flow
15-minute flow
hourly flow
monthly flow
```

Silver does not manufacture geographical detail or temporal resolution.

Final analytical aggregation and source selection are performed in Gold.

---

## 12. Iceberg Silver Tables

The approved Silver design contains 12 Iceberg tables.

### AEMET

#### `silver_aemet_stations`

Granularity:

```text
snapshot / master
```

Natural key:

```text
station_id
```

Partitioning:

```text
none
```

#### `silver_aemet_daily_climatology`

Granularity:

```text
daily
```

Use:

```text
official climatological reference
NOT part of the main analytical flow
```

Natural key:

```text
station_id + observation_date
```

Partitioning:

```text
month
```

#### `silver_aemet_current_observations`

Granularity:

```text
1 hour
```

Use:

```text
official incremental hourly observation / contrast
```

Natural key:

```text
station_id + observation_timestamp
```

Partitioning:

```text
day
```

### Open-Meteo

#### `silver_open_meteo_hourly`

Granularity:

```text
1 hour
```

Natural key:

```text
station_id + observation_timestamp
```

Partitioning:

```text
day
```

#### `silver_open_meteo_historical_forecast`

Granularity:

```text
1 hour
```

Natural key:

```text
station_id + observation_timestamp
```

Partitioning:

```text
day
```

#### `silver_open_meteo_15min`

Granularity:

```text
15 minutes
```

Natural key:

```text
station_id + observation_timestamp
```

Partitioning:

```text
day
```

### CNIG

#### `silver_cnig_provinces`

Granularity:

```text
snapshot / master
```

Natural key:

```text
province_code
```

Partitioning:

```text
none
```

#### `silver_cnig_autonomous_communities`

Granularity:

```text
snapshot / master
```

Natural key:

```text
autonomous_community_code
```

Partitioning:

```text
none
```

#### `silver_cnig_municipalities`

Granularity:

```text
snapshot / master
```

Natural key:

```text
municipality_ine_code
```

Validated municipality code mapping:

```text
municipality_code     <- COD_GEO
municipality_ine_code <- COD_INE
relation_id           <- ID_REL
```

`municipality_code` is preserved as the five-character municipal geographic
code from CNIG, but it is not used as the natural key because the validated
dataset contains 9 records with `COD_GEO = 00000`.

Validation evidence:

```text
COD_INE  = 8132 non-empty / 8132 distinct / 0 duplicates
ID_REL   = 8132 non-empty / 8132 distinct / 0 duplicates
COD_GEO  = 8132 non-empty / 8124 distinct / 8 duplicates
COD_GEO = 00000 in 9 records
```

Partitioning:

```text
none
```

### ESIOS

#### `silver_esios_energy_hourly`

Granularity:

```text
1 hour
```

Natural key:

```text
indicator_id + esios_geo_id + observation_timestamp
```

Partitioning:

```text
day
```

#### `silver_esios_power_5min`

Granularity:

```text
5 minutes
```

Natural key:

```text
indicator_id + esios_geo_id + observation_timestamp
```

Partitioning:

```text
day
```

#### `silver_esios_installed_capacity_monthly`

Granularity:

```text
monthly
```

Natural key:

```text
indicator_id + esios_geo_id + observation_timestamp
```

Partitioning:

```text
month
```

### Silver Granularity Principle

Silver preserves the original granularity of each dataset.

No aggregations from:

```text
5 min -> 15 min -> hour
```

or:

```text
hour -> month
```

are performed in Silver.

Required analytical aggregations are implemented later in Gold.

---


### 12.1 Physical Schema Decisions

The physical schemas below are based on validated Bronze payloads and approved
Silver design decisions. Silver preserves source variables required for a
reusable normalized layer; Gold is responsible for analytical selection.

#### Open-Meteo physical schemas

##### `silver_open_meteo_hourly`

| Column | Type | Required |
|---|---|---|
| `station_id` | STRING | Yes |
| `observation_timestamp` | TIMESTAMP | Yes |
| `station_name` | STRING | Yes |
| `province` | STRING | Yes |
| `latitude` | DOUBLE | Yes |
| `longitude` | DOUBLE | Yes |
| `elevation` | DOUBLE | No |
| `temperature_2m` | DOUBLE | No |
| `relative_humidity_2m` | BIGINT | No |
| `dew_point_2m` | DOUBLE | No |
| `precipitation` | DOUBLE | No |
| `pressure_msl` | DOUBLE | No |
| `surface_pressure` | DOUBLE | No |
| `cloud_cover` | BIGINT | No |
| `shortwave_radiation` | DOUBLE | No |
| `direct_radiation` | DOUBLE | No |
| `diffuse_radiation` | DOUBLE | No |
| `direct_normal_irradiance` | DOUBLE | No |
| `sunshine_duration` | DOUBLE | No |
| `wind_speed_10m` | DOUBLE | No |
| `wind_direction_10m` | BIGINT | No |
| `wind_gusts_10m` | DOUBLE | No |
| `source` | STRING | Yes |
| `ingestion_timestamp` | TIMESTAMP | Yes |

Natural key: `station_id + observation_timestamp`.

Partitioning: day.

##### `silver_open_meteo_historical_forecast`

| Column | Type | Required |
|---|---|---|
| `station_id` | STRING | Yes |
| `observation_timestamp` | TIMESTAMP | Yes |
| `station_name` | STRING | Yes |
| `province` | STRING | Yes |
| `latitude` | DOUBLE | Yes |
| `longitude` | DOUBLE | Yes |
| `elevation` | DOUBLE | No |
| `wind_speed_80m` | DOUBLE | No |
| `wind_direction_80m` | BIGINT | No |
| `wind_speed_120m` | DOUBLE | No |
| `wind_direction_120m` | BIGINT | No |
| `source` | STRING | Yes |
| `ingestion_timestamp` | TIMESTAMP | Yes |

Natural key: `station_id + observation_timestamp`.

Partitioning: day.

##### `silver_open_meteo_15min`

The Bronze metadata field `location_id` is normalized to `station_id`.

| Column | Type | Required |
|---|---|---|
| `station_id` | STRING | Yes |
| `observation_timestamp` | TIMESTAMP | Yes |
| `station_name` | STRING | Yes |
| `province` | STRING | Yes |
| `latitude` | DOUBLE | Yes |
| `longitude` | DOUBLE | Yes |
| `elevation` | DOUBLE | No |
| `temperature_2m` | DOUBLE | No |
| `relative_humidity_2m` | BIGINT | No |
| `dew_point_2m` | DOUBLE | No |
| `precipitation` | DOUBLE | No |
| `pressure_msl` | DOUBLE | No |
| `surface_pressure` | DOUBLE | No |
| `cloud_cover` | BIGINT | No |
| `shortwave_radiation` | DOUBLE | No |
| `direct_radiation` | DOUBLE | No |
| `diffuse_radiation` | DOUBLE | No |
| `direct_normal_irradiance` | DOUBLE | No |
| `sunshine_duration` | DOUBLE | No |
| `wind_speed_10m` | DOUBLE | No |
| `wind_direction_10m` | BIGINT | No |
| `wind_gusts_10m` | DOUBLE | No |
| `wind_speed_80m` | DOUBLE | No |
| `wind_direction_80m` | BIGINT | No |
| `wind_speed_120m` | DOUBLE | No |
| `wind_direction_120m` | BIGINT | No |
| `source` | STRING | Yes |
| `ingestion_timestamp` | TIMESTAMP | Yes |

Natural key: `station_id + observation_timestamp`.

Partitioning: day.

Validated sample consistency:

```text
weather_hourly              -> 96 timestamps
weather_historical_forecast -> 96 timestamps
weather_15min               -> 384 timestamps
```

The inspected arrays had consistent lengths and no `NULL` values. This does
not convert all measurement columns into mandatory fields: Silver still allows
valid source-level missing measurements.

#### CNIG physical schemas

##### `silver_cnig_provinces`

| Column | Bronze field | Type | Required |
|---|---|---|---|
| `province_code` | `COD_PROV` | STRING | Yes |
| `province_name` | `PROVINCIA` | STRING | Yes |
| `autonomous_community_code` | `COD_CA` | STRING | Yes |
| `autonomous_community_name` | `COMUNIDAD_AUTONOMA` | STRING | Yes |
| `capital_name` | `CAPITAL` | STRING | Yes |
| `source` | technical traceability | STRING | Yes |
| `ingestion_timestamp` | technical traceability | TIMESTAMP | Yes |

Natural key: `province_code`.

Partitioning: none.

##### `silver_cnig_autonomous_communities`

Derived from the unique `COD_CA + COMUNIDAD_AUTONOMA` pairs in the validated
province master.

| Column | Type | Required |
|---|---|---|
| `autonomous_community_code` | STRING | Yes |
| `autonomous_community_name` | STRING | Yes |
| `source` | STRING | Yes |
| `ingestion_timestamp` | TIMESTAMP | Yes |

Validated result:

```text
19 autonomous communities
CODES_WITH_MULTIPLE_NAMES = 0
```

Natural key: `autonomous_community_code`.

Partitioning: none.

##### `silver_cnig_municipalities`

All 18 validated source fields are retained in Silver.

| Column | Bronze field | Type | Required |
|---|---|---|---|
| `municipality_ine_code` | `COD_INE` | STRING | Yes |
| `relation_id` | `ID_REL` | STRING | Yes |
| `municipality_code` | `COD_GEO` | STRING | Yes |
| `province_code` | `COD_PROV` | STRING | Yes |
| `province_name` | `PROVINCIA` | STRING | Yes |
| `municipality_name` | `NOMBRE_ACTUAL` | STRING | Yes |
| `municipality_population` | `POBLACION_MUNI` | LONG | Yes |
| `surface_area` | `SUPERFICIE` | DOUBLE | Yes |
| `perimeter` | `PERIMETRO` | LONG | Yes |
| `capital_ine_code` | `COD_INE_CAPITAL` | STRING | Yes |
| `capital_name` | `CAPITAL` | STRING | Yes |
| `capital_population` | `POBLACION_CAPITAL` | LONG | Yes |
| `mtn25_sheet` | `HOJA_MTN25` | STRING | Yes |
| `longitude` | `LONGITUD_ETRS89_REGCAN95` | DOUBLE | Yes |
| `latitude` | `LATITUD_ETRS89_REGCAN95` | DOUBLE | Yes |
| `coordinate_origin` | `ORIGENCOOR` | STRING | Yes |
| `altitude` | `ALTITUD` | DOUBLE | Yes |
| `altitude_origin` | `ORIGENALTITUD` | STRING | Yes |
| `source` | technical traceability | STRING | Yes |
| `ingestion_timestamp` | technical traceability | TIMESTAMP | Yes |

Decimal-comma values are normalized to numeric types. Official codes remain
strings to preserve leading zeroes.

Validated code mapping:

```text
municipality_code     <- COD_GEO
municipality_ine_code <- COD_INE
relation_id           <- ID_REL
```

`municipality_code` is retained as source geography information, but it is not
a valid natural key in the validated CNIG dataset because 9 municipality
records contain `COD_GEO = 00000`, producing 8 duplicates.

`municipality_ine_code` is the approved Silver natural key because the
validated Bronze dataset contains:

```text
8132 records
8132 non-empty COD_INE values
8132 distinct COD_INE values
0 COD_INE duplicates
```

`relation_id` remains a retained CNIG source attribute.

Natural key: `municipality_ine_code`.

Partitioning: none.

#### ESIOS physical schemas

The three ESIOS tables share the same observation-level logical schema and are
separated by the validated magnitude/time classification.

| Column | Source | Type |
|---|---|---|
| `indicator_id` | `indicator.id` | LONG |
| `dataset` | Bronze dataset identifier | STRING |
| `indicator_name` | `indicator.name` | STRING |
| `indicator_short_name` | `indicator.short_name` | STRING |
| `magnitude_id` | `magnitud[].id` | LONG |
| `magnitude_name` | `magnitud[].name` | STRING |
| `time_id` | `tiempo[].id` | LONG |
| `time_name` | `tiempo[].name` | STRING |
| `observation_timestamp` | `values[].datetime_utc` | TIMESTAMP |
| `source_datetime` | `values[].datetime` | normalized temporal field |
| `tz_time` | `values[].tz_time` | normalized temporal field |
| `esios_geo_id` | `values[].geo_id` | LONG |
| `esios_geo_name` | `values[].geo_name` | STRING |
| `value` | `values[].value` | DOUBLE |
| `values_updated_at` | `indicator.values_updated_at` | TIMESTAMP |
| `source` | technical traceability | STRING |
| `ingestion_timestamp` | Bronze metadata | TIMESTAMP |

Natural key for all three tables:

```text
indicator_id + esios_geo_id + observation_timestamp
```

##### `silver_esios_energy_hourly`

Validated classification:

```text
14 datasets
magnitude = Energía (13)
time      = Hora (4)
```

Partitioning: day.

##### `silver_esios_power_5min`

Validated classification:

```text
12 datasets
magnitude = Potencia (20)
time      = Cinco minutos (219)
```

Partitioning: day.

Validated geography includes national and peninsular observations depending
on the indicator. Silver preserves the real geography and does not manufacture
province-level values.

##### `silver_esios_installed_capacity_monthly`

Validated classification:

```text
9 datasets
magnitude = Potencia (20)
time      = Mes (2)
```

Partitioning: month.

Validated monthly observations include autonomous-community-level geography
and source-specific names such as `Islas Baleares`, `Islas Canarias`,
`Cataluña`, `País Vasco`, `Ceuta` and `Melilla`. Geographic normalization uses
the approved canonical geography while preserving source traceability.

#### AEMET physical schemas

The physical AEMET schemas below are based on the validated Bronze inspection.
Structural fields use the approved common Silver naming. Meteorological fields
retain their original AEMET names to preserve direct Bronze-to-Silver
traceability and avoid semantic reinterpretation.

##### `silver_aemet_stations`

Validated Bronze evidence:

```text
records = 921
fields  = 7
```

| Silver column | Bronze field | Type | Required |
|---|---|---|---|
| `station_id` | `indicativo` | STRING | Yes |
| `nombre` | `nombre` | STRING | Yes |
| `provincia` | `provincia` | STRING | Yes |
| `altitud` | `altitud` | numeric type after validation/typing | Yes |
| normalized latitude | `latitud` | DOUBLE | Yes |
| normalized longitude | `longitud` | DOUBLE | Yes |
| `indsinop` | `indsinop` | STRING | No |

`indsinop` is legitimately nullable: the validated payload contained 622 empty
values out of 921 stations.

Natural key:

```text
station_id
```

Partitioning: none.

The already validated coordinate conversion applies to all 921/921 stations.

##### `silver_aemet_daily_climatology`

Validated Bronze fields used by the implemented transformation:

```text
altitud
dir
fecha
horaHrMax
horaHrMin
horaPIntMax
horaPresMax
horaPresMin
horaracha
horatmax
horatmin
hrMax
hrMedia
hrMin
indicativo
nombre
pintMax
prec
presMax
presMin
provincia
racha
sol
tmax
tmed
tmin
velmedia
_bronze_ingestion_timestamp
```

Approved structural normalization:

```text
indicativo -> station_id
fecha      -> observation_date
altitud    -> altitud
```

The remaining AEMET meteorological field names are preserved.

The persisted Silver table was validated with 2,420 rows and 29 Silver
columns, including technical traceability fields.

Natural key:

```text
station_id + observation_date
```

Partitioning: month.

Role:

```text
official climatological reference
NOT part of the main analytical flow
```

##### `silver_aemet_current_observations`

Validated Bronze evidence:

```text
records = 9688
```

Validated Bronze fields used by the implemented transformation:

```text
alt
dmax
dmaxu
dv
dvu
fint
geo700
geo850
geo925
hr
idema
inso
lat
lon
nieve
pacutp
pliqt
prec
pres
pres_nmar
psoltp
rviento
stddv
stddvu
stdvv
stdvvu
ta
tamax
tamin
tpr
ts
tss20cm
tss5cm
ubi
vis
vmax
vmaxu
vv
vvu
_bronze_ingestion_timestamp
```

Approved structural normalization:

```text
idema -> station_id
fint  -> observation_timestamp
lat   -> latitude
lon   -> longitude
```

The remaining meteorological names are preserved exactly as AEMET field names.

The persisted Silver table was validated with:

```text
9688 rows
41 Silver columns
0 null natural keys
0 null observation timestamps
0 duplicate natural keys
0 invalid coordinates
```

Natural key:

```text
station_id + observation_timestamp
```

Partitioning: day.

Role:

```text
official incremental hourly observation / contrast
```

The validated inspection does not by itself revalidate the previously approved
source-specific treatment of AEMET `Ip`; that rule remains documented as a
previously validated transformation case and is not reconstructed from this
payload.


## 13. Silver Data Quality

The following eight Silver quality controls are approved:

1. Null natural keys.
2. Null or invalid timestamps.
3. Coordinates outside valid ranges.
4. Records without geographical/CNIG correspondence when applicable.
5. Duplicates according to the natural keys defined in this document.
6. Mandatory fields / missing values.
7. Temporal coverage and gap detection according to granularity.
8. Possible physical or structural anomalies.

### Quality Principles

Bronze always remains intact.

Values are not invented or imputed.

An allowed `NULL` does not automatically represent an error.

Duplicates are controlled in Silver.

Temporal gaps are detected but are not filled automatically.

Plausible outliers are retained.

Quality incidents are classified as:

```text
ERROR
WARNING
```

No physical `quarantine` zone is created at this stage.

Rejected records, quality metrics and rejection reasons are registered during
Silver processing.

---

## 14. Analytical Scope

The analytical scope must remain explicit throughout subsequent development.

### Main analytical product

```text
Province × hour
Meteorology + compatible ESIOS indicators
```

### High-frequency products

```text
5 minutes / 15 minutes
```

The real geographical granularity provided by the source is preserved.

### Structural products

```text
monthly
```

The real geographical granularity provided by the source is preserved.

### AEMET daily climatology

```text
reference only
NOT part of the main analytical analysis
```

---

## 15. Validation and Closure Criteria

The Silver technical design can be considered closed only after this document
has been checked against the approved design decisions and validated technical
evidence.

The final review must verify:

- no contradictory decisions;
- no invented columns;
- no invented indicator IDs;
- no invented granularities;
- no invented geographical levels;
- schemas are based on real Bronze payloads;
- the 12 approved Silver tables are represented;
- approved natural keys are represented;
- geographical normalization is represented;
- temporal normalization is represented;
- deduplication rules are represented;
- source integration rules are represented;
- the eight approved Silver quality controls are represented;
- the analytical scope is represented explicitly.

Once this review is completed and approved, section 4.2 — Silver technical
design — can be considered closed.

---
## 16. Current Design Status

```text
Physical Silver schema blocks:

AEMET       = VALIDATED
Open-Meteo  = VALIDATED
CNIG        = VALIDATED
ESIOS       = VALIDATED

Total approved Silver tables = 12
```

The detailed AEMET physical schema has been incorporated from validated Bronze
evidence.

The CNIG municipality natural-key decision has also been corrected using the
validated CNIG evidence: `municipality_ine_code` (`COD_INE`) is the natural
key, while `municipality_code` (`COD_GEO`) is retained as a non-key source
attribute.

Section 4.2 is ready for the final 4.2.14 checklist review. It is considered
closed only after that review is explicitly approved.

````


### Implementation reconciliation

During the Silver implementation, the design document was reconciled against
the schemas physically observed in Bronze and against the final Apache Iceberg
tables.

The following documentation corrections were incorporated:

- AEMET daily climatology fields updated to the real Bronze schema used by the
  implemented transformation.
- AEMET current-observation fields updated to the real Bronze schema used by
  the implemented transformation.
- Open-Meteo integer fields documented as `BIGINT` where PySpark/Iceberg
  physically materialized them as long integer types.

These changes do not modify the approved Silver architecture, natural keys,
granularities or partitioning rules. They align the documentation with the
implementation that was subsequently validated end-to-end.
