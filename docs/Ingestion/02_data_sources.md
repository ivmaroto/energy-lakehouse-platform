# Data Sources

## 1. Overview

The Energy Lakehouse Platform integrates public meteorological and energy data
from three external sources:

- AEMET OpenData.
- Open-Meteo.
- REE / ESIOS.

These sources provide the information required to analyse the relationship
between meteorological conditions and the Spanish electricity system.

The ingestion layer is responsible for retrieving the data from each source and
storing the acquired information in the Bronze layer before subsequent
transformation and integration processes are applied.

---

## 2. AEMET OpenData

### 2.1 Description

AEMET (Agencia Estatal de Meteorología) is the Spanish national meteorological
agency.

AEMET OpenData provides programmatic access to official meteorological datasets
through a REST API.

The platform uses AEMET as one of its primary meteorological data sources.

### 2.2 Authentication

Access to AEMET OpenData requires an API key.

The credential is provided to the application through the following environment
variable:

```text
AEMET_API_KEY
```

The API key must never be stored directly in the source code or committed to
the Git repository.

### 2.3 Data acquisition

The AEMET connector is responsible for:

- Sending authenticated requests to AEMET OpenData.
- Retrieving the required meteorological datasets.
- Validating the API response.
- Downloading the data returned by the service.
- Passing the acquired information to the Bronze persistence layer.

### 2.4 Role in the platform

AEMET provides official meteorological observations that will later be
normalized and geographically integrated with the energy datasets.

No analytical transformations are performed during the ingestion stage.

---

## 3. Open-Meteo

### 3.1 Description

Open-Meteo is an open meteorological API providing access to current and
historical weather information.

It is used as an additional meteorological source in the platform.

### 3.2 Authentication

The Open-Meteo endpoints used by this project do not require an API key.

This allows the connector to access the service directly using HTTP requests.

### 3.3 API services

Two types of access are considered by the ingestion layer:

```text
Forecast / current API
Historical archive API
```

The forecast/current service provides recent meteorological information, while
the archive service allows historical periods to be requested.

### 3.4 Data acquisition

The Open-Meteo connector is responsible for:

- Building requests using geographical coordinates.
- Selecting the required meteorological variables.
- Requesting historical or current data depending on the ingestion mode.
- Validating the returned response.
- Passing the acquired information to the Bronze persistence layer.

### 3.5 Role in the platform

Open-Meteo complements the meteorological information obtained from AEMET.

The resulting datasets will later be normalized and integrated during the
Silver-layer processing stage.

---

## 4. REE / ESIOS

### 4.1 Description

REE / ESIOS provides public information related to the operation of the Spanish
electricity system and electricity markets.

It represents the main energy data source of the platform.

### 4.2 Authentication

Access to the selected API services requires an access credential.

The credential is provided to the application using the following environment
variable:

```text
ESIOS_API_KEY
```

The real credential must not be stored in the source code or committed to the
Git repository.

### 4.3 Energy information

The platform is designed to acquire the energy information required by the
analytical use case, including datasets related to:

- Electricity generation.
- Electricity demand.
- Energy prices.

The exact indicators and endpoints used by the implementation are documented
alongside the connector once they have been technically validated against the
API.

### 4.4 Data acquisition

The ESIOS connector is responsible for:

- Authenticating requests.
- Requesting the selected energy indicators.
- Managing requested time ranges.
- Validating API responses.
- Passing the acquired information to the Bronze persistence layer.

### 4.5 Role in the platform

REE / ESIOS provides the energy dimension of the analytical model.

These datasets will subsequently be integrated with meteorological information
to enable analysis of relationships between weather conditions, electricity
generation, demand and energy prices.

---

## 5. Geographic Scope

The final analytical scope of the project is Spain, with the target analytical
aggregation defined at autonomous-community level.

The external sources do not necessarily provide information using the same
geographical structure.

Therefore, geographical normalization is not performed by the ingestion layer.

The ingestion layer preserves the source representation, while geographical
mapping and harmonization are performed during subsequent Lakehouse processing.

---

## 6. Temporal Scope

The platform supports two different acquisition scenarios:

### Historical data

Historical information is retrieved to create the initial dataset of the
platform.

The available historical range may vary depending on the source and dataset.

### Incremental data

After the initial historical load, the platform retrieves newly available data
periodically.

The incremental strategy avoids downloading the complete historical dataset
during every execution.

---

## 7. Source Comparison

| Source | Domain | Authentication | Historical data | Incremental acquisition |
|---|---|---|---|---|
| AEMET | Meteorological | API key | Yes | Yes |
| Open-Meteo | Meteorological | Not required | Yes | Yes |
| REE / ESIOS | Energy | Access credential | Yes | Yes |

---

## 8. Source Independence

Each data source is implemented using an independent connector.

```text
AEMET --------> AEMET connector --------\
                                          \
Open-Meteo ---> Open-Meteo connector -----> Bronze
                                          /
REE / ESIOS --> ESIOS connector ----------/
```

This design prevents changes in one external API from directly affecting the
other ingestion processes.

Common functionality such as HTTP communication, logging, configuration and
storage is shared through the `ingestion.common` package.

---

## 9. Data Preservation

During ingestion, the objective is to preserve the information supplied by each
source with minimal modification.

The Bronze layer therefore acts as the initial landing area for external data.

Operations such as:

- Data cleaning.
- Unit standardization.
- Geographic harmonization.
- Cross-source integration.
- Analytical calculations.

are intentionally excluded from the ingestion layer and are performed during
later Lakehouse processing stages.