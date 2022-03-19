# hexpop
Investigating Helium network coverage by population.

Results [published here](https://heliummaps.aquacow.net/) with periodic updates.
## Purpose
More than [500,000 Helium hot spots](https://www.nytimes.com/2022/02/06/technology/helium-cryptocurrency-uses.html) have been deployed around the world, with thousands more added to the network every day. But how does that translate into actual coverage? Decisions about network deployment are decentralized, so some newly deployed hotspots provide superfluous coverage, while other areas have lingering coverage gaps.

This project seeks to discern the percentage of population covered by the [Helium](https://www.helium.com/) network by mashing up coverage and population data. Google [BigQuery](https://cloud.google.com/bigquery) is used to store and process the data, which is loaded and manipulated using Python scripts. The initial target is [choropleth](https://en.wikipedia.org/wiki/Choropleth_map) visualizations in Google [Data Studio](https://datastudio.google.com/). Several key datasets are made publicly available, so that others can extend the research in areas of interest.
## Hexes
The project data is organized using the [H3 hexagonal geospatial indexing](https://h3geo.org/) system, an open source project first developed by [Uber](https://eng.uber.com/h3/). The main hexagons have an H3 resolution of 8, equivalent to a radius of about 460 meters.
## Coverage
Helium coverage could be defined in various ways. For the purposes of this project, the population in any H3 resolution 8 hex is considered to have Helium coverage if either of these criteria is true:

* Helium [Explorer](https://explorer.helium.com/) lists at least one hotspot within the hex.
* Helium [Mappers](https://mappers.helium.com/) lists at least one uplink from at least one of its seven child H3 resolution 9 hexes.

For now, hotspots are included even if currently offline and uplinks are included even if weak or old.

Whether this definition is too loose or too strict is debateable. For example, Mappers might show one child hex has coverage, but that doesn't mean the entire parent hex has coverage. On the other hand, Explorer might show a hex has no hotspots, but that doesn't mean a hotspot in an adjacent hex can't provide coverage. That said, this definition is unambigruous and lends itself to algorithmic determination. Whatever definition is used, the change of population coverage over time is likely to be most revealing.
## Population
The population data comes from a [global population dataset](https://data.humdata.org/dataset/kontur-population-dataset) published by [Kontur](https://www.kontur.io/). Raw population data from various sources is mapped to H3 resolution 8 hexes. Last revised in November 2021, the dataset comprises 7,673,197,891 pops across 26,146,026 hexes. The dataset is published under the [Creative Commons Attribution International](https://creativecommons.org/licenses/by/4.0/legalcode) license. Thanks and appreciation to Kontur for compiling and sharing this data.
## Boundaries
Boundary data is required to determine which hexes comprise a given administrative territory (country, state, county, etc.). Such geospatial data is given as a polygon or multipolygon (collection of polygons) described by a series of latitude and longitude points. To determine which hexes a territory contains, the H3 polyfill function is used, which checks whether the centroid of each hex is inside the polygon(s).

This criterion can yield surprising corner-case results, for example, when a centroid falls in excluded water areas between two parts of a multipolygon. Thus different versions of boundary data can result in different lists of included hexes, although detailed analysis of sample differences indicates minimal impact on overall coverage calculations.

Boundary data published by governmental authorities is preferred when available.
## Data
Original raw data from public sources has been cached in Google Cloud Storage, then loaded into BigQuery tables.

Both formats are made publicly readable, as listed below.

|Type|Region|Source|Google Cloud Storage|BigQuery|
|---|---|---|---|---|
|Coverage|Global|[Explorer](https://explorer.helium.com/)<br />[Mappers](https://mappers.helium.com/)|&nbsp;&nbsp;N/A|`llang-helium.coverage.updates` [all time]<br />`llang-helium.coverage.most_recent` [most recent]|
|Population|Global|[Kontur](https://data.humdata.org/dataset/kontur-population-dataset)|[`kontur_population_20211109.gpkg`](https://storage.googleapis.com/hexpop/kontur_population_20211109.gpkg)|`llang-helium.public.kontur_population_20211109`|
|Boundary|Australia|[Australian Bureau of Statistics](https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files)|[`aus_STE_2021_AUST_SHP_GDA2020.zip`](https://storage.googleapis.com/hexpop/aus_STE_2021_AUST_SHP_GDA2020.zip)<br />[`aus_LGA_2021_AUST_GDA2020_SHP.zip`](https://storage.googleapis.com/hexpop/aus_LGA_2021_AUST_GDA2020_SHP.zip)|`llang-helium.public.aus_STE_2021_AUST_SHP_GDA2020`<br />`llang-helium.public.aus_LGA_2021_AUST_GDA2020_SHP`|
|Boundary|Canada|[Statistics Canada](https://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/bound-limit-2016-eng.cfm)|[`can_province_territory.zip`](https://storage.googleapis.com/hexpop/can_province_territory.zip)<br />[`can_census_division.zip`](https://storage.googleapis.com/hexpop/can_census_division.zip)|`llang-helium.public.can_province_territory`<br />`llang-helium.public.can_census_division`|
|Boundary|Europe|[Eurostat](https://ec.europa.eu/eurostat/web/gisco/geodata/reference-data/administrative-units-statistical-units/nuts)|[`euro_NUTS_RG_01M_2021_4326.geojson`](https://storage.googleapis.com/hexpop/euro_NUTS_RG_01M_2021_4326.geojson)|`llang-helium.public.euro_NUTS_RG_01M_2021_4326`|
|Boundary|USA|Google|&nbsp;&nbsp;N/A|`bigquery-public-data.geo_us_boundaries.states` `bigquery-public-data.geo_us_boundaries.counties`|
## Code
Python scripts and BigQuery commands that can be run locally or on a Google Compute Engine virtual machine instance.

Assumes environment has been set up for [Google Cloud CLI access](https://cloud.google.com/sdk/docs/install), including BigQuery.

Modules listed below in expected order of use.

|Module|Description|
|---|---|
|`README.md`|This file.|
|`hexpop.py`|Common functions, particularly those used to interact with the BigQuery API.|
|`public.py`|Load public data sources from Google Cloud Storage cache into BigQuery tables, with geospatial column using latitude/longitude reference system. Multiprocessing to accelerate processing of large datasets.|
|`public.ini`|Configuration for each public data source.
|`statoids.py`|Scrape [Statoids website](http://www.statoids.com/yus.html) for data about U.S. counties.|
|`geopop.py`|Assemble list of hexes with population for each region.
|`geopop.ini`|Configuration for each region. Also accessed by `views.py`.|
|`coverage.py`|Survey Explorer and Mappers APIs to determine whether a hex has coverage (see definition above). Multithreading to parallelize API requests, but regions with many hexes still require hours or days to completely update.|
|`views.py`|Join coverage and population data to create dynamic views suitable for [Data Studio geospatial visualization](https://support.google.com/datastudio/answer/7065037).|
|`summarize.py`|deprecated|
|`google-service-account.json`|Account-specific credentials to [authorize BigQuery access](https://cloud.google.com/bigquery/docs/authentication/service-account-file#python). Not recorded in Git repository.|
|`herun.sh`|Shell script for frequently run commands.|
|`hevm.sh`|Shell script for managing Google Compute Engine VM instances.
|`home.py`|Example of [H3 API](https://h3geo.org/docs/api/indexing) for hex identified by environment variable HOME_HEX.|
|`usa48.py`|Example of plotting or filtering by the 48 contiguous U.S. states.|
## Future Plans
- Repeat coverage surveys to track growth of the Helium network.
- Extend to new geographies, particularly in Asia.
- Mash up coverage with metrics other than population, such as value of agricultural production.
