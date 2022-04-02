#!/bin/bash
public() {
  # public data files
  echo GCP instance type recommended e2-highmem-16 for memory and performance
  echo -n "Install kontur population data? (requires memory and time) [yes|No] "
  local KONTUR
  read KONTUR
  echo -n "Install GADM boundaries data? (requires memory and time) [yes|No] "
  local GADM
  read GADM
  if [ "$KONTUR" = "yes" ]; then
    python3 public.py kontur  # global population allocated to each H3 hex
  fi
  if [ "$GADM" = "yes" ]; then
    python3 public.py gadm  # global population allocated to each H3 hex
  fi
  python3 public.py eurostat  # European georaphic boundaries
  python3 public.py statcan-province  # Canadian provinces geographc boundaries
  python3 public.py statcan-census  # Canadian census districts geographc boundaries
  python3 public.py abs-state  # Australian states and territories boundaries
  python3 public.py abs-lga  # Australian local government areas boundaries
  python3 statoids.py  # US counties for HASC-FIPS translation
  # countries by ISO 3166 geocode, patches for UK and Greece
  bq query --nouse_legacy_sql 'CREATE OR REPLACE TABLE public.countries_iso3166 AS
  (SELECT * FROM`fh-bigquery.util.country_emoji_flags` UNION ALL
  SELECT "UK" AS iso, "uk" AS iso_lower, emoji, unicode, name
  FROM `fh-bigquery.util.country_emoji_flags` WHERE iso="GB" UNION ALL
  SELECT "EL" AS iso, "el" AS iso_lower, emoji, unicode, name
  FROM `fh-bigquery.util.country_emoji_flags` WHERE iso="GR")'
}

geopop() {
  python3 geopop.py  # map each hex with its population to region
}

coverage() {
  echo GCP instance type e2-medium recommended for frugal io-bound processes
  expire=$1
  expire="${expire:=7}" # default expiration seven days
  python3 coverage.py -x $expire # fetch coverage status for each hex by region
}

views() {
  python3 views.py  # create dynamic views of population coverage by region
}

regions() {
  # BigQuery view rolling up statistics for each region
  echo old
  bq query --nouse_legacy_sql "SELECT * FROM `bq show | grep Project | cut -d ' ' -f 2`.views.region_stats"
  echo new
  bq query --nouse_legacy_sql "SELECT * FROM `bq show | grep Project | cut -d ' ' -f 2`.views.region_stats_new"
}

case "$1" in
  setup)
    setup
    ;;
  geopop)
    geopop
    ;;
  coverage)
    coverage $2
    ;;
  views)
    views
    ;;
  regions)
    regions $2
    ;;
  *)
    echo "Usage: $0 {public|geopop|coverage|views|regions}"
esac
