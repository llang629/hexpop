"""Create dynamic views of population coverage percentage by region levels."""
import logging
import pathlib

import coverexp
import covermap
import geopop
import hexpop

MOST_RECENT = """
WITH most_recent AS (SELECT
mappers.h3_index AS h3_index,
IFNULL(explorer.explorer_coverage, FALSE) AS explorer_coverage,
IFNULL(mappers.mappers_coverage, FALSE) AS mappers_coverage,
LEAST(IFNULL(explorer.update_time, mappers.update_time),
      IFNULL(mappers.update_time, explorer.update_time)) AS update_time
FROM (SELECT *
  FROM `{project}.{coverage_dataset}.most_recent_explorer`) AS explorer
RIGHT JOIN (
  SELECT * FROM `{project}.{coverage_dataset}.most_recent_mappers`) AS mappers
  ON explorer.h3_index = mappers.h3_index)
SELECT * FROM (
  SELECT h3_index, 'australia' AS region, * EXCEPT (h3_index) FROM most_recent
    WHERE h3_index IN
    (SELECT h3_index FROM `{project}.{regional_dataset}.australia`)
  UNION ALL
  SELECT h3_index, 'canada' AS region, * EXCEPT (h3_index) FROM most_recent
    WHERE h3_index IN
    (SELECT h3_index FROM `{project}.{regional_dataset}.canada`)
  UNION ALL
  SELECT h3_index, 'europe' AS region, * EXCEPT (h3_index) FROM most_recent
    WHERE h3_index IN
    (SELECT h3_index FROM `{project}.{regional_dataset}.europe`)
  UNION ALL
  SELECT h3_index, 'usa' AS region, * EXCEPT (h3_index) FROM most_recent
    WHERE h3_index IN
    (SELECT h3_index FROM `{project}.{regional_dataset}.usa`))
"""

COVERED_HEXES = """
SELECT h3_index FROM (
    SELECT * FROM `{project}.{coverage_dataset}.most_recent_explorer`
) WHERE explorer_coverage
UNION DISTINCT
SELECT h3_index FROM (
    SELECT * FROM `{project}.{coverage_dataset}.most_recent_mappers`
) WHERE mappers_coverage
"""

REGION_STATS = """
SELECT
  IF(ARRAY_LENGTH(region) = 1, region[OFFSET(0)],'total') AS region,
  * EXCEPT (region),
  ROUND(100*IFNULL(pop_cover, 0)/pop_total, 1) AS percent
FROM (
  SELECT
    ARRAY_AGG(DISTINCT region) AS region,
    COUNT(cover.h3_index) AS h3_indices,
    MIN(update_time) AS earliest_update_time,
    MAX(update_time) AS latest_update_time,
    SUM(pop.population) AS pop_total,
    SUM(CASE WHEN explorer_coverage OR mappers_coverage
      THEN pop.population ELSE 0 END) AS pop_cover
  FROM (
    SELECT * FROM `{project}.{coverage_dataset}.most_recent`) AS cover
    LEFT JOIN `{project}.public.kontur_population_20211109` AS pop
    ON cover.h3_index = pop.h3_index
  GROUP BY
    ROLLUP (cover.region) )
ORDER BY
  pop_total
"""

GEOPOP_COVERAGE_BY_REGION = """
SELECT geopop.*, coverage.explorer_coverage, coverage.mappers_coverage
  FROM `{project}.{regional_dataset}.{region}` AS geopop
INNER JOIN (SELECT * FROM `{project}.{coverage_dataset}.most_recent`
  WHERE region='{region}') AS coverage
ON geopop.h3_index = coverage.h3_index
ORDER BY {bis_code}, h3_index
"""

SUMMARY_BY_SEM = """
SELECT
  summary_sem.*,
  sem.{sem_name} as {sem_admin}
  {geoignore_start}, sem.{sem_geom}{geoignore_stop}
FROM (
  SELECT
    pop_total.{sem_code},
    IFNULL(pop_cover.covered, 0) AS covered,
    pop_total.total,
    ROUND(100*IFNULL(pop_cover.covered, 0)/pop_total.total, 1) AS percent
  FROM (
    SELECT {sem_code}, SUM(population) AS covered
    FROM `{project}.{views_dataset}.geopop_coverage_{region}`
    WHERE explorer_coverage = TRUE OR mappers_coverage = TRUE
    GROUP BY {sem_code}) AS pop_cover
  RIGHT JOIN (
    SELECT
      {sem_code}, SUM(population) AS total
    FROM `{project}.{views_dataset}.geopop_coverage_{region}`
    GROUP BY {sem_code}) AS pop_total
  ON
    pop_cover.{sem_code} = pop_total.{sem_code}) AS summary_sem
INNER JOIN
  {sem_source} AS sem
ON
  summary_sem.{sem_code} = sem.{sem_code}
ORDER BY
  percent DESC
"""

SUMMARY_BY_BIS = """
SELECT
  summary_bis.*,
  bis.{bis_name} as {bis_admin},
  bis.{sem_name} as {sem_admin}
  {geoignore_start}, bis.{bis_geom}{geoignore_stop}
FROM (
  SELECT SPLIT(pop_total.sem_list, ',')[OFFSET(0)] AS {sem_code},
    pop_total.{bis_code},
    IFNULL(pop_cover.covered, 0) AS covered,
    pop_total.total,
    ROUND(100*IFNULL(pop_cover.covered, 0)/pop_total.total, 1) AS percent
  FROM (
    SELECT {bis_code}, SUM(population) AS covered
    FROM `{project}.{views_dataset}.geopop_coverage_{region}`
    WHERE explorer_coverage = TRUE OR mappers_coverage = TRUE
    GROUP BY {bis_code}) AS pop_cover
  RIGHT JOIN (
    SELECT {bis_code}, SUM(population) AS total,
      STRING_AGG({sem_code}) AS sem_list
    FROM `{project}.{views_dataset}.geopop_coverage_{region}`
    GROUP BY {bis_code}) AS pop_total
  ON
    pop_cover.{bis_code} = pop_total.{bis_code} ) AS summary_bis
LEFT JOIN
  {bis_source} AS bis
ON
  summary_bis.{bis_code} = bis.{bis_code}
ORDER BY percent DESC
"""

SUMMARY_COMBO_USA_CANADA = """
SELECT state as spt_id, state_name AS spt_name, percent
  FROM `{project}.{views_dataset}.div1_usa_by_state_name`
UNION ALL
SELECT PRUID as spt_id, province AS spt_name, percent
  FROM `{project}.{views_dataset}.div1_canada_by_province`
ORDER BY percent DESC
"""

# dependency on div1_europe summary
SUMMARY_BY_COUNTRY = """
(SELECT
  country, covered, total, percent
 FROM `{project}.{views_dataset}.div1_europe_by_country`)
UNION ALL (
  SELECT
    'USA' AS country, covered, total,
    ROUND(100*IFNULL(covered, 0)/total, 1) AS percent
  FROM (
    SELECT SUM(population) AS covered
    FROM `{project}.{views_dataset}.geopop_coverage_usa`
    WHERE explorer_coverage = TRUE OR mappers_coverage = TRUE),
    (SELECT SUM(population) AS total
     FROM `{project}.{views_dataset}.geopop_coverage_usa`))
UNION ALL (
  SELECT
    'Canada' AS country, covered, total,
    ROUND(100*IFNULL(covered, 0)/total, 1) AS percent
  FROM (
    SELECT SUM(population) AS covered
    FROM `{project}.{views_dataset}.geopop_coverage_canada`
    WHERE explorer_coverage = TRUE OR mappers_coverage = TRUE),
    (SELECT SUM(population) AS total
     FROM `{project}.{views_dataset}.geopop_coverage_canada`))
UNION ALL (
  SELECT
    'Australia' AS country, covered, total,
    ROUND(100*IFNULL(covered, 0)/total, 1) AS percent
  FROM (
    SELECT SUM(population) AS covered
    FROM `{project}.{views_dataset}.geopop_coverage_australia`
    WHERE explorer_coverage = TRUE OR mappers_coverage = TRUE),
    (SELECT SUM(population) AS total
     FROM `{project}.{views_dataset}.geopop_coverage_australia`))
ORDER BY
  percent DESC
"""


def _create_id(name_list, suffix=None):
    if suffix:
        name_list.append(suffix)
    return '_'.join(name_list)


def _geom_add(include):
    if include:
        return '', '', 'geo'
    return '/*', '*/', None


if __name__ == '__main__':
    logger = logging.getLogger(pathlib.Path(__file__).stem)
    hexpop.initialize_logging(logger)
    geopop_dataset = hexpop.bq_prep_dataset('geopop')
    coverage_dataset = hexpop.bq_prep_dataset('coverage')
    views_dataset = hexpop.bq_prep_dataset('views')

    logger.info("most_recent_explorer")
    coverexp.query_explorer_coverage()
    logger.info("most_recent_mappers")
    covermap.query_mappers_coverage()

    view_id = 'most_recent'
    logger.info(view_id)
    region_stats = hexpop.bq_create_view(
        coverage_dataset,
        view_id,
        MOST_RECENT.format(project=coverage_dataset.project,
                           coverage_dataset=coverage_dataset.dataset_id,
                           regional_dataset=geopop_dataset.dataset_id),
        force_new=True)

    view_id = 'covered_hexes'
    logger.info(view_id)
    region_stats = hexpop.bq_create_view(
        views_dataset,
        view_id,
        COVERED_HEXES.format(project=coverage_dataset.project,
                             coverage_dataset=coverage_dataset.dataset_id),
        force_new=True)

    view_id = 'region_stats'
    logger.info(view_id)
    region_stats = hexpop.bq_create_view(
        views_dataset,
        view_id,
        REGION_STATS.format(project=coverage_dataset.project,
                            coverage_dataset=coverage_dataset.dataset_id),
        force_new=True)

    for region in hexpop.clean_regions('all'):
        if region == 'gadm':
            continue
        params = geopop.parse_ini(region)
        view_id = _create_id(
            [geopop_dataset.dataset_id, coverage_dataset.dataset_id, region])
        logger.info(view_id)
        query = GEOPOP_COVERAGE_BY_REGION.format(
            project=coverage_dataset.project,
            coverage_dataset=coverage_dataset.dataset_id,
            regional_dataset=geopop_dataset.dataset_id,
            region=region,
            bis_code=params.bis_code)
        hexpop.bq_create_view(views_dataset, view_id, query, force_new=True)

        geoignore_start, geoignore_stop, geo_suffix = _geom_add(
            params.sem_geom_include)
        view_id = _create_id(
            ['div1', region, 'by',
             params.sem_admin.replace(' ', '_')], geo_suffix)
        logger.info(view_id)
        query = SUMMARY_BY_SEM.format(project=geopop_dataset.project,
                                      views_dataset=views_dataset.dataset_id,
                                      region=region,
                                      sem_admin=params.sem_admin,
                                      sem_code=params.sem_code,
                                      sem_name=params.sem_name,
                                      sem_geom=params.sem_geom,
                                      geoignore_start=geoignore_start,
                                      geoignore_stop=geoignore_stop,
                                      sem_source=params.sem_source.format(
                                          project=geopop_dataset.project))
        hexpop.bq_create_view(views_dataset, view_id, query, force_new=True)

        geoignore_start, geoignore_stop, geo_suffix = _geom_add(
            params.bis_geom_include)
        view_id = _create_id(
            ['div2', region, 'by',
             params.bis_admin.replace(' ', '_')], geo_suffix)
        logger.info(view_id)
        query = SUMMARY_BY_BIS.format(project=geopop_dataset.project,
                                      views_dataset=views_dataset.dataset_id,
                                      region=region,
                                      sem_admin=params.sem_admin,
                                      sem_code=params.sem_code,
                                      sem_name=params.sem_name,
                                      bis_admin=params.bis_admin.replace(
                                          ' ', '_'),
                                      bis_code=params.bis_code,
                                      bis_name=params.bis_name,
                                      bis_geom=params.bis_geom,
                                      geoignore_start=geoignore_start,
                                      geoignore_stop=geoignore_stop,
                                      bis_source=params.bis_source.format(
                                          project=geopop_dataset.project))
        hexpop.bq_create_view(views_dataset, view_id, query, force_new=True)

    view_id = 'div1_usa_canada_by_state_province'
    logger.info(view_id)
    query = SUMMARY_COMBO_USA_CANADA.format(
        project=views_dataset.project, views_dataset=views_dataset.dataset_id)
    hexpop.bq_create_view(views_dataset, view_id, query, force_new=True)

    view_id = 'div0_global_by_country'
    logger.info(view_id)
    query = SUMMARY_BY_COUNTRY.format(
        project=geopop_dataset.project,
        views_dataset=views_dataset.dataset_id,
    )
    hexpop.bq_create_view(views_dataset, view_id, query, force_new=True)
