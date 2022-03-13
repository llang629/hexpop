"""Summarize population coverage percentage by region sublevels."""
import logging
import pathlib

import geopop
import hexpop

GEOPOP_COVERAGE_BY_REGION = """
SELECT geopop.*, coverage.explorer_coverage, coverage.mappers_coverage
  FROM `{project}.geopop.{region}` AS geopop
INNER JOIN (SELECT * FROM `{project}.coverage.most_recent`
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
    FROM `{project}.summaries.geopop_coverage_{region}`
    WHERE explorer_coverage = TRUE OR mappers_coverage = TRUE
    GROUP BY {sem_code}) AS pop_cover
  RIGHT JOIN (
    SELECT
      {sem_code}, SUM(population) AS total
    FROM `{project}.summaries.geopop_coverage_{region}`
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
    FROM `{project}.summaries.geopop_coverage_{region}`
    WHERE explorer_coverage = TRUE OR mappers_coverage = TRUE
    GROUP BY {bis_code}) AS pop_cover
  RIGHT JOIN (
    SELECT {bis_code}, SUM(population) AS total,
      STRING_AGG({sem_code}) AS sem_list
    FROM `{project}.summaries.geopop_coverage_{region}`
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
  FROM `{project}.{dataset}.div1_usa_by_state_name*`
UNION ALL
SELECT PRUID as spt_id, province AS spt_name, percent
  FROM `{project}.{dataset}.div1_canada_by_province*`
ORDER BY percent DESC
"""

# dependency on div1_europe summary
SUMMARY_BY_COUNTRY = """
(SELECT
  country, covered, total, percent
 FROM `{project}.summaries.div1_europe_by_country*`)
UNION ALL (
  SELECT
    'USA' AS country, covered, total,
    ROUND(100*IFNULL(covered, 0)/total, 1) AS percent
  FROM (
    SELECT SUM(population) AS covered
    FROM `{project}.summaries.geopop_coverage_usa`
    WHERE explorer_coverage = TRUE OR mappers_coverage = TRUE),
    (SELECT SUM(population) AS total
     FROM `{project}.summaries.geopop_coverage_usa`))
UNION ALL (
  SELECT
    'Canada' AS country, covered, total,
    ROUND(100*IFNULL(covered, 0)/total, 1) AS percent
  FROM (
    SELECT SUM(population) AS covered
    FROM `{project}.summaries.geopop_coverage_canada`
    WHERE explorer_coverage = TRUE OR mappers_coverage = TRUE),
    (SELECT SUM(population) AS total
     FROM `{project}.summaries.geopop_coverage_canada`))
UNION ALL (
  SELECT
    'Australia' AS country, covered, total,
    ROUND(100*IFNULL(covered, 0)/total, 1) AS percent
  FROM (
    SELECT SUM(population) AS covered
    FROM `{project}.summaries.geopop_coverage_australia`
    WHERE explorer_coverage = TRUE OR mappers_coverage = TRUE),
    (SELECT SUM(population) AS total
     FROM `{project}.summaries.geopop_coverage_australia`))
ORDER BY
  percent DESC
"""


def parse_args():
    """Parse command line arguments."""
    parser = hexpop.initialize_parser(__doc__)
    parser.add_argument('regions',
                        type=str,
                        nargs='*',
                        default='all',
                        help='global regions to summarize')
    parser.add_argument('-d',
                        '--divisions',
                        type=int,
                        nargs='*',
                        choices=range(0, 3),
                        default=[0, 1, 2],
                        help=("""summary divisions
              (0: global, 1: U.S. state, Canada province, Europe country,
              2: U.S. county, Canada census district, Europe NUTS region)"""))
    parser.add_argument('-g',
                        '--geography',
                        action='store_true',
                        default=False,
                        help='include geography data (border polygons)')
    args = parser.parse_args()
    return (args.regions, args.divisions, args.geography)


def _name_table(name_list, suffix=None):
    if suffix:
        name_list.append(suffix)
    return '_'.join(name_list)


if __name__ == '__main__':
    logger = logging.getLogger(pathlib.Path(__file__).stem)
    hexpop.initialize_logging(logger)
    regions, divisions, geography = parse_args()
    regional_dataset = hexpop.bq_prep_dataset('geopop')
    coverage_dataset = hexpop.bq_prep_dataset('coverage')
    summary_dataset = hexpop.bq_prep_dataset('summaries')

    if geography:
        geoignore_start, geoignore_stop = '', ''
        geo_suffix = 'geo'
    else:
        geoignore_start, geoignore_stop = '/*', '*/'
        geo_suffix = None

    for region in hexpop.clean_regions(regions):
        if region == 'gadm':
            continue
        params = geopop.parse_ini(region)
        logger.info(
            '%s for %s joined with most recent %s',
            regional_dataset.dataset_id,
            region,
            coverage_dataset.dataset_id,
        )
        geopop_coverage_table = hexpop.bq_create_table(
            summary_dataset,
            _name_table([
                regional_dataset.dataset_id, coverage_dataset.dataset_id,
                region
            ]),
            force_new=True)
        hexpop.bq_query_table(GEOPOP_COVERAGE_BY_REGION.format(
            project=regional_dataset.project,
            region=region,
            bis_code=params.bis_code),
                              destination=geopop_coverage_table)

        if 1 in divisions:
            logger.info('division 1 for %s by %s', region, params.sem_admin)
            summary_sem_table = hexpop.bq_create_table(
                summary_dataset,
                _name_table(
                    ['div1', region, 'by',
                     params.sem_admin.replace(' ', '_')], geo_suffix),
                force_new=True)
            hexpop.bq_query_table(SUMMARY_BY_SEM.format(
                project=regional_dataset.project,
                region=region,
                sem_admin=params.sem_admin,
                sem_code=params.sem_code,
                sem_name=params.sem_name,
                sem_geom=params.sem_geom,
                geoignore_start=geoignore_start,
                geoignore_stop=geoignore_stop,
                sem_source=params.sem_source.format(
                    project=regional_dataset.project)),
                                  destination=summary_sem_table)

        if 2 in divisions:
            logger.info('division 2 for %s by %s', region, params.bis_admin)
            summary_bis_table = hexpop.bq_create_table(
                summary_dataset,
                _name_table(
                    ['div2', region, 'by',
                     params.bis_admin.replace(' ', '_')], geo_suffix),
                force_new=True)
            hexpop.bq_query_table(SUMMARY_BY_BIS.format(
                project=regional_dataset.project,
                region=region,
                sem_admin=params.sem_admin,
                sem_code=params.sem_code,
                sem_name=params.sem_name,
                bis_admin=params.bis_admin.replace(' ', '_'),
                bis_code=params.bis_code,
                bis_name=params.bis_name,
                bis_geom=params.bis_geom,
                geoignore_start=geoignore_start,
                geoignore_stop=geoignore_stop,
                bis_source=params.bis_source.format(
                    project=regional_dataset.project)),
                                  destination=summary_bis_table)

    if 1 in divisions and set(['usa', 'europe']) <= set(
            hexpop.clean_regions(regions)):
        logger.info('division 1 combining usa and canada')
        combo_usa_canada_table = hexpop.bq_create_table(
            summary_dataset,
            "div1_usa_canada_by_state_province",
            force_new=True)
        hexpop.bq_query_table(SUMMARY_COMBO_USA_CANADA.format(
            project=summary_dataset.project,
            dataset=summary_dataset.dataset_id),
                              destination=combo_usa_canada_table)

    if 0 in divisions and set(['europe']) <= set(
            hexpop.clean_regions(regions)):
        logger.info('division 0 global by country')
        summary_country_table = hexpop.bq_create_table(
            summary_dataset, "div0_global_by_country", force_new=True)
        hexpop.bq_query_table(
            SUMMARY_BY_COUNTRY.format(project=regional_dataset.project),
            destination=summary_country_table)
