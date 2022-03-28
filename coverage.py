"""Fetch H3 hex coverage status based on Explorer hotspots, Mappers uplinks."""
import asyncio
import datetime
import itertools
import logging
import logging.handlers
import math
import pathlib
import time

import aiohttp
import h3
import numpy
import pandas
import tenacity

import hexpop

EXPLORER_ENABLED = True
EXPLORER_BASE = "https://api.helium.io/v1/hotspots/hex/"
# private "https://helium-api.stakejoy.com/v1/hotspots/hex/"

MAPPERS_ENABLED = True
MAPPER_BASE = "https://mappers.helium.com/api/v1/uplinks/hex/"
# avoid "https://mappers.helium.com/api/v1/coverage/geo/"

MOST_RECENT = """
SELECT updates.*
FROM `{project}.{dataset}.updates` AS updates
INNER JOIN (
  SELECT h3_index, region, MAX(update_time) AS update_time
  FROM `{project}.{dataset}.updates`
  GROUP BY h3_index, region) AS most_recent
ON
  updates.h3_index=most_recent.h3_index
  AND updates.region=most_recent.region
  AND updates.update_time=most_recent.update_time
"""
ADDITIONS = """
SELECT h3_index
FROM `{project}.{regional_dataset}.{region}`
WHERE h3_index NOT IN (
    SELECT h3_index FROM `{project}.{coverage_dataset}.updates`
      WHERE region='{region}'
)
"""
REFRESH = """
SELECT h3_index, update_time
FROM `{project}.{dataset}.most_recent`
WHERE region = '{region}'
ORDER BY update_time ASC, h3_index
"""


def parse_args():
    """Parse command line arguments."""
    parser = hexpop.initialize_parser(__doc__)
    parser.add_argument('regions',
                        type=str,
                        nargs='*',
                        default='all',
                        help='regions for which to fetch coverage status')
    parser.add_argument('-a',
                        '--analyze',
                        action='store_true',
                        default=False,
                        help='analyze only, do not fetch coverage status')
    parser.add_argument('-b',
                        '--batch_size',
                        type=int,
                        default=1000,
                        help='number per batch of fetch coverage status')
    parser.add_argument('-v',
                        '--verbose',
                        action='store_true',
                        default=False,
                        help='verbose about fetching coverage status')
    parser.add_argument('-x',
                        '--expire',
                        type=int,
                        default=None,
                        help='refetch coverage status older than EXPIRE days')
    args = parser.parse_args()
    return (args.regions, args.analyze, args.batch_size, args.verbose,
            args.expire)


logger_tenacity = logging.getLogger('tenacity')
hexpop.initialize_logging(logger_tenacity)


@tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, min=1, max=300),
                before_sleep=tenacity.before_sleep_log(logger_tenacity,
                                                       logging.WARNING))
async def fetch_coverage(h3_index, session):
    """Fetch H3 hex coverage based on Explorer hotspots or Mappers uplinks."""
    explorer_url = EXPLORER_BASE + h3_index
    explorer_coverage = False
    if EXPLORER_ENABLED:
        async with session.get(explorer_url, headers={'user-agent':
                                                      'hexpop'}) as response:
            hotspots = (await response.json())['data']
            explorer_coverage = len(hotspots) > 0
    logger.debug("exp %s (attempts %d)", h3_index,
                 fetch_coverage.retry.statistics['attempt_number'])
    mappers_coverage = False
    if MAPPERS_ENABLED:
        mapper_urls = [
            MAPPER_BASE + h for h in h3.k_ring(h3.h3_to_center_child(h3_index))
        ]
        for mapper_url in mapper_urls:
            async with session.get(mapper_url,
                                   headers={'user-agent':
                                            'hexpop'}) as response:
                uplinks = (await response.json())['uplinks']
                mappers_coverage |= len(uplinks) > 0
                if mappers_coverage:
                    break
    logger.debug("map %s (attempts %d)", h3_index,
                 fetch_coverage.retry.statistics['attempt_number'])
    return [
        h3_index, explorer_coverage, mappers_coverage,
        datetime.datetime.utcnow()
    ]


async def fetch_coverages(h3hexes):
    """Queue coroutines to fetch coverage, gather results."""
    async with aiohttp.ClientSession(raise_for_status=True) as session:
        return await asyncio.gather(
            *map(fetch_coverage, h3hexes, itertools.repeat(session)))


if __name__ == '__main__':
    logger = logging.getLogger(pathlib.Path(__file__).stem)
    hexpop.initialize_logging(logger)
    regions, analyze, batch_size, verbose, expire = parse_args()
    regional_dataset = hexpop.bq_prep_dataset('geopop')
    coverage_dataset = hexpop.bq_prep_dataset('coverage')
    updates_schema = hexpop.bq_form_schema([('h3_index', 'STRING'),
                                            ('region', 'STRING'),
                                            ('explorer_coverage', 'BOOLEAN'),
                                            ('mappers_coverage', 'BOOLEAN'),
                                            ('update_time', 'TIMESTAMP')])
    updates_table = hexpop.bq_create_table(coverage_dataset,
                                           'updates',
                                           schema=updates_schema,
                                           partition='update_time',
                                           cluster=['region'],
                                           force_new=False)
    most_recent = hexpop.bq_create_view(
        coverage_dataset, 'most_recent',
        MOST_RECENT.format(project=coverage_dataset.project,
                           dataset=coverage_dataset.dataset_id))

    for region in hexpop.clean_regions(regions):
        if region == 'gadm':
            continue
        logger = logging.getLogger(' '.join(
            [pathlib.Path(__file__).stem, region]))
        hexpop.initialize_logging(logger, verbose)
        df_additions = hexpop.bq_query_table(
            ADDITIONS.format(project=coverage_dataset.project,
                             regional_dataset=regional_dataset.dataset_id,
                             coverage_dataset=coverage_dataset.dataset_id,
                             region=region)).to_dataframe()
        df_refresh = hexpop.bq_query_table(
            REFRESH.format(project=coverage_dataset.project,
                           dataset=coverage_dataset.dataset_id,
                           table=updates_table.table_id,
                           region=region)).to_dataframe()
        retain = 0
        if expire:
            retain = df_refresh.shape[0]
            expiration_date = datetime.datetime.now().replace(
                tzinfo=datetime.timezone.utc) - datetime.timedelta(days=expire)
            df_refresh = df_refresh[(df_refresh['update_time'] <=
                                     expiration_date)]
            retain -= df_refresh.shape[0]
        try:
            early = df_refresh['update_time'].iloc[0].strftime('%Y-%m-%dT%X')
            late = df_refresh['update_time'].iloc[-1].strftime('%Y-%m-%dT%X')
        except IndexError:
            early = 'none'
            late = 'none'
        df_total = pandas.concat([df_additions,
                                  df_refresh]).reset_index(drop=True)
        logger.info("%d hexes (retain %d, add %d, refresh %d from %s to %s)",
                    df_total.shape[0], retain, df_additions.shape[0],
                    df_refresh.shape[0], early, late)
        logger.debug("\n%s", df_total)
        if analyze or df_total.shape[0] == 0:
            continue
        processed = 0
        time_start = time.perf_counter()
        for df_batch in numpy.array_split(
                df_total, math.ceil(df_total.shape[0] / batch_size)):
            loop = asyncio.get_event_loop()
            loop_output = loop.run_until_complete(
                fetch_coverages(df_batch['h3_index'].tolist()))
            df_output = pandas.DataFrame(loop_output,
                                         columns=[
                                             'h3_index', 'explorer_coverage',
                                             'mappers_coverage', 'update_time'
                                         ])
            df_output.insert(1, 'region', region)
            hexpop.bq_load_table(df_output,
                                 updates_table,
                                 schema=None,
                                 write='WRITE_APPEND')
            processed += df_output.shape[0]
            proc_pcnt = 100 * processed / df_total.shape[0]
            elapsed = time.perf_counter() - time_start
            rate = processed / elapsed
            message = f"Processed {processed} hexes ({proc_pcnt:.1f}%) \t"
            message += f"Elapsed {elapsed:.0f} seconds ({rate:.0f} hexes/sec)"
            logger.info(message)
        logger.info("completed %s, elapsed time %.2f secods", region,
                    time.perf_counter() - time_start)
