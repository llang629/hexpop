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

MAPPERS_URL = "https://mappers.helium.com/api/v1/uplinks/hex/"
# avoid "https://mappers.helium.com/api/v1/coverage/geo/"


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


@tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, min=1, max=60),
                before_sleep=tenacity.before_sleep_log(logger_tenacity,
                                                       logging.WARNING))
async def fetch_uplinks(h3_index, session):
    """Fetch H3 hex coverage based on Explorer hotspots or Mappers uplinks."""
    mappers_coverage = False
    mapper_urls = [
        MAPPERS_URL + h for h in h3.k_ring(h3.h3_to_center_child(h3_index))
    ]
    for mapper_url in mapper_urls:
        async with session.get(mapper_url) as response:
            if response.status == 200:
                uplinks = (await response.json())['uplinks']
                mappers_coverage |= len(uplinks) > 0
            elif response.status == 500:
                logger_tenacity.warning(
                    "response status 500 for %s, assuming no coverage",
                    mapper_url)
                mappers_coverage |= False
            else:
                response.raise_for_status()
        if mappers_coverage:
            break
    return [h3_index, mappers_coverage, datetime.datetime.utcnow()]


async def fetch_mappers(h3hexes):
    """Queue coroutines to fetch coverage, gather results."""
    async with aiohttp.ClientSession() as session:
        return await asyncio.gather(
            *map(fetch_uplinks, h3hexes, itertools.repeat(session)))


regional_dataset = hexpop.bq_prep_dataset('geopop')
coverage_dataset = hexpop.bq_prep_dataset('coverage')

ADDITIONS = """
SELECT h3_index FROM `{project}.{regional_dataset}.{region}`
WHERE h3_index NOT IN (SELECT h3_index
FROM `{project}.{coverage_dataset}.mappers_updates`)
"""
REFRESH = """
SELECT * FROM
(SELECT h3_index, MAX(update_time) as update_time
FROM `{project}.{coverage_dataset}.mappers_updates`
GROUP BY h3_index
ORDER BY update_time ASC, h3_index)
WHERE h3_index IN
(SELECT h3_index FROM `{project}.{regional_dataset}.{region}`)
"""

MOST_RECENT_MAPPERS_UPDATES = """
SELECT mappers_updates.*
FROM `{project}.{coverage_dataset}.mappers_updates` AS mappers_updates
INNER JOIN (
  SELECT h3_index, MAX(update_time) AS update_time
  FROM `{project}.{coverage_dataset}.mappers_updates`
  GROUP BY h3_index) AS most_recent
ON
  mappers_updates.h3_index=most_recent.h3_index
  AND mappers_updates.update_time=most_recent.update_time
"""


def load_mappers_coverage(df=None):
    """Load Mappers coverage data frame to table."""
    hexpop.bq_load_table(df, mappers_table, write='WRITE_APPEND')


def query_mappers_coverage(hexset=True):
    """Query most recent Mappers coverage hex set from view."""
    view_id = 'most_recent_mappers'
    most_recent_mappers = hexpop.bq_create_view(
        coverage_dataset,
        view_id,
        MOST_RECENT_MAPPERS_UPDATES.format(
            project=coverage_dataset.project,
            coverage_dataset=coverage_dataset.dataset_id),
        force_new=True)
    try:
        df = hexpop.bq_query_table('SELECT * FROM {table}'.format(
            table=hexpop.bq_full_id(most_recent_mappers))).to_dataframe()
    except AttributeError:
        return set(), datetime.datetime.fromtimestamp(0)
    if not hexset:
        return df
    return set(df['h3_index'].loc[df['mappers_coverage']].to_list()), min(
        df['update_time'])


if __name__ == '__main__':
    logger = logging.getLogger(pathlib.Path(__file__).stem)
    hexpop.initialize_logging(logger)
    regions, analyze, batch_size, verbose, expire = parse_args()
    mappers_schema = hexpop.bq_form_schema([('h3_index', 'STRING'),
                                            ('mappers_coverage', 'BOOLEAN'),
                                            ('update_time', 'TIMESTAMP')])
    mappers_table = hexpop.bq_create_table(coverage_dataset,
                                           'mappers_updates',
                                           schema=mappers_schema,
                                           partition='update_time',
                                           force_new=False)
    for region in hexpop.clean_regions(regions):
        if region == 'gadm':
            continue
        logger = logging.getLogger(' '.join(
            [pathlib.Path(__file__).stem, region]))
        hexpop.initialize_logging(logger, verbose)
        df_additions = hexpop.bq_query_table(
            ADDITIONS.format(project=coverage_dataset.project,
                             coverage_dataset=coverage_dataset.dataset_id,
                             regional_dataset=regional_dataset.dataset_id,
                             region=region)).to_dataframe()
        df_refresh = hexpop.bq_query_table(
            REFRESH.format(project=coverage_dataset.project,
                           coverage_dataset=coverage_dataset.dataset_id,
                           table=mappers_table.table_id,
                           regional_dataset=regional_dataset.dataset_id,
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
                fetch_mappers(df_batch['h3_index'].tolist()))
            df_output = pandas.DataFrame(
                loop_output,
                columns=['h3_index', 'mappers_coverage', 'update_time'])
            load_mappers_coverage(df_output)
            processed += df_output.shape[0]
            proc_pcnt = 100 * processed / df_total.shape[0]
            elapsed = time.perf_counter() - time_start
            rate = processed / elapsed
            message = f"Processed {processed} hexes ({proc_pcnt:.1f}%) \t"
            message += f"Elapsed {elapsed:.0f} seconds ({rate:.0f} hexes/sec)"
            logger.info(message)
        logger.info("completed %s, %d seconds elapsed", region,
                    time.perf_counter() - time_start)
