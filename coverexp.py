"""Fetch Explorer hotspot hexes via Helium API."""
import datetime
import logging
import pathlib
import time

import pandas
import requests
import tenacity

import hexpop

EXPLORER_URL = "https://api.helium.io/v1/hotspots"

logger_tenacity = logging.getLogger('tenacity')
hexpop.initialize_logging(logger_tenacity)


@tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, min=1, max=60),
                before_sleep=tenacity.before_sleep_log(logger_tenacity,
                                                       logging.WARNING))
def helium_api(cursor=''):
    """Helium API query, isolated for tenacity."""
    if cursor:
        resp = requests.get(url=EXPLORER_URL + '?cursor=' + cursor)
    else:
        resp = requests.get(url=EXPLORER_URL)
    resp.raise_for_status()
    return resp.json()


def fetch_hotspots():
    """Fetch hotspots via Helium API."""
    first_pass = True
    cursor = ''
    while first_pass or cursor:
        first_pass = False
        payload = helium_api(cursor)
        try:
            cursor = payload['cursor']
        except KeyError:
            cursor = ''
        yield payload['data']


coverage_dataset = hexpop.bq_prep_dataset('coverage')

MOST_RECENT_EXPLORER_UPDATES = """
SELECT * FROM `{project}.{coverage_dataset}.explorer_updates`
WHERE update_time >= PARSE_TIMESTAMP('%Y%m%d%H',(
    SELECT MAX(IF(CONTAINS_SUBSTR(partition_id, 'NULL'),'0', partition_id))
    FROM `{project}.{coverage_dataset}.INFORMATION_SCHEMA.PARTITIONS`
    WHERE table_name = 'explorer_updates'))
"""


def load_explorer_coverage(hex_set):
    """Load Explorer coverage hex set to table."""
    df = pandas.DataFrame({'h3_index': sorted(list(hex_set))})
    df['explorer_coverage'] = True
    df['update_time'] = datetime.datetime.utcnow()
    hexpop.bq_load_table(df, explorer_table, write='WRITE_APPEND')


def query_explorer_coverage(hexset=True):
    """Query most recent Explorer coverage hex set from view."""
    view_id = 'most_recent_explorer'
    most_recent_explorer = hexpop.bq_create_view(
        coverage_dataset,
        view_id,
        MOST_RECENT_EXPLORER_UPDATES.format(
            project=coverage_dataset.project,
            coverage_dataset=coverage_dataset.dataset_id),
        force_new=True)
    try:
        df = hexpop.bq_query_table('SELECT * FROM {table}'.format(
            table=hexpop.bq_full_id(most_recent_explorer))).to_dataframe()
    except AttributeError:
        return set(), datetime.datetime.fromtimestamp(0)
    if not hexset:
        return df
    return set(df['h3_index'].loc[df['explorer_coverage']].to_list()), min(
        df['update_time'])


if __name__ == '__main__':
    logger = logging.getLogger(pathlib.Path(__file__).stem)
    hexpop.initialize_logging(logger)
    explorer_schema = hexpop.bq_form_schema([('h3_index', 'STRING'),
                                             ('explorer_coverage', 'BOOLEAN'),
                                             ('update_time', 'TIMESTAMP')])
    explorer_table = hexpop.bq_create_table(coverage_dataset,
                                            'explorer_updates',
                                            schema=explorer_schema,
                                            partition='update_time',
                                            partition_hourly=True,
                                            force_new=False)
    time_start = time.perf_counter()
    total_hotspots = 0
    hex_set = set()
    for hotspots in fetch_hotspots():
        total_hotspots += len(hotspots)
        for hotspot in hotspots:
            if hotspot['location_hex']:
                hex_set.add(hotspot['location_hex'])
        logger.info("%d hotspots covering %d hexes, %d seconds elapsed",
                    total_hotspots, len(hex_set),
                    time.perf_counter() - time_start)
    load_explorer_coverage(hex_set)
    logger.info("completed %d hotspots covering %d hexes, %d seconds elapsed",
                total_hotspots, len(hex_set),
                time.perf_counter() - time_start)
