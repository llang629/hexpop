"""Fetch Explorer hotspot hexes via Helium API."""
import datetime
import logging
import pathlib
import time

import pandas
import requests
import tenacity

import hexpop

HOTSPOTS_URL = "https://api.helium.io/v1/hotspots"

logger_tenacity = logging.getLogger('tenacity')
hexpop.initialize_logging(logger_tenacity)


@tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, min=1, max=60),
                before_sleep=tenacity.before_sleep_log(logger_tenacity,
                                                       logging.WARNING))
def helium_api(cursor=''):
    """Helium API query, isolated for tenacity."""
    if cursor:
        resp = requests.get(url=HOTSPOTS_URL + '?cursor=' + cursor)
    else:
        resp = requests.get(url=HOTSPOTS_URL)
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
    """Load Explorer coverage hex set to BigQuery table."""
    explorer_schema = hexpop.bq_form_schema([('h3_index', 'STRING'),
                                             ('explorer_coverage', 'BOOLEAN'),
                                             ('update_time', 'TIMESTAMP')])
    explorer_table = hexpop.bq_create_table(coverage_dataset,
                                            'explorer_updates',
                                            schema=explorer_schema,
                                            partition='update_time',
                                            partition_hourly=True,
                                            force_new=False)
    df = pandas.DataFrame({'h3_index': sorted(list(hex_set))})
    df['explorer_coverage'] = True
    df['update_time'] = datetime.datetime.utcnow()
    hexpop.bq_load_table(df, explorer_table, write='WRITE_APPEND')


def query_explorer_coverage():
    """Query most recent Explorer coverage hex set from BigQuery table."""
    try:
        df = hexpop.bq_query_table(
            MOST_RECENT_EXPLORER_UPDATES.format(
                project=coverage_dataset.project,
                coverage_dataset=coverage_dataset.dataset_id)).to_dataframe()
    except AttributeError:
        return set(), datetime.datetime.fromtimestamp(0)
    return set(df['h3_index'].to_list()), max(df['update_time'])


if __name__ == '__main__':
    logger = logging.getLogger(pathlib.Path(__file__).stem)
    hexpop.initialize_logging(logger)
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
