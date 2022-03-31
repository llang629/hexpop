"""Functions for hex population analysis with BigQuery."""
import argparse
import logging
import logging.handlers
import os
import pathlib
import platform
import sys
import time

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery
from google.oauth2 import service_account

LOG_DIR = '/var/log/hexpop'
CACHE_DIR = '/var/cache/hexpop'


def initialize_logging(logger, verbose=False):
    """Initialize logger with console, file, and syslog handlers."""
    if 'linux' in platform.platform().lower():
        syslog_path = '/dev/log'
    elif 'macos' in platform.platform().lower():
        syslog_path = '/var/run/syslog'
    else:
        syslog_path = '/dev/null'

    logger.handlers = []  # clean slate
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s.%(msecs)03d %(name)s: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S')
    _ch = logging.StreamHandler()
    if verbose:
        _ch.setLevel(logging.DEBUG)
    else:
        _ch.setLevel(logging.INFO)
    _ch.setFormatter(formatter)
    logger.addHandler(_ch)
    os.makedirs(LOG_DIR, exist_ok=True)
    _fh = logging.handlers.RotatingFileHandler(
        pathlib.Path(LOG_DIR) / pathlib.Path(
            sys.modules['__main__'].__file__).with_suffix('.log').name,
        maxBytes=10**6,
        backupCount=5)
    _fh.setLevel(logging.INFO)
    _fh.setFormatter(formatter)
    logger.addHandler(_fh)
    _sh = logging.handlers.SysLogHandler(address=syslog_path)
    _sh.setLevel(logging.WARN)
    _sh.setFormatter(logging.Formatter('%(name)s: %(message)s'))
    logger.addHandler(_sh)
    logger.debug("logger initialized")


def initialize_parser(docstring):
    """Initialize parser with help showing docstring and defaults."""
    return argparse.ArgumentParser(
        description=docstring,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)


def clean_regions(regions):
    """Check regions input against prepared geopop tables."""
    logger = logging.getLogger(f"{__name__}.{sys._getframe().f_code.co_name}")
    initialize_logging(logger)
    client = bq_client()
    dataset_id = '.'.join([client.project, 'geopop'])
    regional_tables = [
        t.table_id for t in client.list_tables(dataset_id)
        if t.table_type == 'TABLE'
    ]
    if 'all' in regions:
        return regional_tables
    for region in regions:
        if region not in regional_tables:
            logger.warning("no table found for region %s in dataset %s",
                           region, dataset_id)
    return regions


# https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries
def bq_client():
    """Set up client for Google BigQuery API requests."""
    credentials = service_account.Credentials.from_service_account_file(
        pathlib.Path(__file__).parent / 'google-service-account.json')
    return bigquery.Client(credentials=credentials)


def bq_prep_dataset(dataset_name, list_tables=False, test_dataset=False):
    """Check that BigQuery project and dataset are ready."""
    logger = logging.getLogger(f"{__name__}.{sys._getframe().f_code.co_name}")
    initialize_logging(logger)
    client = bq_client()
    logger.info("bq project %s ready", client.project)
    if test_dataset:
        dataset_name += '_test'
    dataset_id = f'{client.project}.{dataset_name}'
    try:
        dataset = client.get_dataset(dataset_id)
    except NotFound:
        logger.warning("bq dataset %s not found, creating", dataset_id)
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = 'US'
        dataset = client.create_dataset(dataset)
    logger.info("bq dataset %s ready", dataset.full_dataset_id)
    if list_tables:
        return dataset, [
            t.table_id for t in client.list_tables(dataset_id)
            if t.table_type == 'TABLE'
        ]
    return dataset


def bq_form_schema(fields):
    """Convert list of (name, field_type) tuples into BigQuery schema."""
    return [bigquery.SchemaField(x[0].strip(), x[1].strip()) for x in fields]


def bq_create_table(dataset,
                    table_id,
                    schema=None,
                    partition=None,
                    cluster=None,
                    description=None,
                    force_new=False):
    """Create BigQuery table, forcefully if required."""
    client = bq_client()
    table_id = '.'.join([dataset.project, dataset.dataset_id, table_id])
    if force_new:
        client.delete_table(table_id, not_found_ok=True)  # clean slate
    table = bigquery.Table(table_id, schema=schema)
    if partition:
        table.time_partitioning = bigquery.TimePartitioning()
        table.time_partitioning.field = partition
    if cluster:
        table.clustering_fields = cluster
    if description:
        table.description = description
    try:
        return client.create_table(table)
    except Conflict:
        return client.get_table(table)


def bq_create_view(dataset, view_id, view_query, force_new=False):
    """Create BigQuery view forcefully if required."""
    client = bq_client()
    view_id = '.'.join([dataset.project, dataset.dataset_id, view_id])
    if force_new:
        client.delete_table(view_id, not_found_ok=True)  # clean slate
    view = bigquery.Table(view_id)
    view.view_query = view_query
    try:
        return client.create_table(view)
    except Conflict:
        return client.get_table(view)


def bq_load_table(df, table_id, schema=None, write='WRITE_APPEND'):
    """Load Pandas dataframe into BigQuery table."""
    client = bq_client()
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=write  # default replace existing
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    result = job.result()  # wait for job to complete
    return result


def bq_query_table(query, destination=None, write='WRITE_APPEND'):
    """Query BigQuery table using SQL."""
    client = bq_client()
    job_config = bigquery.QueryJobConfig(
        destination=destination,
        write_disposition=write  # default append existing
    )
    job = client.query(query=query, job_config=job_config)
    result = job.result()  # wait for job to complete
    return result


def bq_delete_table(table_id):
    """Delete BigQuery table, if present."""
    client = bq_client()
    client.delete_table(table_id, not_found_ok=True)


def bq_full_id(table):
    """Fix annoyance where BigQuery outputs colon, inputs period."""
    return f"{table.full_table_id.replace(':', '.')}"


def bq_tmp_id(table_id):
    """Generate temporary table_id by appending timestamp string."""
    return f"{table_id}_{str(time.time()).replace('.', '_')}"
