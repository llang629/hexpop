"""Read public data source, load as BigQuery table."""
import configparser
import itertools
import logging
import logging.handlers
import math
import multiprocessing
import os
import pathlib
import sys
import time
from types import SimpleNamespace

import geopandas
import numpy
import pandas

import hexpop


def parse_args():
    """Parse command line arguments."""
    parser = hexpop.initialize_parser(__doc__)
    parser.add_argument('source',
                        type=str,
                        help='public data source to load into BigQuery table')
    parser.add_argument('-r',
                        '--rows',
                        type=int,
                        default=0,
                        help='number of rows to input, 0 for all rows')
    parser.add_argument('-s',
                        '--start',
                        type=int,
                        default=0,
                        help='starting row number to input')
    parser.add_argument('-t',
                        '--test',
                        action='store_true',
                        default=False,
                        help='create test dataset')
    args = parser.parse_args()
    return args.source, args.rows, args.start, args.test


def parse_ini(source):
    """Parse configurations from .ini file."""
    config = SimpleNamespace(source=source)
    ini = configparser.ConfigParser()
    ini.read(pathlib.Path(__file__).with_suffix('.ini'))
    try:
        config.description = ini.get(source, 'description').strip()
    except configparser.NoOptionError:
        config.description = None
    try:
        config.gdfile = ini.get(source, 'path')
    except configparser.NoSectionError:
        logger.error("ERROR: configuration not found for source %s", source)
        sys.exit()
    try:
        config.encoding = ini.get(source, 'encoding')
    except configparser.NoOptionError:
        config.encoding = None
    try:
        _dc = ini.get(source, 'drop_columns')
        config.drop_columns = list(
            filter(None, [x.strip() for x in _dc.splitlines()]))
    except configparser.NoOptionError:
        config.drop_columns = []
    config.recast_query = ini.get(source, 'recast_query')
    return config


def init_worker():
    """Name worker and initialize its logger."""
    multiprocessing.current_process().name = pathlib.Path(
        __file__).stem + '_worker' + multiprocessing.current_process(
        ).name.split('-')[-1]
    multiprocessing.current_process().logger = logging.getLogger(
        multiprocessing.current_process().name)
    hexpop.initialize_logging(multiprocessing.current_process().logger)


def read_gdf(source, test_rows=0, test_start=0):
    """Read GeoPandas dataframe, possibly with smaller set of test rows."""
    if test_rows != 0:
        return geopandas.read_file(source,
                                   rows=slice(test_start,
                                              test_start + test_rows))
    return geopandas.read_file(source)


def gdf2df(gdf):
    """Convert GeoPandas to Pandas dataframe, encoding geometry as WKT."""
    return pandas.DataFrame(
        gdf.assign(WKT=gdf['geometry'].apply(lambda g: g.wkt))).drop(
            columns=['geometry'])


def gdf2table(gdf_batch, main_table, config):
    """Load, recast, and append gdf_batch to main_table."""
    logger = multiprocessing.current_process().logger
    target_crs = 'epsg:4326'  # latitude/longitude
    if gdf_batch.crs != target_crs:
        logger.info("converting coordinates from %s to %s", gdf_batch.crs,
                    target_crs)
        gdf_batch.to_crs(target_crs, inplace=True)
        logger.info("converted coordinates to %s", gdf_batch.crs)
    df = gdf2df(gdf_batch)
    del gdf_batch  # free memory, maybe
    if config.encoding:  # example: non-ASCII in Canadian French place names
        for col, dtype in df.dtypes.items():
            if dtype == object:  # process byte-object columns
                df[col] = df[col].apply(lambda x: x.decode(config.encoding)
                                        if isinstance(x, bytes) else x)
    temp_table_id = hexpop.bq_tmp_id(hexpop.bq_full_id(main_table))
    try:
        logger.info("loading dataframe batch to temporary bq table %s",
                    temp_table_id)
        result = hexpop.bq_load_table(df, temp_table_id, schema=config.schema)
        logger.info("loaded %d rows across %d columns", result.output_rows,
                    len(result.schema))
    except Exception as err:  # unexpected errors can occur with new datasets
        logger.error('EXCEPTION while loading: %s', err)
    try:
        result = hexpop.bq_query_table(
            config.recast_query.format(temp_table_id),
            hexpop.bq_full_id(main_table))
        logger.info(
            "recast and appended to main bq table %s, currently with %d rows",
            main_table.table_id, result.total_rows)
    except Exception as err:  # unexpected errors can occur with new datasets
        logger.error('EXCEPTION while recasting: %s', err)
    hexpop.bq_delete_table(temp_table_id)


if __name__ == '__main__':
    logger = logging.getLogger(pathlib.Path(__file__).stem)
    hexpop.initialize_logging(logger)
    source, test_rows, test_start, test_dataset = parse_args()
    config = parse_ini(source)
    time_start = time.perf_counter()
    gdlocal = config.gdfile.rsplit('/', maxsplit=1)[-1]
    if os.path.isfile(gdlocal):
        config.gdfile = gdlocal
    gdname = gdlocal.split('.')[0]
    logger.info("reading data %s", config.gdfile)
    gdf = read_gdf(config.gdfile, test_rows, test_start)
    gdf.drop(columns=config.drop_columns, inplace=True)
    rows = gdf.shape[0]
    logger.info("dataframe %d rows across columns %s", rows,
                ', '.join(gdf.columns.values))

    # ignore administrative divisions without physical areas
    gdf = gdf[gdf['geometry'].notnull()]

    dtype2sql = {
        'float64': 'FLOAT64',
        'int64': 'INTEGER',
        'object': 'STRING',
        'geometry': 'GEOGRAPHY'
    }
    schema_fields = []
    for column, dtype in zip(gdf.columns.values, gdf.dtypes):
        if dtype.name == 'geometry':
            schema_fields.append(('WKT', 'STRING'))
        else:
            schema_fields.append((column, dtype2sql[dtype.name]))
    config.schema = hexpop.bq_form_schema(schema_fields)
    dataset = hexpop.bq_prep_dataset('public', test_dataset=test_dataset)
    table = hexpop.bq_create_table(dataset,
                                   gdname,
                                   description=config.description,
                                   force_new=True)

    batch_count = max(min(multiprocessing.cpu_count(), rows),
                      math.ceil(rows / 10**6))
    logger.info("dataframe divided into %d batches", batch_count)
    with multiprocessing.Pool(initializer=init_worker) as p:
        p.starmap(
            gdf2table,
            zip(numpy.array_split(gdf, batch_count), itertools.repeat(table),
                itertools.repeat(config)))

    logger.info("completed %s, elapsed time %.2f secods", gdname,
                time.perf_counter() - time_start)
