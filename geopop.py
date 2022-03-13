"""Map H3 hexes to region based on geo boundary, load as geopop table."""
import configparser
import logging
import logging.handlers
import pathlib
import sys
from types import SimpleNamespace

import hexpop


def parse_args():
    """Parse command line arguments."""
    parser = hexpop.initialize_parser(__doc__)
    parser.add_argument('regions',
                        nargs='*',
                        type=str,
                        help='global region to load into geopop table')
    args = parser.parse_args()
    return args.regions


def parse_ini(region=None):
    """Parse configurations from .ini file."""
    logger = logging.getLogger(f"{__name__}.{sys._getframe().f_code.co_name}")
    hexpop.initialize_logging(logger)
    ini = configparser.ConfigParser()
    ini.read(pathlib.Path(__file__).with_suffix('.ini'))
    if not region:
        return ini.sections()
    params = SimpleNamespace()
    try:
        params.geo_query = ini.get(region, 'geo_query')
    except (configparser.NoSectionError, configparser.NoOptionError):
        logger.critical("no valid geo_query for region %s", region)
    try:
        params.sem_admin = ini.get(region, 'sem_admin')
        params.sem_code = ini.get(region, 'sem_code')
        params.sem_name = ini.get(region, 'sem_name')
        params.sem_geom = ini.get(region, 'sem_geom')
        params.sem_source = ini.get(region, 'sem_source')
        params.bis_admin = ini.get(region, 'bis_admin')
        params.bis_code = ini.get(region, 'bis_code')
        params.bis_name = ini.get(region, 'bis_name')
        params.bis_geom = ini.get(region, 'bis_geom')
        params.bis_source = ini.get(region, 'bis_source')
    except (configparser.NoSectionError, configparser.NoOptionError):
        logger.critical("no valid sem/bis for region %s", region)
    return params


if __name__ == '__main__':
    main_name = pathlib.Path(__file__).stem
    logger = logging.getLogger(main_name)
    hexpop.initialize_logging(logger)
    dataset = hexpop.bq_prep_dataset(main_name)
    regions = parse_args()
    if not regions:
        regions = parse_ini()
    for region in hexpop.clean_regions(regions):
        geo_query = parse_ini(region).geo_query
        logger.info("creating %s table for %s", main_name, region)
        table = hexpop.bq_create_table(dataset, region, force_new=True)
        result = hexpop.bq_query_table(
            geo_query.format(project=dataset.project),
            hexpop.bq_full_id(table))
        logger.info("created table %s with %d rows", table.full_table_id,
                    result.total_rows)
