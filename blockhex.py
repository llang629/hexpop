"""Fetch hotspot hexes via Helium API."""
import json
import logging
import os
import pathlib
import sys
import time
import types

import requests
import tenacity

import hexpop

HOTSPOTS_URL = "https://api.helium.io/v1/hotspots"
CACHE_FILE = pathlib.Path(
    hexpop.CACHE_DIR) / pathlib.Path(__file__).with_suffix('.json').name

logger_tenacity = logging.getLogger('tenacity')
hexpop.initialize_logging(logger_tenacity)


@tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, min=1, max=60),
                before_sleep=tenacity.before_sleep_log(logger_tenacity,
                                                       logging.WARNING))
def fetch_hotspots(cursor):
    """Fetch hotspots via Helium API."""
    if cursor:
        resp = requests.get(url=HOTSPOTS_URL + '?cursor=' + cursor)
    else:
        resp = requests.get(url=HOTSPOTS_URL)
    resp.raise_for_status()
    payload = resp.json()
    try:
        new_cursor = payload['cursor']
    except KeyError:
        new_cursor = ''
    return new_cursor, payload['data']


def dump_hex_cache(hex_list):
    """Dump hostspot hexes to local cache."""
    os.makedirs(hexpop.CACHE_DIR, exist_ok=True)
    with open(CACHE_FILE, 'w', encoding='utf8') as f:
        f.write(json.dumps(hex_list))


def load_hex_cache(max_cache_age=float('inf')):
    """Load hotspot hexes from local cache."""
    logger = logging.getLogger(f"{__name__}.{sys._getframe().f_code.co_name}")
    hexpop.initialize_logging(logger)
    try:
        assert time.time() - os.path.getmtime(CACHE_FILE) < max_cache_age
        with open(CACHE_FILE, 'r', encoding='utf8') as f:
            logger.info('%s loaded', CACHE_FILE)
            return types.SimpleNamespace(
                **{
                    'set': set(json.load(f)),
                    'timestamp': os.path.getmtime(CACHE_FILE)
                })
    except AssertionError:
        logger.error('%s expired', CACHE_FILE)
        return types.SimpleNamespace(**{
            'set': set(),
            'timestamp': os.path.getmtime(CACHE_FILE)
        })
    except FileNotFoundError:
        logger.error('%s not found', CACHE_FILE)
        return types.SimpleNamespace(**{'set': set(), 'timestamp': 0})


if __name__ == '__main__':
    logger = logging.getLogger(pathlib.Path(__file__).stem)
    hexpop.initialize_logging(logger)
    time_start = time.perf_counter()
    first_pass = True
    cursor = ''
    total_hotspots = 0
    hex_set = set()
    while first_pass or cursor:
        first_pass = False
        cursor, hotspots = fetch_hotspots(cursor)
        total_hotspots += len(hotspots)
        for hotspot in hotspots:
            if hotspot['location_hex']:
                hex_set.add(hotspot['location_hex'])
        logger.info("%d hotspots covering %d hexes, %d seconds elapsed",
                    total_hotspots, len(hex_set),
                    time.perf_counter() - time_start)
    dump_hex_cache(sorted(list(hex_set)))
    logger.info(
        "completed %d hotspots covering %d hexes, %d seconds elapsed",
        total_hotspots, len(hex_set),
        time.perf_counter() - time_start)
