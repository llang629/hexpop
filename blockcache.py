"""Fetch H3 hex coverage via Helium API for hotspots."""
import json
import logging
import os
import pathlib
import time

import requests
import tenacity

import hexpop

HOTSPOTS_URL = "https://api.helium.io/v1/hotspots"

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
    return payload['cursor'], payload['data']


if __name__ == '__main__':
    logger = logging.getLogger(pathlib.Path(__file__).stem)
    hexpop.initialize_logging(logger)
    time_start = time.perf_counter()
    first_pass = True
    cursor = ''
    total_hotspots = 0
    hex_set = set()
    while first_pass or cursor:
        cursor, hotspots = fetch_hotspots(cursor)
        first_pass = False
        total_hotspots += len(hotspots)
        for hotspot in hotspots:
            if hotspot['location_hex']:
                hex_set.add(hotspot['location_hex'])
        logger.info("%d hotspots covering %d hexes, %d seconds elapsed",
                    total_hotspots, len(hex_set),
                    time.perf_counter() - time_start)
        break
    hex_list = list(hex_set)
    hex_list.sort()
    os.makedirs(hexpop.CACHE_DIR, exist_ok=True)
    with open(pathlib.Path(hexpop.CACHE_DIR) /
              pathlib.Path(__file__).with_suffix('.json').name,
              'w',
              encoding='utf8') as f:
        f.write(json.dumps(hex_list))
