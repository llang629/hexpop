"""Convert Statoids webpage to dataframe, load table."""
import logging
import pathlib

import pandas
import requests
from bs4 import BeautifulSoup

import hexpop

INT_FIELDS = ['Pop_2010', 'Area_mi', 'Area_km']


def string2fields(string, lengths):
    """Slice string into fixed-length fields."""
    return (string[pos:pos + length].strip()
            for idx, length in enumerate(lengths)
            for pos in [sum(map(int, lengths[:idx]))])


def usa_counties():
    """Scrape webpage, return dataframe."""
    table = BeautifulSoup(
        requests.get('http://www.statoids.com/yus.html').text,
        'html.parser').pre.string.split('\r\n')
    column_lines = table[2]
    column_widths = [len(column) + 1 for column in column_lines.split(' ')]
    column_widths[-1] = 99
    column_names = table[1]
    keys = [
        field.replace('-', '_').replace('2010 pop.', 'Pop_2010')
        for field in list(string2fields(column_names, column_widths))
    ]
    data_dict_list = []
    for line in table:
        if line.strip() in ['', column_names, column_lines]:
            continue
        data_dict = {}
        for key, field in zip(keys, list(string2fields(line, column_widths))):
            data_dict[key] = field.replace(',', '')  # no comma in numbers
        data_dict_list += [data_dict]
    df = pandas.DataFrame(data_dict_list)
    for col in INT_FIELDS:
        df[col] = pandas.to_numeric(df[col])
    return df


if __name__ == '__main__':
    logger = logging.getLogger(pathlib.Path(__file__).stem)
    hexpop.initialize_logging(logger)
    counties_df = usa_counties()
    logger.debug(counties_df)
    dataset = hexpop.bq_prep_dataset('public')
    table_id = table_id = '.'.join(
        [dataset.project, dataset.dataset_id, 'statoids_usa_counties'])
    schema = hexpop.bq_form_schema([(col, 'INTEGER') for col in INT_FIELDS])
    result = hexpop.bq_load_table(counties_df, table_id, schema=schema)
    logger.info("loaded %d rows across %d columns", result.output_rows,
                len(result.schema))
