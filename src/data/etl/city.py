import pandas as pd
import config as conf
import bonobo
import bonobo.config
import bonobo.util
import date as d

CITIES_FILE_NAME = conf.PROCESSED_DATA_DIRECTORY + '/cities.csv'


def read_cities():
    return pd.read_csv(CITIES_FILE_NAME, index_col=0, header=None, squeeze=True, skiprows=1).to_dict()


def get_cities():
    try:
        print("Reading Cities")
        return read_cities()
    except FileNotFoundError:
        Exception('City directory not exists.')


def get_services():
    return {
        'cities': get_cities()
    }


@bonobo.config.use('cities')
def transform_city(data, cities):
    yield {
        **data,
        'city_id': list(cities[1].keys())[list(cities[1].values()).index(data['scrape_city'])]
    }
