import bonobo
import bonobo.config
import bonobo.util
import csv
import config as conf
import pandas as pd
from bonobo.config import use

HOTELS_FILE_NAME = conf.PROCESSED_DATA_DIRECTORY + '/hotels.csv'


def read_hotels():
    return pd.read_csv(HOTELS_FILE_NAME, index_col=0, header=None, squeeze=True).to_dict()


def get_hotels():
    try:
        print("Reading Hotels")
        return read_hotels()
    except FileNotFoundError:
        print("Hotels will be initialized")
        return {}


def get_hotel_ids():
    hotels = get_hotels()
    if bool(hotels):
        return set(hotels[1].values())
    else:
        return set()


@use('hotel_ids')
def transform_hotel(data, hotel_ids):
    row = data['row']
    hotel_id = int(row[1])
    if hotel_id not in hotel_ids:
        current_index = len(hotel_ids)
        hotel_ids.add(hotel_id)
        hotel_name = row[2]
        load(current_index, hotel_id, hotel_name, row[3])
    yield {
        **data,
        'hotel_id': hotel_id
    }


def load(hotel):
    with open(HOTELS_FILE_NAME, 'a') as f:
        writer = csv.writer(f)
        writer.writerow(hotel)

def get_services():
    return {
        'hotel_ids': get_hotel_ids()
    }
