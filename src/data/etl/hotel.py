import bonobo
import bonobo.config
import bonobo.util
import glob
import csv
import config as conf
from bonobo.config import use

HOTELS_FILE_NAME = conf.PROCESSED_DATA_DIRECTORY + '/hotels.csv'


def get_current_hotels():
    try:
        with open(HOTELS_FILE_NAME, 'r') as f:
            reader = csv.reader(f)
            return list(reader)
    except FileNotFoundError:
        print('First initialization')


def get_hotel_ids():
    hotels = get_current_hotels()
    if hotels is not None:
        return set(map(int, get_col(hotels, 1)))
    else:
        return set()


def get_col(list, col):
    return [row[col] for row in list]


def get_file_names():
    return glob.glob(conf.RAW_DATA_DIRECTORY + "/**/*.csv", recursive=True)


@use('hotel_ids')
def transform(*row, hotel_ids):
    hotel_id = int(row[1])
    if hotel_id not in hotel_ids:
        hotel_ids.add(hotel_id)
        return len(hotel_ids), hotel_id, row[2]


def load(*hotel):
    with open(HOTELS_FILE_NAME, 'a') as f:
        writer = csv.writer(f)
        writer.writerow(hotel)


def get_graph(graph=None, producer=None):
    graph = graph or bonobo.Graph()
    graph.add_chain(transform,
                    load, _input=producer.output)
    return graph


def get_services():
    return {
        'hotel_ids': get_hotel_ids()
    }
