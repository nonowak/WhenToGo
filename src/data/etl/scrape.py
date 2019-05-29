import csv

import bonobo
import bonobo.config
import bonobo.util
import config as conf

SCRAPE_DICTIONARY = conf.PROCESSED_DATA_DIRECTORY + '/scrapes/{}_scrape.csv'
SCRAPE_INFO_FILE_NAME = conf.PROCESSED_DATA_DIRECTORY + '/scrape_info.csv'


def delete_unnecessary_keys(data):
    [data.pop(k, None) for k in ('scrape_hour',
                                 'scrape_city',
                                 'scrape_date',
                                 'score',
                                 'name',
                                 '')]
    return data


def load_scrape(data):
    city = data['scrape_city']
    data = delete_unnecessary_keys(data)
    with open(SCRAPE_DICTIONARY.format(city), 'a') as f:
        writer = csv.writer(f)
        writer.writerow(data.values())


def get_graph(graph=None):
    graph = graph or bonobo.Graph()
    graph.add_chain(load_scrape, _input=graph)
    return graph
