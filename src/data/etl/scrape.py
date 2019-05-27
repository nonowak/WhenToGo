import csv

import bonobo
import bonobo.config
import bonobo.util
import config as conf

SCRAPE_FILE_NAME = conf.PROCESSED_DATA_DIRECTORY + '/scrapes.csv'
SCRAPE_INFO_FILE_NAME = conf.PROCESSED_DATA_DIRECTORY + '/scrape_info.csv'


def load_scrape(data):
    scrape = list(data.values())[1:]
    with open(SCRAPE_FILE_NAME, 'a') as f:
        writer = csv.writer(f)
        writer.writerow(scrape)


def get_graph(graph=None):
    graph = graph or bonobo.Graph()
    graph.add_chain(load_scrape, _input=graph)
    return graph
