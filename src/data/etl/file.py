import csv
import re
import config as conf
import os

import bonobo.config
import random

EXPECTED_KEYS_COUNT = 15
SCRAPE_DIRECTORY = conf.PROCESSED_DATA_DIRECTORY + '/scrapes'


@bonobo.config.use('scrape_ids')
def extract(name, scrape_ids):
    if not os.path.exists(SCRAPE_DIRECTORY):
        os.makedirs(SCRAPE_DIRECTORY)
    with open(name) as f:
        reader = csv.DictReader(f)
        for row in reader:
            scrape_id = get_session_id(scrape_ids)
            scrape_ids.add(scrape_id)
            yield {
                **row,
                'scrape_id': scrape_id,
                'scrape_hour': name[-9:-7],
                'scrape_date': name[-20:-10],
                'scrape_city': re.search('raw/(.*)/', name).group(1)
            }


def get_session_id(current_ids):
    result = 0
    while result in current_ids:
        result = random.randint(0, 500000)
    return result


def get_services():
    return {
        'scrape_ids': set(),
        'not_saved_scrapes': {}
    }


def delete_unnecessary_keys(data):
    [data.pop(k, None) for k in ('scrape_city',
                                 '')]
    return data


def remove_from_lists(data, scrape_ids, not_saved_scrapes):
    scrape_id = int(data['scrape_id'])
    scrape_ids.remove(scrape_id)
    not_saved_scrapes.pop(scrape_id, None)


@bonobo.config.use('scrape_ids', 'not_saved_scrapes')
def load(data, scrape_ids, not_saved_scrapes):
    if data is not None:
        if data != 15:
            data['is_broken'] = True
        remove_from_lists(data, scrape_ids, not_saved_scrapes)
        city = data['scrape_city']
        data = delete_unnecessary_keys(data)
        with open(f'{SCRAPE_DIRECTORY}/{city}_scrape.csv', 'a') as f:
            writer = csv.writer(f)
            writer.writerow(data.values())
