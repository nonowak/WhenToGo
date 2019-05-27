import csv
import re


def extract(name):
    with open(name) as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield {
                **row,
                'scrape_hour': name[-9:-7],
                'scrape_date': name[-20:-10],
                'scrape_city': re.search('raw/(.*)/', name).group(1)
            }
