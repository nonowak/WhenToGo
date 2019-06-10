import datetime

import bonobo.config
import bonobo.util
import config as conf
import pandas as pd

START_DATE = datetime.date(2019, 4, 17)
END_DATE = datetime.date(2020, 4, 17)
DATES_FILE_NAME = conf.PROCESSED_DATA_DIRECTORY + '/dates.csv'



def generate_dates():
    dates = pd.date_range(START_DATE, END_DATE).tolist()
    df = pd.DataFrame(dates)
    df.to_csv(DATES_FILE_NAME)
    return read_dates()


def read_dates():
    return pd.read_csv(DATES_FILE_NAME, index_col=0, header=None, squeeze=True, skiprows=1, dtype={0: str}).to_dict()


def get_dates():
    try:
        print("Reading Dates")
        return read_dates()
    except FileNotFoundError:
        print("Generating Dates")
        return generate_dates()


def get_services():
    return {
        'dates': get_dates()
    }


@bonobo.config.use('dates')
def transform_date(data, dates):
    yield {
        'scrape_id': data['scrape_id'],
        'date_id': list(dates.keys())[list(dates.values()).index(data['scrape_date'])]
    }