import bonobo.util
import config as conf
import pandas as pd
from bonobo.config import use

HOURS_FILE_NAME = conf.PROCESSED_DATA_DIRECTORY + '/hours.csv'


def generate_hours():
    df = pd.DataFrame(["00", "04", "08", "12", "16", "20"])
    df.to_csv(HOURS_FILE_NAME)
    return read_hours()


def read_hours():
    return pd.read_csv(HOURS_FILE_NAME, index_col=0, header=None, squeeze=True, skiprows=1, dtype={1: str}).to_dict()


def get_hours():
    try:
        print("Reading Hours")
        return read_hours()
    except FileNotFoundError:
        print("Generating Hours")
        return generate_hours()


def get_services():
    return {
        "hours": get_hours()
    }


@bonobo.config.use('hours')
def transform_hour(data, hours):
    yield {
        **data,
        'hour': list(hours.keys())[list(hours.values()).index(data['hour'])]
    }
