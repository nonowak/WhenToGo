import luigi
import extractRawFiles as erf
import config as conf
import pandas as pd
import common as cmn

HOURS_FILE_NAME = f'{conf.PROCESSED_DATA_DIRECTORY}/hours.csv'
LUIGI_HOURS_DIRECTORY = f'{conf.LUIGI_DIRECTORY}/hours'


class TransformHours(luigi.Task):
    hours_dictionary = luigi.DictParameter(
        default=pd.read_csv(HOURS_FILE_NAME, index_col=0, squeeze=True, header=None, dtype={1: str, 2: int}).to_dict(),
        is_global=True
    )
    city_date = 'temp.csv'
    raw_data_filename = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f'{LUIGI_HOURS_DIRECTORY}/{self.city_date}')

    def run(self):
        df = pd.read_csv(self.raw_data_filename, index_col=0)
        df = df.replace({'scrape_hour': self.hours_dictionary})
        df_hours = df[['scrape_hour']]
        self.city_date = cmn.get_city_date(self.raw_data_filename)
        cmn.create_directory(LUIGI_HOURS_DIRECTORY)
        df_hours.to_csv(f'{LUIGI_HOURS_DIRECTORY}/{self.city_date}')
