import luigi
import extractRawFiles as erf
import config as conf
import pandas as pd
import common as cmn

DATES_FILE_NAME = f'{conf.PROCESSED_DATA_DIRECTORY}/dates.csv'
LUIGI_DATES_DIRECTORY = f'{conf.LUIGI_DIRECTORY}/dates'


class TransformDates(luigi.Task):
    dates_dictionary = luigi.DictParameter(
        default=pd.read_csv(DATES_FILE_NAME, index_col=0, squeeze=True, header=None).to_dict(),
        is_global=True
    )
    city_date = 'temp.csv'
    raw_data_filename = luigi.Parameter()


    def output(self):
        return luigi.LocalTarget(f'{LUIGI_DATES_DIRECTORY}/{self.city_date}')

    def run(self):
        df = pd.read_csv(self.raw_data_filename, index_col=0)
        df = df.replace({'scrape_date': self.dates_dictionary})
        df_dates = df[['scrape_date']]
        self.city_date = cmn.get_city_date(self.raw_data_filename)
        cmn.create_directory(LUIGI_DATES_DIRECTORY)
        df_dates.to_csv(f'{LUIGI_DATES_DIRECTORY}/{self.city_date}')
