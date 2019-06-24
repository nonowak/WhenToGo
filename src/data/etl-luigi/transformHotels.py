import luigi
import extractRawFiles as erf
import config as conf
import pandas as pd
import common as cmn

LUIGI_HOTELS_DIRECTORY = f'{conf.LUIGI_DIRECTORY}/hotels'


class TransformHotels(luigi.Task):
    city_date = 'temp.csv'
    raw_data_filename = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f'{LUIGI_HOTELS_DIRECTORY}/{self.city_date}')

    def run(self):
        df = pd.read_csv(self.raw_data_filename, index_col=0)
        df_hotels = df[['hotel_id']]
        self.city_date = cmn.get_city_date(self.raw_data_filename)
        cmn.create_directory(LUIGI_HOTELS_DIRECTORY)
        df_hotels.to_csv(f'{LUIGI_HOTELS_DIRECTORY}/{self.city_date}')
