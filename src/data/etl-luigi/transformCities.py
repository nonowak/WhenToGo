import luigi
import extractRawFiles as erf
import config as conf
import pandas as pd
import common as cmn

CITIES_FILE_NAME = f'{conf.PROCESSED_DATA_DIRECTORY}/cities.csv'
LUIGI_CITIES_DIRECTORY = f'{conf.LUIGI_DIRECTORY}/cities'


class TransformCities(luigi.Task):
    cities_dictionary = luigi.DictParameter(
        default=pd.read_csv(CITIES_FILE_NAME, index_col=1, squeeze=True, header=None,
                            usecols=[0, 1], skiprows=1, dtype={0: int, 1: str}).to_dict(),
        is_global=True
    )
    city_date = 'temp.csv'
    raw_data_filename = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f'{LUIGI_CITIES_DIRECTORY}/{self.city_date}')

    def run(self):
        df = pd.read_csv(self.raw_data_filename, index_col=0)
        self.city_date = cmn.get_city_date(self.raw_data_filename)
        city = cmn.get_city(self.raw_data_filename)
        df['city'] = self.cities_dictionary[city]
        df_cities = df['city']
        cmn.create_directory(LUIGI_CITIES_DIRECTORY)
        df_cities.to_csv(f'{LUIGI_CITIES_DIRECTORY}/{self.city_date}')
