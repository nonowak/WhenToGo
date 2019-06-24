import luigi
import extractRawFiles as erf
import config as conf
import pandas as pd
import common as cmn

LUIGI_PRICES_DIRECTORY = f'{conf.LUIGI_DIRECTORY}/prices'


class TransformPrices(luigi.Task):
    city_date = 'temp.csv'
    price_columns = ['price_new_year', 'price_spring', 'price_weekend', 'price_one_month', 'price_winter',
                     'price_today', 'price_autumn', 'price_five_months', 'price_three_months', 'price_summer']

    price_dates_columns = ['day_of_week_new_year', 'day_of_week_spring', 'day_of_week_weekend', 'day_of_week_one_month',
                           'day_of_week_winter', 'day_of_week_today',
                           'day_of_week_autumn', 'day_of_week_five_months', 'day_of_week_three_months',
                           'day_of_week_summer']
    raw_data_filename = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f'{LUIGI_PRICES_DIRECTORY}/{self.city_date}')

    def run(self):
        df = pd.read_csv(self.raw_data_filename, index_col=0)
        df_prices = df[self.price_columns]
        df_prices.applymap(remove_currency)
        df_dates = df[self.price_dates_columns]
        df_prices = df_prices.join(df_dates)
        self.city_date = cmn.get_city_date(self.raw_data_filename)
        cmn.create_directory(LUIGI_PRICES_DIRECTORY)
        df_prices.to_csv(f'{LUIGI_PRICES_DIRECTORY}/{self.city_date}')


def remove_currency(price):
    return None if str(price) == 'nan' else str(price[:-3]).strip()
