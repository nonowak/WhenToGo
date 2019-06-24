import luigi
import config as conf
import pandas as pd
import common as cmn
import transformHours as th
import transformDates as td
import transformHotels as tho
import transformPrices as tp
import transformCities as tc
import os

LUIGI_OUTPUT_DIRECTORY = f'{conf.LUIGI_DIRECTORY}/output'


class Load(luigi.WrapperTask):
    city_date = 'UNDEFINED.csv'
    city = 'UNDEFINED'
    raw_data_filename = luigi.Parameter('')

    def requires(self):
        return [
            th.TransformHours(raw_data_filename=self.raw_data_filename),
            td.TransformDates(raw_data_filename=self.raw_data_filename),
            tc.TransformCities(raw_data_filename=self.raw_data_filename),
            tho.TransformHotels(raw_data_filename=self.raw_data_filename),
            tp.TransformPrices(raw_data_filename=self.raw_data_filename)
        ]

    def get_output_filename(self):
        return f'{LUIGI_OUTPUT_DIRECTORY}/{self.city}/{self.city_date}'

    def prepare_local_variables(self, input_filename):
        if self.city_date == 'UNDEFINED.csv':
            self.city_date = cmn.get_city_date(input_filename)
        if self.city == 'UNDEFINED':
            self.city = cmn.get_city(input_filename)

    def parse_df(self, result, temp):
        return temp if result.empty else result.join(temp)

    def save_df_to_file(self, df):
        cmn.create_directory(f'{LUIGI_OUTPUT_DIRECTORY}/')
        filename = f'{LUIGI_OUTPUT_DIRECTORY}/{self.city}.csv'
        if os.path.exists(filename):
            with open(filename, 'a') as file:
                df.to_csv(file, header=False, index=False)
        else:
            with open(filename, 'w') as file:
                df.to_csv(file, index=False)

    def run(self):
        df_result = pd.DataFrame()
        for input_file in self.input():
            input_filename = input_file.path
            self.prepare_local_variables(input_filename)
            df_temp = pd.read_csv(input_filename, index_col=0)
            df_result = self.parse_df(df_result, df_temp)
        self.save_df_to_file(df_result)
