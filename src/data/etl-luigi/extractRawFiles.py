import luigi
import glob
import config as conf
import re
import pandas as pd
import common as cmn
from load import Load

LUIGI_RAW_DIRECTORY = f'{conf.LUIGI_DIRECTORY}/raw'


class ExtractRawFiles(luigi.Task):
    city = luigi.Parameter('')

    def run(self):
        for input_filename in glob.glob(f'{conf.RAW_DATA_DIRECTORY}/{self.city}/*.csv', recursive=True):
            df = pd.read_csv(input_filename, index_col=0)
            if not df.empty:
                scrape_date = input_filename[-20:-10]
                scrape_hour = input_filename[-9:-7]
                scrape_city = re.search('raw/(.*)/', input_filename).group(1)
                df = df.assign(**{'scrape_hour': scrape_hour,
                                  'scrape_date': scrape_date,
                                  'scrape_city': scrape_city}

                               )
                cmn.create_directory(LUIGI_RAW_DIRECTORY)
                scrape_directory = f'{LUIGI_RAW_DIRECTORY}/{scrape_city}_{scrape_date}_{scrape_hour}.csv'
                df.to_csv(scrape_directory)
                yield Load(raw_data_filename=scrape_directory)
