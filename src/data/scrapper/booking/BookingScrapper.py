import config as conf
import pandas as pd
from multiprocessing import Manager, Pool
import os

def run_spider(city):
    os.system("python3 SpidersExecutive.py {}".format(city))

if __name__ == '__main__':
    cities = pd.read_csv(conf.CITIES_PATH, usecols=['city_ascii'])['city_ascii'].values
    manager = Manager()
    shared_list = manager.list(cities)
    pool = Pool(processes=conf.PROCESS_COUNT)
    pool.map(run_spider, cities)
    pool.close()