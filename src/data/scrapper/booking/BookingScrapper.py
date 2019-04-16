import config as conf
import pandas as pd
from multiprocessing import Manager, Pool
import os
from datetime import datetime
from notification.NotificationHandler import scan_directory

#
def run_spider(city):
    city = city.replace(' ', '+')
    os.system("python3 {} {}".format(conf.SPIDERS_EXECUTIVE_DIRECTORY, city))


if __name__ == '__main__':
    start_time = datetime.now()
    cities = pd.read_csv(conf.CITIES_PATH, usecols=['city_ascii'])['city_ascii'].values
    manager = Manager()
    shared_list = manager.list(cities)
    pool = Pool(processes=conf.PROCESS_COUNT)
    pool.map(run_spider, cities)
    pool.close()
    print("Scrapped {} cities in {}s".format(len(cities), str((datetime.now() - start_time).seconds)))
    scan_directory(conf.SCRAPPING_DIRECTORY)

