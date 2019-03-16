import logging
import os
from datetime import datetime
from datetime import timedelta

import config as conf
import pandas as pd
import spiders.BookingSpider as bs
from scrapy import signals
from scrapy.crawler import CrawlerProcess, Crawler
from scrapy.utils.project import get_project_settings
import sys

logging.getLogger('scrapy').propagate = False


def next_friday():
    d = datetime.now()
    while d.weekday() != 4:
        d = d + timedelta(1)
    return d


class SpidersExecutive:
    result = pd.DataFrame(columns=['hotel_id', 'name', 'score'])

    def __init__(self, city):
        self.__city = city
        self.__stay_length = 1
        self.execute()

    def prepare_directory(self, directory_path=None):
        os.makedirs(directory_path, exist_ok=True)

    def all_spiders_done(self):
        directory_path = "{}/{}".format(conf.SCRAPPING_DIRECTORY, self.__city)
        self.prepare_directory(directory_path)
        self.result.to_csv('{}/booking_days_{}_date_{}.csv'.format(directory_path,
                                                                    self.__stay_length,
                                                                    datetime.now().strftime("%Y-%m-%d_%H_%M")), sep=',')

    def spider_done(self, spider):
        self.result = pd.merge(self.result, spider.result_data_frame, on=['hotel_id', 'name', 'score'], how='outer')

    def execute(self):
        start_time= datetime.now()
        date_columns = {"weekend": next_friday(), "today": datetime.now()}
        spiders_executor = CrawlerProcess()
        print("Start Scrapping {}: {}".format(self.__city, str(start_time)))
        for process_name, date in date_columns.items():
            booking_crawler = Crawler(bs, get_project_settings())
            booking_crawler.signals.connect(self.spider_done, signals.spider_closed)
            booking_crawler.signals.connect(self.all_spiders_done, signals.engine_stopped)
            spiders_executor.crawl(booking_crawler, column_name=process_name, date=date, city=self.__city)
        spiders_executor.start()
        print("End Scrapping {}: {}".format(self.__city, str(datetime.now())))

if __name__ == '__main__':
    SpidersExecutive(sys.argv[1])