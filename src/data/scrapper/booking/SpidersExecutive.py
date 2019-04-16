import logging
import os
from datetime import datetime

import DateHelper as dh
import config as conf
import pandas as pd
import spiders.BookingSpider as bs
from scrapy import signals
from scrapy.crawler import CrawlerProcess, Crawler
from scrapy.utils.project import get_project_settings
import sys

logging.getLogger('scrapy').propagate = False


class SpidersExecutive:
    result = pd.DataFrame(columns=['hotel_id', 'name', 'score'])

    def __init__(self, city):
        self.__city = city
        self.__stay_length = 1
        self.execute()

    def prepare_directory(self, directory_path=None):
        os.makedirs(directory_path, exist_ok=True)

    def add_day_of_week(self):
        self.result['day_of_week_scrapping'] = dh.day_of_week()

    def all_spiders_done(self):
        self.add_day_of_week()
        directory_path = "{}/{}".format(conf.SCRAPPING_DIRECTORY, self.__city)
        self.prepare_directory(directory_path)
        self.result.to_csv('{}/booking_days_{}_date_{}.csv'.format(directory_path,
                                                                   self.__stay_length,
                                                                   datetime.now().strftime("%Y-%m-%d_%H_%M")), sep=',')

    def spider_done(self, spider):
        self.result = pd.merge(self.result, spider.result_data_frame, on=['hotel_id', 'name', 'score'], how='outer')

    def execute(self):
        start_time = datetime.now()
        date_columns = {"weekend": dh.next_friday(),
                        "today": datetime.now(),
                        "autumn": dh.autumn_date(),
                        "summer": dh.summer_date(),
                        "spring": dh.spring_date(),
                        "winter": dh.winter_date(),
                        "new_year": dh.new_year_date(),
                        "one_month": dh.current_date_plus_months(1),
                        "three_months": dh.current_date_plus_months(3),
                        "five_months": dh.current_date_plus_months(5)
                        }
        spiders_executor = CrawlerProcess()
        for process_name, date in date_columns.items():
            booking_crawler = Crawler(bs, get_project_settings())
            booking_crawler.signals.connect(self.spider_done, signals.spider_closed)
            booking_crawler.signals.connect(self.all_spiders_done, signals.engine_stopped)
            spiders_executor.crawl(booking_crawler, column_name=process_name, date=date, city=self.__city)
        if len(spiders_executor.crawlers) < len(date_columns):
            print("Less crawlers than date_columns")
            self.execute()
        spiders_executor.start()
        print("Scrapping {} in {}s".format(self.__city, str((datetime.now() - start_time).seconds)))


if __name__ == '__main__':
    SpidersExecutive(sys.argv[1])