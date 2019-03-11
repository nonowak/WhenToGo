from datetime import datetime
from datetime import timedelta
import pandas as pd
import logging
from functools import reduce
from scrapy.crawler import CrawlerProcess, Crawler
from scrapy import signals
from scrapy.utils.project import get_project_settings
import sys
import importlib.util
spec = importlib.util.spec_from_file_location("BookingSpider", "./spiders/BookingSpider.py")
bs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bs)

logging.getLogger('scrapy').propagate = False

class BookingCrawler():
    dfNextWeekend = None
    dfToday = None
    dfDate = None
    daysOfStay = int(sys.argv[1])
    path = sys.argv[2]

    def nextFriday(self):
        d = datetime.now()
        while d.weekday() != 4:
            d = d + timedelta(1)
        return d

    def saveToFile(self):
        frames = [self.dfDate, self.dfNextWeekend,self.dfToday]
        result = reduce(lambda l,r: pd.merge(l,r, on=['hotel_id', 'name', 'score'], how='outer'), frames)
        result.to_csv('{}booking_days_{}_date_{}_.csv'.format(self.path, self.daysOfStay, datetime.now().strftime("%Y-%m-%d_%H_%M")), sep=',')

    def spider1_closed(self, spider, reason):
        self.dfNextWeekend = spider.resultHotels
        self.dfNextWeekend.rename(columns = {'price':'price_weekend'}, inplace=True)

    def spider2_closed(self, spider, reason):
        self.dfToday = spider.resultHotels
        self.dfToday.rename(columns = {'price':'price_today'}, inplace=True)

    def spider3_closed(self, spider, reason):
        self.dfDate = spider.resultHotels
        self.dfDate.rename(columns = {'price':'price_30-11-2018'}, inplace=True)

    def main(self):
        now = datetime.now()

        print("Start Scrapping " + str(now))
        process = CrawlerProcess()

        spider1 = bs.BookingSpider()
        spider2 = bs.BookingSpider()
        spider3 = bs.BookingSpider()

        crawler1 = Crawler(spider1, get_project_settings())
        crawler1.signals.connect(self.spider1_closed, signals.spider_closed)

        crawler2 = Crawler(spider2, get_project_settings())
        crawler2.signals.connect(self.spider2_closed, signals.spider_closed)

        crawler3 = Crawler(spider3, get_project_settings())
        crawler3.signals.connect(self.spider3_closed, signals.spider_closed)

        crawler1.signals.connect(self.saveToFile, signals.engine_stopped)
        crawler2.signals.connect(self.saveToFile, signals.engine_stopped)
        crawler3.signals.connect(self.saveToFile, signals.engine_stopped)

        process.crawl(crawler1, date = self.nextFriday(), daysOfStay = self.daysOfStay)
        process.crawl(crawler2, date = datetime.now(), daysOfStay = self.daysOfStay)
        process.crawl(crawler3, date = datetime(2018, 11, 30), daysOfStay = self.daysOfStay)

        process.start()

        print("End Scrapping " + str(datetime.now()))

if __name__ == "__main__":
    crawler = BookingCrawler()
    crawler.main()