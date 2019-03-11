from scrapy.item import Item, Field
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.http.request import Request
from datetime import datetime
from datetime import timedelta
import pandas as pd

import importlib.util

spec = importlib.util.spec_from_file_location("config", "./config.py")
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)

class BookingSpider(scrapy.Spider):
    name = 'NextWeekendSpider'
    allowed_domains = config.ALLOWED_DOMAINS
    resultHotels = pd.DataFrame(columns=['hotel_id', 'name', 'score', 'price'])
    date = None
    daysOfStay = None

    def __init__(self, date=None, daysOfStay=None, *args, **kwargs):
        self.date = date
        self.daysOfStay = daysOfStay

    def start_requests(self):
        dateDaysOfStay = self.date + timedelta(days=self.daysOfStay)
        yield Request(url="{}/searchresults.pl.html?ss=Warszawa&is_ski_area=0&dest_type=city&checkin_monthday={}&checkin_month={}&checkin_year={}&checkout_monthday={}&checkout_month={}&checkout_year={}&no_rooms=1&group_adults=2&group_children=0".format(config.BASE_URL,self.date.day, self.date.month, self.date.year,dateDaysOfStay.day, dateDaysOfStay.month, dateDaysOfStay.year),headers=config.HEADERS, callback=self.parse)

    def parse(self, response):
        url = parseUrl(response)
        for hotel in response.css('li.sr-card'):
            self.resultHotels = self.resultHotels.append({
                'hotel_id': parseHotelId(hotel),
                'name': hotel.css('h3.sr-card__name::text').extract_first().strip(),
                'score': parseScore(hotel),
                'price': parsePrice(hotel)
            }, ignore_index=True)
        if url != None:
            yield Request(url = '{}{}'.format(config.BASE_URL, url), headers=config.HEADERS, callback=self.parse)


def parseUrl(response):
    tempUrl = response.css('a.js-pagination-next-link::attr(href)').extract_first()
    return None if tempUrl is None else tempUrl.strip()

def parsePrice(hotel):
    tempPrice = hotel.css('span.sr-card__price-cheapest::text').extract_first()
    return None if tempPrice is None else tempPrice.strip()

def parseScore(hotel):
    tempScore = hotel.css('span.sr-card__review-score::text').extract_first()
    return None if tempScore is None else float(tempScore.strip().replace(',','.'))

def parseHotelId(hotel):
    startIndex = str(hotel).index('id="hotel_') + len('id="hotel_')
    endIndex = str(hotel).index('"', startIndex)
    tempHotelId = str(hotel)[startIndex:endIndex]
    return None if tempHotelId is None else tempHotelId