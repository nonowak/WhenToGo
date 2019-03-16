from datetime import timedelta

import pandas as pd
import scrapy
import config as conf
from scrapy.http.request import Request


def parse_url(response):
    tempUrl = response.css('a.js-pagination-next-link::attr(href)').extract_first()
    return None if tempUrl is None else tempUrl.strip()


def parse_price(hotel):
    tempPrice = hotel.css('span.sr-card__price-cheapest::text').extract_first()
    return None if tempPrice is None else tempPrice.strip()


def parse_score(hotel):
    # tempScore = hotel.css('span.sr-card__review-score::text').extract_first()
    tempScore = hotel.css('div.bui-review-score__badge::text').extract_first()
    return None if tempScore is None else float(tempScore.strip().replace(',', '.'))


def parse_hotel_id(hotel):
    hotelId = None
    try:
        startIndex = str(hotel).index('id="hotel_') + len('id="hotel_')
        endIndex = str(hotel).index('"', startIndex)
        hotelId = str(hotel)[startIndex:endIndex]
    except:
        pass
    return hotelId


class BookingSpider(scrapy.Spider):
    result_data_frame = pd.DataFrame()

    def __init__(self, date=None, stay_length=1, city="", column_name="", **kwargs):
        self.__name = column_name + "_spider"
        super().__init__(**kwargs, name=self.__name)
        self.__date = date
        self.__stay_length = stay_length
        self.__column_name = "price_" + column_name
        self.__end_date = self.__date + timedelta(days=self.__stay_length)
        self.__city = city
        self.result_data_frame = pd.DataFrame(columns=['hotel_id', 'name', 'score', self.__column_name])

    def start_requests(self):
        yield Request(
            url="{}/searchresults.pl.html?ss={}&is_ski_area=0&dest_type=city&"
                "checkin_monthday={}&checkin_month={}&checkin_year={}"
                "&checkout_monthday={}&checkout_month={}&checkout_year={}"
                "&no_rooms=1&group_adults=2&group_children=0"
                "&rows=100"
                .format(conf.BASE_URL, self.__city,
                        self.__date.day, self.__date.month, self.__date.year,
                        self.__end_date.day, self.__end_date.month, self.__end_date.year),
            headers=conf.HEADERS,
            callback=self.parse)

    def parse(self, response):
        url = parse_url(response)
        for body in response.css('li.sr-card'):
            price = parse_price(body)
            hotel_id = parse_hotel_id(body)
            score = parse_score(body)
            if hotel_id is not None and price is not None:
                self.result_data_frame = self.result_data_frame.append({
                    'hotel_id': hotel_id,
                    'name': body.css('h3.sr-card__name::text').extract_first().strip(),
                    'score': score,
                    self.__column_name: price
                }, ignore_index=True)
        if url is not None:
            yield Request(url='{}{}'.format(conf.BASE_URL, url), headers=conf.HEADERS, callback=self.parse)
