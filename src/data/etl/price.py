def remove_currency(price):
    return price[:-3]


def transform_price(data):
    yield {
        'scrape_id': data['scrape_id'],
        'price_new_year': remove_currency(data['price_new_year']),
        'price_spring': remove_currency(data['price_spring']),
        'price_weekend': remove_currency(data['price_weekend']),
        'price_one_month': remove_currency(data['price_one_month']),
        'price_winter': remove_currency(data['price_winter']),
        'price_today': remove_currency(data['price_today']),
        'price_autumn': remove_currency(data['price_autumn']),
        'price_five_months': remove_currency(data['price_five_months']),
        'price_three_months': remove_currency(data['price_three_months']),
        'price_summer': remove_currency(data['price_summer']),
    }
