from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta

def next_friday():
    d = datetime.now()
    while d.weekday() != 4:
        d = d + timedelta(1)
    return d


def day_of_week():
    return datetime.today().weekday()

def is_after_date(date):
    return datetime.now() > date

def plus_year_if_after(date):
    return date + relativedelta(years=1) if is_after_date(date) else date

def spring_date():
    return plus_year_if_after(datetime(2019, 4, 15))

def summer_date():
    return plus_year_if_after(datetime(2019, 7, 15))

def autumn_date():
    return plus_year_if_after(datetime(2019, 10, 15))

def winter_date():
    return plus_year_if_after(datetime(2019, 1, 15))

def new_year_date():
    return plus_year_if_after(datetime(2019, 12, 31))

def current_date_plus_months(month_count):
    return datetime.now() + relativedelta(months=month_count)