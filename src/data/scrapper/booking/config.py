DEFAULT_DIRECTORY = "/home/nnowak/WhenToGo"
CITIES_PATH = "{}/data/interim/cities.csv".format(DEFAULT_DIRECTORY)
SCRAPPING_DIRECTORY = "{}/raw".format(DEFAULT_DIRECTORY)
SPIDERS_EXECUTIVE_DIRECTORY = "{}/src/data/scrapper/booking/SpidersExecutive.py".format(DEFAULT_DIRECTORY)
ALLOWED_DOMAINS = ['booking.com']
BASE_URL = "https://www.booking.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0(Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Mobile Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Host": "www.booking.com"
}
PROCESS_COUNT = 4
