import bonobo
import glob
import hotel as h
import file as f
import hour as t
import date as d
import city as c
import price as p
import scrape as s
import config as conf


def get_graph():
    graph = bonobo.Graph()
    producer = graph.add_chain(glob.glob(conf.RAW_DATA_DIRECTORY + "/**/*.csv", recursive=True),
                               f.extract,
                               d.transform_date,
                               c.transform_city,
                               t.transform_hour,
                               h.transform_hotel,
                               p.transform_price,
                               s.load_scrape)
    return graph


def get_services():
    return {**h.get_services(), **d.get_services(), **t.get_services(), **c.get_services()}


if __name__ == '__main__':
    with bonobo.parse_args() as options:
        bonobo.run(get_graph(), services=get_services())
