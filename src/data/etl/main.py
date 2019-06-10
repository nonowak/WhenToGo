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
    graph.add_chain(glob.glob(conf.RAW_DATA_DIRECTORY + "/**/*.csv", recursive=True), f.extract)
    graph.add_chain(s.merge_data, f.load, _input=None)
    graph.add_chain(d.transform_date, _input=f.extract, _output=s.merge_data)
    graph.add_chain(c.transform_city, _input=f.extract, _output=s.merge_data)
    graph.add_chain(t.transform_hour, _input=f.extract, _output=s.merge_data)
    graph.add_chain(h.transform_hotel, _input=f.extract, _output=s.merge_data)
    graph.add_chain(p.transform_price, _input=f.extract, _output=s.merge_data)
    return graph


def get_services():
    return {**h.get_services(), **d.get_services(), **t.get_services(), **c.get_services(), **f.get_services()}


if __name__ == '__main__':
    with bonobo.parse_args() as options:
        bonobo.run(get_graph(), services=get_services())
