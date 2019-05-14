import bonobo
import hotel as h
import file as f
import glob
import config as conf


def get_graph():
    graph = bonobo.Graph()
    producer = graph.add_chain(glob.glob(conf.RAW_DATA_DIRECTORY + "/**/*.csv", recursive=True),
                               f.extract)
    graph = h.get_graph(graph, producer)
    return graph


if __name__ == '__main__':
    with bonobo.parse_args() as options:
        bonobo.run(get_graph(), services=h.get_services())
