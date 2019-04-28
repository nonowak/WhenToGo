import bonobo
import bonobo.config
import bonobo.util
import glob
import csv
import config as conf

HOTELS_FILE_NAME = conf.PROCESSED_DATA_DIRECTORY + '/hotels.csv'
hotels = []
hotel_ids = []
hotel_names = []
last_date = ''
current_index = 0


@bonobo.config.use_context
def read_raw_csv(context, name):
    with open(name) as f:
        reader = csv.reader(f)
        headers = next(reader)
        if not context.output_type:
            context.set_output_fields(headers)
        for row in reader:
            yield tuple(row)


def save_hotels():
    with open(HOTELS_FILE_NAME, 'w') as f:
        writer = csv.writer(f)
        writer.writerows(hotels)


def init_hotels():
    try:
        with open(HOTELS_FILE_NAME, 'r') as f:
            reader = csv.reader(f)
            global hotels, hotel_ids, hotel_names, current_index
            hotels = list(reader)
            hotel_ids = get_col(hotels, 1)
            hotel_names = get_col(hotels, 2)
            current_index = int(hotels[-1][0])
    except FileNotFoundError:
        print('First initialization')


def get_col(list, col):
    return [row[col] for row in list]


def is_hotel_in_current_hotels(row):
    return row[0] in hotel_ids and row[1] in hotel_names


def get_file_names():
    return glob.glob(conf.RAW_DATA_DIRECTORY + "/**/*.csv", recursive=True)


def row_to_hotel(*row):
    yield row[1], row[2]


def add_hotel_to_list(*row):
    if not is_hotel_in_current_hotels(row):
        global current_index
        current_index = current_index + 1
        hotel_id = row[0]
        hotel_name = row[1]
        hotels.append([current_index, hotel_id, hotel_name])
        hotel_ids.append(hotel_id)
        hotel_names.append(hotel_name)
        yield len(hotels)


def get_graph(**options):
    graph = bonobo.Graph()
    file_names = get_file_names()
    graph.add_chain(file_names,
                    read_raw_csv,
                    row_to_hotel,
                    add_hotel_to_list)
    return graph


if __name__ == '__main__':
    init_hotels()
    with bonobo.parse_args() as options:
        bonobo.run(get_graph(**options))
    save_hotels()
