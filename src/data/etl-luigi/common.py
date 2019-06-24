from pathlib import Path


def create_directory(directory):
    output_dir = Path(directory)
    output_dir.mkdir(parents=True, exist_ok=True)


def get_city_date(filename):
    return filename[filename.rindex("/") + 1:]


def get_city(filename):
    return get_city_date(filename)[:-18]
