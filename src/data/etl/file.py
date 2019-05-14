import bonobo.config
import csv
import glob
import config as conf


@bonobo.config.use_context
def extract(context, name):
    with open(name) as f:
        reader = csv.reader(f)
        headers = next(reader)
        if not context.output_type:
            context.set_output_fields(headers)
        for row in reader:
            yield tuple(row)
