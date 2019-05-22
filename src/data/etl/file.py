import bonobo.config
import csv
import re


@bonobo.config.use_context
def extract(context, name):
    with open(name) as f:
        reader = csv.reader(f)
        headers = next(reader)
        date = name[-20:-10]
        hour = name[-9:-7]
        city = re.search('raw/(.*)/', name).group(1)
        if not context.output_type:
            context.set_output_fields(headers)
        for row in reader:
            yield {
                'row': tuple(row),
                'date': date,
                'hour': hour,
                'city': city
            }