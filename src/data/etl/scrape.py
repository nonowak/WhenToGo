import bonobo.config
import bonobo.util


@bonobo.config.use('not_saved_scrapes')
def merge_data(data, not_saved_scrapes):
    scrape_id = int(data['scrape_id'])
    data.pop('scrape_id', None)
    current_scrape = not_saved_scrapes.get(scrape_id)
    if not_saved_scrapes.get(scrape_id) is None:
        not_saved_scrapes[scrape_id] = {**data}
    else:
        not_saved_scrapes[scrape_id] = {**current_scrape, **data}
    if len(not_saved_scrapes[scrape_id]) == 15:
        yield {
            'scrape_id': scrape_id,
            **not_saved_scrapes[scrape_id]
        }
