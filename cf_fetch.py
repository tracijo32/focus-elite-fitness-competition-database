
from api import CrossFitClient
import pandas as pd
pd.set_option('display.max_columns', None)
from tqdm import tqdm
from dotenv import load_dotenv
import os, json
from pathlib import Path

load_dotenv()

MAX_PAGES = 200
root_path = os.getenv('ELITE_COMPETITION_DATA_PATH')
crossfit_path = Path(root_path) / 'crossfit'
crossfit_path.mkdir(parents=True, exist_ok=True)


cf = CrossFitClient()
events = cf.get_events()
with open(crossfit_path / 'events.json', 'w') as f:
    json.dump(events, f)


comp_types = ['open','games','quarterfinalsindividual','regional']
events_to_pull = [
    e for e in events 
    if e['type'] in comp_types and
     e['slug'] != 'all' and 
    e['leaderboard_mode'] == 'completed'
]

lb_path = Path(root_path) / 'crossfit' / 'leaderboard'
lb_path.mkdir(parents=True, exist_ok=True)

event_iterator = tqdm(events_to_pull, desc="CrossFit events", unit="event")
for event in event_iterator:
    event_iterator.set_postfix_str(f"id={event['id']}")
    for division in tqdm([1,2], desc=f"Divisions for {event['id']}", unit="division", leave=False):
        fn = f'{event["id"]}_{division}_1.json'
        if not os.path.exists(lb_path / fn):
            data = cf.get_leaderboard_page(event['id'], division)
            with open(lb_path / fn, 'w') as f:
                json.dump(data, f)
        else:
            with open(lb_path / fn, 'r') as f:
                data = json.load(f)
        total_pages = int(data['pagination']['totalPages'])
        if total_pages > 1:
            pages = range(2, min(total_pages, MAX_PAGES) + 1)
            for page in tqdm(
                pages,
                desc=f"Pages e{event['id']} d{division}",
                unit="page",
                leave=False
            ):
                fn = f'{event["id"]}_{division}_{page}.json'
                if not os.path.exists(lb_path / fn):
                    data = cf.get_leaderboard_page(event['id'], division, page)
                    with open(lb_path / fn, 'w') as f:
                        json.dump(data, f)