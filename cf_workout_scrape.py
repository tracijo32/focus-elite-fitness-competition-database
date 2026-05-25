import requests
from bs4 import BeautifulSoup
import json
from pathlib import Path
from typing import Callable

def fetch_games_workouts(year: int):
    days_of_week = ['sunday','monday','tuesday','wednesday','thursday','friday','saturday']
    for slug in ['games','finals']:
        try:
            url = f'https://games.crossfit.com/workouts/{slug}/{year}'
            r = requests.get(url)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, 'html.parser')
            break
        except Exception as e:
            pass

    if year == 2021:
        return parse_games_2021(soup)

    events = soup.find('ol', attrs={'class':'events'})
    if events is None:
        return []
    cols = events.find_all('li', attrs={'class':'panel'})

    workouts = []
    for i, col in enumerate(cols):
        class_values = col.attrs['class']
        out = {
            'year': year,
            'seq': i+1,
            'workout_name': col.find('header').text.strip(),
            'description': col.find('div',attrs={'class':'description'}).text.strip()
        }
        for class_value in class_values:
            for day in days_of_week:
                if day in class_value:
                    out['date'] = class_value
                    break
        workouts.append(out)
    return workouts

def parse_games_2021(soup: BeautifulSoup):
    comp_days = soup.find_all('h2',attrs={'class':'calendar-heading'})

    workouts = []
    for day in comp_days:
        w = {}
        name = None
        for s in day.next_siblings:
            if s.text.startswith('Event'):
                name = s.text
            elif name is not None:
                w.setdefault(name,[]).append(s.text)

        w = [
            {
                'year': 2021,
                'date': day.text,
                'seq': i,
                'workout_name': k,
                'description': ''.join(v)
            }
            for i, (k,v) in enumerate(w.items())
        ]
        workouts.extend(w)

    return workouts

def fetch_stage_workout(
    stage: str,
    year: int,
    max_seq: int = 5
):
    workouts = []
    for seq in range(1,max_seq+1):
        url = f"https://games.crossfit.com/workouts/{stage}/{year}/{seq}"
        r = requests.get(url,params={'division':1})
        soup = BeautifulSoup(r.text, 'html.parser')

        div = soup.find('div', attrs={'class':'exercises'})
        if div is None:
            continue
        description = div.text.strip()

        out = {
            'year': year,
            'seq': seq,
            'description': description
        }
        workouts.append(out)

    return workouts

def fetch_stage_workout_old(
    year: int,
    stage: str
):
    url = f"https://games.crossfit.com/workouts/{stage}/{year}"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')

    workouts = []
    for seq in range(1,6):
        tab = soup.find('li',attrs={'id':f'workoutsTab{seq}'})
        if tab is None:
            continue
        description = tab.find('section').text
        out = {
            'year': year,
            'seq': seq,
            'description': description
        }
        workouts.append(out)

    return workouts
    
def fetch_open_workouts(year: int):
    workouts = []
    if year <= 2016:
        workouts = fetch_stage_workout_old(year, 'open')
    else:
        workouts = fetch_stage_workout('open', year)
    return workouts

def fetch_quarterfinals_workouts(year: int):
    if year == 2025:
        return None
    return fetch_stage_workout('quarterfinalsindividual', year)

def fetch_semis_2022():
    url = "https://games.crossfit.com/workouts/semifinals/individual/2022"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')

    workouts = []
    tables = soup.find_all('table')
    for table in tables:
        title = table.find('thead').text.strip()
        for i,td in enumerate(table.find('tbody').find_all('td')):
            workout = {
                'year': 2022,
                'comp': title,
                'seq': i+1,
                'description': td.text.strip()
            }
            workouts.append(workout)
    return workouts

def fetch_semifinals_workouts(year: int):
    if year == 2022:
        workouts = fetch_semis_2022()
    else:
        workouts = fetch_stage_workout('semifinals', year,max_seq=8)
    return workouts

def fetch_regionals_workouts(year: int):
    if year <= 2016:
        return fetch_stage_workout_old(year, 'regionals')
    else:
        return fetch_stage_workout('regionals', year,max_seq=6)

def dump_workouts(workouts: list, file_path: str | Path):
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, 'w') as f:
        for workout in workouts:
            f.write(json.dumps(workout) + '\n')

def run_scrape(
    scrape_fn: Callable,
    comp_type: str,
    years: list[int], 
    root_path: str | Path,
    overwrite: bool = False
):
    root_path = Path(root_path)
    root_path.mkdir(parents=True, exist_ok=True)

    for year in years:
        fn = root_path / f'{comp_type}_{year}.jsonl'
        if fn.exists() and not overwrite:
            continue
        workouts = scrape_fn(year)
        if workouts is None:
            continue
        dump_workouts(workouts, fn)

if __name__ == '__main__':
    cf_workouts_path = 'crossfit_workouts/raw'

    run_scrape(
        scrape_fn=fetch_games_workouts,
        comp_type='games',
        years=range(2007,2026),
        root_path=cf_workouts_path,
        overwrite=True
    )

    # run_scrape(
    #     scrape_fn=fetch_open_workouts,
    #     comp_type='open',
    #     years=range(2011,2027),
    #     root_path=cf_workouts_path,
    #     overwrite=False
    # )

    # run_scrape(
    #     scrape_fn=fetch_quarterfinals_workouts,
    #     comp_type='quarterfinals',
    #     years=range(2021,2027),
    #     root_path=cf_workouts_path,
    #     overwrite=False
    # )

    # run_scrape(
    #     scrape_fn=fetch_semifinals_workouts,
    #     comp_type='semifinals',
    #     years=range(2021,2026),
    #     root_path=cf_workouts_path,
    #     overwrite=False
    # )

    # run_scrape(
    #     scrape_fn=fetch_regionals_workouts,
    #     comp_type='regionals',
    #     years=range(2011,2019),
    #     root_path=cf_workouts_path,
    #     overwrite=False
    # )