import requests
from bs4 import BeautifulSoup
import json
from pathlib import Path

def fetch_games_workouts():
    years = range(2007,MAX_YEAR+1)
    days_of_week = ['sunday','monday','tuesday','wednesday','thursday','friday','saturday']
    workouts_all = []
    for year in years:
        for slug in ['games','finals']:
            try:
                url = f'https://games.crossfit.com/workouts/{slug}/{year}'
                r = requests.get(url)
                r.raise_for_status()
                soup = BeautifulSoup(r.text, 'html.parser')
                break
            except Exception as e:
                pass

        events = soup.find('ol', attrs={'class':'events'})
        if events is None:
            continue
        cols = events.find_all('li', attrs={'class':'panel'})

        workouts = []
        for col in cols:
            class_values = col.attrs['class']
            out = {
                'year': year,
                'workout_name': col.find('header').text.strip(),
                'description': col.find('div',attrs={'class':'description'}).text.strip()
            }
            for class_value in class_values:
                for day in days_of_week:
                    if day in class_value:
                        out['date'] = class_value
                        break
            workouts.append(out)
        workouts_all.extend(workouts)

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

def fetch_open_workout_old(
    year: int
):
    url = f"https://games.crossfit.com/workouts/open/{year}"
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
    
def fetch_open_workouts():
    workouts_all = []
    for year in range(2011,MAX_YEAR+1):
        if year <= 2016:
            workouts = fetch_open_workout_old(year)
        else:
            workouts = fetch_stage_workout('open', year)
        workouts_all.extend(workouts)
    return workouts_all

def fetch_quarterfinals_workouts():
    years = range(2021,MAX_YEAR+1)
    workouts_all = []
    for year in years:
        if year == 2025:
            continue
        workouts = fetch_stage_workout('quarterfinalsindividual', year)
        workouts_all.extend(workouts)
    return workouts_all

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

def fetch_semifinals_workouts():
    years = range(2021,MAX_YEAR+1)
    workouts_all = []
    for year in years:
        if year == 2022:
            workouts = fetch_semis_2022()
        else:
            workouts = fetch_stage_workout('semifinals', year,max_seq=8)
        workouts_all.extend(workouts)
    return workouts_all

def dump_workouts(workouts: list, file_path: str | Path):
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, 'w') as f:
        for workout in workouts:
            f.write(json.dumps(workout) + '\n')

if __name__ == '__main__':
    import os
    from dotenv import load_dotenv
    load_dotenv()

    MAX_YEAR = 2026
    OVERWRITE = False

    root_dir = Path(os.getenv('ELITE_COMPETITION_DATA_PATH'))
    cf_workouts_path = root_dir / 'crossfit' / 'workouts'
    cf_workouts_path.mkdir(parents=True, exist_ok=True)

    fn = cf_workouts_path / 'open.jsonl'
    if fn.exists() and not OVERWRITE:
        print(f'{fn} already exists, skipping')
    else:
        print('Fetching open workouts...',end='')
        open_workouts = fetch_open_workouts()
        dump_workouts(open_workouts, fn)
        print('done')

    fn = cf_workouts_path / 'quarterfinals.jsonl'
    if fn.exists() and not OVERWRITE:
        print(f'{fn} already exists, skipping')
    else:
        print('Fetching quarterfinals workouts...',end='')
        quarterfinals_workouts = fetch_quarterfinals_workouts()
        dump_workouts(quarterfinals_workouts, fn)
        print('done')

    fn = cf_workouts_path / 'semis.jsonl'
    if fn.exists() and not OVERWRITE:
        print(f'{fn} already exists, skipping')
    else:
        print('Fetching semis workouts...',end='')
        semis_workouts = fetch_semifinals_workouts()
        dump_workouts(semis_workouts, fn)
        print('done')

    fn = cf_workouts_path / 'games.jsonl'
    if fn.exists() and not OVERWRITE:
        print(f'{fn} already exists, skipping')
    else:
        print('Fetching games workouts...',end='')
        games_workouts = fetch_games_workouts()
        dump_workouts(games_workouts, fn)
        print('done')