import json, requests
import pandas as pd
from google.cloud import storage
from parameters import GoogleCloudParameters
from unidecode import unidecode
from models import Entrant, Score, Metadata
from util import recover_points_table

gcp_params = GoogleCloudParameters()
storage_client = storage.Client(project=gcp_params.project_id)
bucket = storage_client.bucket(gcp_params.bucket_name)

def fix_name_column(col: pd.Series):
    col = col.apply(unidecode)\
        .str.replace(r'\s+', ' ',regex=True)\
            .str.strip()
    return col

def file_exists(file_path,gcp=False):
    if gcp:
        blob = bucket.blob(file_path)
        return blob.exists()
    else:
        import os
        return os.path.exists(file_path)

def load_data(file_path,gcp=False):
    if gcp:
        blob = bucket.blob(file_path)
        return blob.download_as_string()
    else:
        with open(file_path, 'r') as f:
            return f.read()

def dump_data(data,file_path,gcp=False):
    if gcp:
        blob = bucket.blob(file_path)
        blob.upload_from_string(data)
    else:
        with open(file_path, 'w') as f:
            f.write(data)

def solve_points_table(
    entrants: list[Entrant],
    scores: list[Score],
    gcp: bool = True,
    refresh: bool = False,
) -> list[Score]:
    comp_id = scores[0].source_comp_id

    blob = f"manual/raw/{comp_id}/points_table.json"
    if file_exists(blob,gcp) and not refresh:
        points_table = json.load(open(blob))
    else:
        points_table = {
            gender: recover_points_table(
                [e for e in entrants if e.gender == gender],
                [s for s in scores if s.gender == gender]
            )
            for gender in ["M","F"]
        }
        dump_data(json.dumps(points_table),blob,gcp)
        
    scores_new = [
        Score(**{
            **score.model_dump(),
            'points':points_table[score.gender].get(score.rank,0)
        })
        for score in scores
    ]
    return scores_new

def parse_scc2019_leaderboard(gcp=False):
    comp_id = 'scc2019'
    lb_json = load_data(f'manual/raw/{comp_id}/{comp_id}_leaderboard.json',gcp=gcp)
    lb = pd.DataFrame(json.loads(lb_json)).rename(columns={'name':'display_name'})\
        .assign(source_comp_id='scc2019')
    lb['source_athlete_id'] = lb['source_comp_id'] + '_' + \
        lb['gender'] + '_' + \
        lb['display_name'].rank().astype(int).astype(str)
    lb['dq'] = False

    entrants = [
        Entrant(**row.dropna())
        for _, row in lb.iterrows()
    ]
    entrants_json = "\n".join([e.model_dump_json() for e in entrants])
    dump_data(entrants_json,f"manual/parsed/{comp_id}/entrants.ndjson",gcp)
    return

def parse_lcq2025_leaderboard(
    gcp=False
):
    comp_id = 'lcq2025'
    dfs = []
    for gender in ['male','female']:
        data_json = load_data(f'manual/raw/{comp_id}/leaderboard_{gender}.json',gcp=gcp)
        data = json.loads(data_json)
        df = pd.DataFrame(data).assign(gender=gender[0].upper())
        dfs.append(df)

    df = pd.concat(dfs).assign(source_comp_id=comp_id)\
        .rename(columns={
            'name': 'display_name',
            'total_points': 'overall_points'
            })
    df['source_athlete_id'] = df['source_comp_id'] + '_' + \
        df['gender'] + '_' + \
        df['display_name'].rank().astype(int).astype(str)
    df['dq'] = False

    entrants = [
        Entrant(**row.dropna())
        for _, row in df.iterrows()
    ]
    entrants_json = "\n".join([e.model_dump_json() for e in entrants])
    dump_data(entrants_json,f"manual/parsed/{comp_id}/entrants.ndjson",gcp)

    df['scores'] = df['scores'].apply(
        lambda x: [{'source_workout_id': k, **v} for k,v in x.items()]
    )

    df = df[['source_athlete_id','scores','source_comp_id','gender']]\
        .explode('scores').reset_index(drop=True)
    df = pd.merge(
        df.drop(columns=['scores']),
        df['scores'].apply(pd.Series),
        left_index=True,
        right_index=True
    ).rename(columns={'score': 'score_display','tiebreak': 'tiebreak_display'})
    df['score_display'] = df['score_display'].fillna('--')
    df['points'] = df['rank'].astype(float)

    scores = [
        Score(**row.dropna())
        for _, row in df.iterrows()
    ]
    scores_json = "\n".join([s.model_dump_json() for s in scores])
    dump_data(scores_json,f"manual/parsed/{comp_id}/scores.ndjson",gcp)
    return

def parse_txt(txt,offset=0):    
    athletes = []
    scores = []
    lines = txt.split('\n')

    for line in lines:
        line = line.strip()
        ssplit = line.split(' ')
        if len(ssplit) == 3:
            overall_rank = ssplit[0]
            first_name = ssplit[1]
            last_name = ssplit[2]
            name = unidecode(first_name + ' ' + last_name)
            athletes.append(
                {
                    'overall_rank': int(overall_rank), 
                    'name_lower': name.lower().replace('*','')
                }
            )
        else:
            tsplit = line.split('\t')
            overall_points = int(tsplit[0])
            if len(tsplit) == 1:
                scores.append(
                    {
                        'overall_points': overall_points,
                        'scores': {}
                    }
                )
            else:
                w = tsplit[1:]
                scores.append(
                    {
                        'overall_points': overall_points,
                        'scores': {i+1+offset: w[i] for i in range(len(w))}
                    }
                )
    
    assert len(athletes) == len(scores), 'Athletes and scores are not same length'

    out = [
        {**a, **s} for a, s in zip(athletes, scores)
    ]

    df = pd.DataFrame(out)\
        .set_index(['overall_rank','name_lower','overall_points'])['scores']\
            .apply(pd.Series)

    return df

def fetch_capturefit_leaderboard(
    event: str,
    entry_type: str,
    category: str,
    gender: str,
):
    url = "https://capturefit.com/api/leaderboards/get-calulated-leaderboard"
    payload = {
        "event": event,
        "entrytype": entry_type, 
        "category": category,
        "gender": gender,
    }
    r = requests.post(url, data=payload)
    r.raise_for_status()
    data = r.json()
    return data

def parse_fict2019_leaderboard(gcp=False,refresh=False):
    comp_id = 'fict2019'
    lb = []
    for gender in ['male','female']:
        file = f'manual/raw/{comp_id}_leaderboard_{gender}.json'
        if not file_exists(file,gcp=gcp) or refresh:
            event = "5bdc47726e82987c55448ab7"
            entry_type = "Individual"
            category = "RX"
            data = fetch_capturefit_leaderboard(
                event=event,
                entry_type=entry_type,
                category=category,
                gender=gender.capitalize()
            )
            data_json = json.dumps(data)
            dump_data(data_json,file,gcp=gcp)
        else:
            data_json = load_data(file,gcp=gcp)
            data = json.loads(data_json)
        lb.extend(data['leaderboard'])

    lb = pd.DataFrame(lb).assign(source_comp_id=comp_id)
    lb['gender'] = lb['gender'].str.slice(0,1)

    lb['overall_rank'] = pd.to_numeric(lb['position'],errors='coerce')
    lb['overall_points'] = pd.to_numeric(lb['total'],errors='coerce')
    lb['display_name'] = lb['name']

    lb['source_athlete_id'] = lb['source_comp_id'] + '_' + \
        lb['gender'] + '_' + \
        lb['display_name'].rank().astype(int).astype(str)
    lb['dq'] = False

    entrants = [
        Entrant(**row.dropna())
        for _, row in lb.iterrows()
    ]
    entrants_json = "\n".join([e.model_dump_json() for e in entrants])
    dump_data(entrants_json,f"manual/parsed/{comp_id}/entrants.ndjson",gcp)

    scores_df = lb[['gender','source_athlete_id','scores']]\
        .explode('scores').reset_index(drop=True)

    scores_df = pd.merge(
        scores_df.drop(columns=['scores']),
        scores_df['scores'].apply(pd.Series),
        left_index=True,
        right_index=True,
        how='left'
    ).assign(source_comp_id=comp_id)

    scores_df['source_workout_id'] = scores_df['workoutnumber'].astype(int).astype(str)
    scores_df['score_display'] = scores_df['time'].fillna('--')
    scores_df['rank'] = pd.to_numeric(scores_df['position'],errors='coerce')
    scores_df['tiebreak_display'] = scores_df['tiebreaker'].apply(
        lambda x: str(int(x)) if not pd.isna(x) else x
    )

    scores = [
        Score(**row.dropna())
        for _, row in scores_df.iterrows()
    ]
    scores_json = "\n".join([s.model_dump_json() for s in scores])
    dump_data(scores_json,f"manual/parsed/{comp_id}/scores.ndjson",gcp)
    return

def fetch_sanctional_leaderboard(
    year: int,
    sanctional: int,
    division: int
):
    base_url = "https://c3po.crossfit.com/api/leaderboards/v2/competitions"
    url = f"{base_url}/sanctionals/{year}/leaderboards"

    params = {
        'sanctional': sanctional,
        'division': division
    }

    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json()
    return data

def parse_isd2019_leaderboard(gcp=False,refresh=False):
    comp_id = 'isd2019'
    lb = []
    for d, gender in enumerate(['male','female']):
        file_path = f"manual/raw/{comp_id}/{comp_id}_{gender}.json"
        if not file_exists(file_path,gcp) or refresh:
            data = fetch_sanctional_leaderboard(2019, 43, d+1)
            data_json = json.dumps(data)
            dump_data(data_json,file_path,gcp)
        else:
            data_json = load_data(file_path,gcp)
            data = json.loads(data_json)
        lb.extend(data['leaderboardRows'])
    lb = pd.DataFrame(lb)

    entrant_df = pd.merge(
        lb[['overallRank','overallScore','scores']],
        lb['entrant'].apply(pd.Series),
        left_index=True, right_index=True
    ).rename(columns={
        'competitorName':'display_name',
        'overallRank':'overall_rank',
        'overallScore':'overall_score'
    }).assign(source_comp_id=comp_id,dq=False)
    entrant_df['source_athlete_id'] = entrant_df['source_comp_id'] + '_' + \
        entrant_df['gender'] + '_' + \
        entrant_df['display_name'].rank().astype(int).astype(str)

    entrants = [
        Entrant(**row.dropna())
        for _, row in entrant_df.iterrows()
    ]
    entrants_json = "\n".join([e.model_dump_json() for e in entrants])
    dump_data(entrants_json,f"manual/parsed/{comp_id}/entrants.ndjson",gcp)

    scores_df = entrant_df[['source_athlete_id','gender','scores']]\
        .explode('scores').reset_index(drop=True)
    scores_df = pd.merge(
        scores_df.drop(columns=['scores']),
        scores_df['scores'].apply(pd.Series),
        left_index=True,
        right_index=True,
        how='left'
    ).assign(source_comp_id=comp_id)\
        .rename(columns={'scoreDisplay':'score_display',
        'ordinal':'source_workout_id'})
    scores_df['source_workout_id'] = scores_df['source_workout_id'].astype(str)

    scores = [
        Score(**row.dropna())
        for _, row in scores_df.iterrows()
    ]
    scores_json = "\n".join([s.model_dump_json() for s in scores])
    dump_data(scores_json,f"manual/parsed/{comp_id}/scores.ndjson",gcp)
    return

def fetch_rcc2019_leaderboard(
    gender: str,
    n: int = 0
):
    assert n <= 10, "n must be less than or equal to 10"

    from api import WodcastAPIRequestClient
    client = WodcastAPIRequestClient()

    event_id = 2813
    gender = 'MENS' if gender[0].upper() == 'M' else 'WOMENS'
    if n == 0:
        data = client.get_overall_results_page(
            event_id=event_id,
            gender=gender,
        )
    else:
        data = client.get_workout_results_page(
            event_id=event_id,
            gender=gender,
            event_number=n,
        )
    return data

def parse_rcc2019_leaderboard(gcp=False,refresh=False):
    comp_id = 'rcc2019'
    entrants_data = []
    scores_data = []
    for gender in ['M','F']:
        for n in range(11):
            file_path = f'manual/raw/{comp_id}]/{comp_id}_{gender}_{n}.json'
            if not file_exists(file_path) or refresh:
                data = fetch_rcc2019_leaderboard(gender, n)
                data_json = json.dumps(data)
                dump_data(data_json, file_path, gcp=gcp)
            else:
                data_json = load_data(file_path, gcp=gcp)
                data = json.loads(data_json)
            if n == 0:
                entrants_data.extend(data['athletes'])
            else:
                scores_data.extend([
                    {'source_workout_id': n, **row}
                    for row in data['athletes']
                ])

    entrants_df = pd.DataFrame(entrants_data).drop_duplicates()\
        .assign(source_comp_id=comp_id,dq=False)\
            .rename(columns={'rank':'overall_rank','result':'overall_points'})
    entrants_df['gender'] = entrants_df['gender'].str.upper()
    entrants_df['display_name'] = entrants_df[['first_name','last_name']].apply(' '.join, axis=1)
    entrants_df['source_athlete_id'] = entrants_df['source_comp_id'] + '_' + \
        entrants_df['gender'] + '_' + \
        entrants_df['display_name'].rank().astype(int).astype(str)

    entrants = [
        Entrant(**row.dropna())
        for _, row in entrants_df.iterrows()
    ]
    entrants_json = "\n".join([e.model_dump_json() for e in entrants])
    dump_data(entrants_json,f"manual/parsed/{comp_id}/entrants.ndjson",gcp)

    scores_df = pd.DataFrame(scores_data).drop_duplicates()
    scores_df = pd.merge(
        scores_df,
        entrants_df[['source_athlete_id','id']],
        on='id',
        how='left'
    ).assign(source_comp_id=comp_id)
    scores_df['gender'] = scores_df['gender'].str.upper()
    scores_df['score_display'] = scores_df['result']
    scores_df['source_workout_id'] = scores_df['source_workout_id'].astype(str)

    scores = [
        Score(**row.dropna())
        for _, row in scores_df.iterrows()
    ]

    scores = solve_points_table(entrants,scores,gcp=gcp)

    scores_json = "\n".join([s.model_dump_json() for s in scores])
    dump_data(scores_json,f"manual/parsed/{comp_id}/scores.ndjson",gcp)
    return

def fetch_btwb_leaderboard_page(
    leaderboard_id: int,
    page: int = 0
):
    url = f'https://us-central1-btwb-thewire.cloudfunctions.net/ProdLeaderboardPages?leaderboard_id={leaderboard_id}&page={page}'
    r = requests.get(url)
    return r.json()

def fetch_btwb_config(
    division_id: int
):
    url = f'https://us-central1-btwb-thewire.cloudfunctions.net/ProdLeaderboards?division_id={division_id}'
    r = requests.get(url)
    return r.json()

RI_BTWB_QUALIFIER_DIVS = {
    'ri2019q': {'M': 3, 'F': 4},
    'ri2020q': {'M': 13, 'F': 14},
}

def parse_ri_btwb_qualifier_leaderboard(
    comp_id: str,
    gcp: bool = True,
    refresh: bool = False,
):
    if comp_id not in RI_BTWB_QUALIFIER_DIVS:
        raise ValueError(
            f'Unknown qualifier {comp_id!r}; '
            f'expected one of {sorted(RI_BTWB_QUALIFIER_DIVS)}'
        )
    lb_data = []
    event_map = {}
    for gender, div_id in RI_BTWB_QUALIFIER_DIVS[comp_id].items():
        file = f'manual/raw/{comp_id}/config_{div_id}.json'
        if not file_exists(file,gcp=gcp) or refresh:
            data = fetch_btwb_config(div_id)
            data_json = json.dumps(data)
            dump_data(data_json,file,gcp=gcp)
        else:
            data_json = load_data(file,gcp=gcp)
            data = json.loads(data_json)
        page_count = data['Pages']
        lb_id = data['LeaderboardId']
        event_map.update({
            event_id: i+1 for i,event_id in enumerate(json.loads(data['Config'])['event_ids'])
        })
        for page in range(page_count):
            file = f'manual/raw/{comp_id}/leaderboard_{lb_id}_{page}.json'
            if not file_exists(file,gcp=gcp) or refresh:
                data = fetch_btwb_leaderboard_page(lb_id,page)
                data_json = json.dumps(data)
                dump_data(data_json,file,gcp=gcp)
            else:
                data_json = load_data(file,gcp=gcp)
                data = json.loads(data_json)
            lb_data.extend(
                [{'gender':gender,**d} for d in data['Standings']]
            )

    lb = pd.DataFrame(lb_data).assign(source_comp_id=comp_id)
    lb['source_athlete_id'] = lb[['source_comp_id','AthleteId']]\
        .astype(str).agg('-'.join,axis=1)
    
    entrants_df = lb.rename(
        columns={'PlaceOrdinal':'overall_rank','DisqualifiedCount':'dq',
        'PlacePoints':'overall_points',
        'WithdrawnCount':'wd','FullName':'display_name',
        'Nationality':'nationality','Age':'age'
        }
    ).reindex(columns=[
        'source_comp_id','source_athlete_id','overall_rank','overall_points',
        'dq','wd','display_name','nationality','age','gender'
    ])
    entrants_df['dq'] = entrants_df['dq'].gt(0)
    entrants_df['wd'] = entrants_df['wd'].gt(0)
    
    scores_df = pd.merge(
        lb[['source_comp_id','gender','source_athlete_id']],
        lb['EventStandingsData'].apply(json.loads),
        left_index=True,
        right_index=True
    ).explode('EventStandingsData',ignore_index=True)
        
    scores_df = pd.merge(
        scores_df[['source_comp_id','source_athlete_id','gender']],
        scores_df['EventStandingsData'].apply(pd.Series),
        left_index=True,
        right_index=True
    ).rename(columns={'PlaceRank':'rank','PlacePoints':'points'})

    scores_df['score_raw'] = scores_df['ScoreRankingPhrase'].str.split('|').str[-1]

    scores_df[['score_display','tiebreak_display']] = scores_df['score_raw'].str.extract(
        r'^(?P<score_display>.*?)\s*\[(?P<tiebreak_display>[^\]]+)\]$'
    )

    scores_df['source_workout_id'] = scores_df['EventId'].map(event_map).astype(str)

    scores_df.loc[
        ~scores_df['score_raw'].str.contains(r'\d+'),
        'score_display'
    ] = scores_df['score_raw']

    scores_df = scores_df.reindex(columns=[
        'source_comp_id','source_athlete_id','source_workout_id','gender',
        'rank','points','score_display','tiebreak_display'])

    entrants = [
        Entrant(**row.dropna())
        for _, row in entrants_df.iterrows()
    ]
    entrants_json = "\n".join([e.model_dump_json() for e in entrants])
    dump_data(entrants_json,f"manual/parsed/{comp_id}/entrants.ndjson",gcp)

    scores = [
        Score(**row.dropna())
        for _, row in scores_df.iterrows()
    ]
    scores_json = "\n".join([s.model_dump_json() for s in scores])
    dump_data(scores_json,f"manual/parsed/{comp_id}/scores.ndjson",gcp)
    return

RI_SMT_STANDINGS = {
    'ri2019': {
        'data_path': 'smt-prod',
        'divs': {'M': '101', 'F': '201'},
    },
    'ri2020': {
        'data_path': 'smt-2020-prod',
        'divs': {'M': '101', 'F': '201'},
    },
}

def parse_ri_smt_leaderboard(
    comp_id: str,
    refresh: bool = False,
    gcp: bool = True,
):
    if comp_id not in RI_SMT_STANDINGS:
        raise ValueError(
            f'Unknown invitational {comp_id!r}; '
            f'expected one of {sorted(RI_SMT_STANDINGS)}'
        )
    config = RI_SMT_STANDINGS[comp_id]
    lb = []
    event_names = None
    for gender, div in config['divs'].items():
        blob = f'manual/raw/{comp_id}-{gender}.json'
        if not file_exists(blob,gcp) or refresh:
            url = (
                f"https://rogue.btwb.com/data/{config['data_path']}"
                f"/standings-200-{div}.json"
            )
            r = requests.get(url)
            r.raise_for_status()
            data = r.json()
            dump_data(json.dumps(data),blob,gcp)
        else:
            data = json.loads(load_data(blob,gcp))
        if event_names is None:
            event_names = data['events']
        lb.extend([{'gender': gender, **d} for d in data['competitors']])

    lb = pd.DataFrame(lb)
    lb['source_comp_id'] = comp_id
    lb['source_athlete_id'] = lb[['source_comp_id','id']].astype(str).agg('-'.join,axis=1)
    
    entrants_df = lb.rename(columns={
        'name':'display_name','country':'nationality',
        'rank':'overall_rank','total':'overall_points'
    })
    entrants_df['dq'] = entrants_df['status'].eq('DQ')
    entrants_df['wd'] = entrants_df['status'].eq('WD')

    entrants_df = entrants_df.reindex(columns=[
        'source_comp_id','gender','source_athlete_id','display_name','nationality',
        'overall_rank','overall_points','dq','wd'
    ])

    scores_df = pd.melt(
        lb,
        id_vars=['source_comp_id','source_athlete_id','gender'],
        value_vars=event_names,
        var_name='event',
        value_name='data',
    )

    scores_df = pd.merge(
        scores_df[['source_comp_id','source_athlete_id','gender']],
        scores_df['data'].apply(pd.Series),
        left_index=True,
        right_index=True
    ).rename(columns={'order':'source_workout_id','eventOverallRank':'rank'})

    scores_df[['score_display','tiebreak_display']] = scores_df['score'].str.extract(
        r'^(?P<score_display>.*?)\s*\[(?P<tiebreak_display>[^\]]+)\]$'
    )

    scores_df['score_display'] = scores_df['score_display'].fillna(scores_df['score'])
    scores_df['rank'] = scores_df['rank'].astype(str).str.replace('T','',regex=False)
    scores_df['rank'] = pd.to_numeric(scores_df['rank'], errors='coerce')
    scores_df['points'] = pd.to_numeric(scores_df['points'], errors='coerce')

    scores_df = scores_df.reindex(columns=[
        'source_comp_id','source_athlete_id','gender','source_workout_id',
        'rank','points','score_display','tiebreak_display'
        ])

    entrants = [
        Entrant(**row.dropna())
        for _, row in entrants_df.iterrows()
    ]
    entrants_json = "\n".join([e.model_dump_json() for e in entrants])
    dump_data(entrants_json,f"manual/parsed/{comp_id}/entrants.ndjson",gcp)

    scores = [
        Score(**row.dropna())
        for _, row in scores_df.iterrows()
    ]

    scores_json = "\n".join([s.model_dump_json() for s in scores])
    dump_data(scores_json,f"manual/parsed/{comp_id}/scores.ndjson",gcp)
    return

def parse_metadata_all(gcp=False):
    inpath = 'manual/raw/manual-metadata-all.json'
    meta_all = json.loads(load_data(inpath,gcp=gcp))
    for m in meta_all:
        meta = Metadata(**m)
        outpath = f'manual/parsed/{meta.source_comp_id}/metadata.ndjson'
        meta_json = meta.model_dump_json()
        dump_data(meta_json,outpath,gcp=gcp)
    return

def parse_all(gcp=False):
    parse_metadata_all(gcp=gcp)
    try:
        parse_scc2019_leaderboard(gcp=gcp)
    except Exception as e:
        print(f'Error parsing SCC 2019: {e}')
    try:
        parse_lcq2025_leaderboard(gcp=gcp)
    except Exception as e:
        print(f'Error parsing LCQ 2025: {e}')
    for comp_id in RI_SMT_STANDINGS:
        try:
            parse_ri_smt_leaderboard(comp_id=comp_id, gcp=gcp)
        except Exception as e:
            print(f'Error parsing {comp_id}: {e}')
    try:
        parse_fict2019_leaderboard(gcp=gcp)
    except Exception as e:
        print(f'Error parsing Fict 2019: {e}')
    try:
        parse_isd2019_leaderboard(gcp=gcp)
    except Exception as e:
        print(f'Error parsing Italian Showdown 2019: {e}')
    try:
        parse_rcc2019_leaderboard(gcp=gcp)
    except Exception as e:
        print(f'Error parsing RCC 2019: {e}')
    for comp_id in RI_BTWB_QUALIFIER_DIVS:
        try:
            parse_ri_btwb_qualifier_leaderboard(comp_id=comp_id, gcp=gcp)
        except Exception as e:
            print(f'Error parsing {comp_id}: {e}')

if __name__ == '__main__':
    parse_metadata_all(gcp=True)
    # parse_scc2019_leaderboard(gcp=True)
    # parse_lcq2025_leaderboard(gcp=True)
    # parse_ri_smt_leaderboard('ri2019', gcp=True)
    # parse_fict2019_leaderboard(gcp=True)
    # parse_isd2019_leaderboard(gcp=True)
    # parse_rcc2019_leaderboard(gcp=True)
    parse_ri_btwb_qualifier_leaderboard('ri2020q', gcp=True)
    parse_ri_btwb_qualifier_leaderboard('ri2019q', gcp=True)
