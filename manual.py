import json, requests
import pandas as pd
from google.cloud import storage
from parameters import GoogleCloudParameters
from unidecode import unidecode
from models import Entrant, Score, Metadata

gcp_params = GoogleCloudParameters()
storage_client = storage.Client(project=gcp_params.project_id)
bucket = storage_client.bucket(gcp_params.bucket_name)

def fix_name_column(col: pd.Series):
    col = col.apply(unidecode)\
        .str.replace(r'\s+', ' ',regex=True)\
            .str.strip()
    return col

def get_athlete_names(gcp=False):
    file_path = 'consolidated/athletes_master.json'
    data_json = load_data(file_path,gcp=gcp)
    df = pd.DataFrame(json.loads(data_json))\
        [['athlete_id','first_name','last_name','gender']]
    df['name'] = df.apply(
        lambda x: [f'{f} {l}' for f in x['first_name'] for l in x['last_name']],
        axis=1
    )
    df = df.explode('name').drop(columns=['first_name','last_name'])
    return df

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

def parse_scc2019_leaderboard(gcp=False):
    comp_id = 'scc2019'
    lb_json = load_data(f'manual/raw/{comp_id}/{comp_id}_leaderboard.json',gcp=gcp)
    lb = pd.DataFrame(json.loads(lb_json))
    lb['display_name'] = lb['name']
    lb['name'] = fix_name_column(lb['name'])
    lb['name_lower'] = lb['name'].str.lower()

    athletes = get_athlete_names(gcp=gcp)
    athletes['name_lower'] = athletes['name'].str.lower()

    lb = pd.merge(
        lb,
        athletes.drop(columns=['name']),
        on=['gender','name_lower'],how='left')\
        .assign(source_comp_id='scc2019')
    lb['source_athlete_id'] = lb['athlete_id'].astype(str)

    assert lb.groupby('name')['athlete_id'].nunique().eq(1).all(), \
        "SCC 2019: Some athletes have multiple ids"

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

    df = pd.concat(dfs)
    df['display_name'] = df['name']
    df['name'] = fix_name_column(df['name'])
    df['name_lower'] = df['name'].str.lower()

    athletes = get_athlete_names(gcp=gcp)
    athletes['name_lower'] = athletes['name'].str.lower()
    df = pd.merge(
        df,
        athletes.drop(columns=['name']),
        on=['gender','name_lower'],how='left')\
        .assign(source_comp_id=comp_id)\
            .rename(columns={'total_points': 'overall_points'})
    df['source_athlete_id'] = df['athlete_id'].astype(str)

    assert df.groupby('name')['athlete_id'].nunique().eq(1).all(), \
        "LCQ 2025: Some athletes have multiple ids"

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

def parse_ri2019_leaderboard(gcp=False):
    comp_id = 'ri2019'
    lb = []
    for gender in ['male','female']:
        pgs = []
        for i in range(2):
            file = f'manual/raw/{comp_id}_{gender}_{i+1}.txt'
            txt = load_data(file,gcp=False)
            pg = parse_txt(txt,offset=4*i).assign(gender=gender[0].upper())\
                .set_index('gender',append=True)
            pgs.append(pg)
        full = pd.merge(pgs[0],pgs[1],left_index=True,right_index=True)
        lb.append(full)        
            
    lb = pd.concat(lb)

    idx = lb.index.to_frame().reset_index(drop=True)
    athletes = get_athlete_names(gcp=False)
    athletes['name_lower'] = athletes['name'].str.lower()

    idx = pd.merge(idx,athletes,on=['gender','name_lower'],how='left')\
        .assign(source_comp_id=comp_id).rename(columns={'name': 'display_name'})
    idx['source_athlete_id'] = idx['athlete_id'].astype(str)
    entrants = [
        Entrant(**row.dropna())
        for _, row in idx.iterrows()
    ]
    entrants_json = "\n".join([e.model_dump_json() for e in entrants])
    dump_data(entrants_json,f"manual/parsed/{comp_id}/entrants.ndjson",gcp)

    scores_df = pd.melt(
        lb.reset_index(),
        id_vars=['name_lower','gender'],
        value_vars=lb.columns,
        var_name='source_workout_id',
        value_name='score_display',
    )
    scores_df = pd.merge(
        scores_df,
        idx,
        on=['name_lower','gender'],
        how='left'
    ).reindex(columns=[
        'source_athlete_id','gender','source_workout_id','score_display'
    ])

    assert scores_df['source_athlete_id'].notna().all(), \
        'Some athletes are not in the athlete master'

    scores_df['capped'] = scores_df['score_display'].str.contains('CAP',case=False)\
        .astype(int)
    scores_df['no_score'] = scores_df['score_display'].isna()

    v = scores_df['score_display'].str.split(':',expand=True,n=2)
    v.columns = ['v1','v2']
    v['v1'] = v['v1'].str.extract(r'(\d+)').astype(float)
    v['v2'] = v['v2'].astype(float)

    scores_df = pd.merge(
        scores_df,
        v,
        left_index=True,
        right_index=True
    )

    scores_df = scores_df\
        .sort_values(
            by=['gender','source_workout_id','no_score','capped','v1','v2'],
            ascending=True
        ).assign(source_comp_id='ri2019')

    scores_df['rank'] = scores_df.groupby(['gender','source_workout_id']).cumcount() + 1
    scores_df['source_workout_id'] = scores_df['source_workout_id'].astype(str)
    scores_df['score_display'] = scores_df['score_display'].fillna('--').astype(str)

    scores = [
        Score(**row.dropna())
        for _, row in scores_df.iterrows()
    ]
    scores_json = "\n".join([s.model_dump_json() for s in scores])
    dump_data(scores_json,f"manual/parsed/{comp_id}/scores.ndjson",gcp)

    return

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

    lb = pd.DataFrame(lb)
    lb['gender'] = lb['gender'].str.slice(0,1)

    lb['overall_rank'] = pd.to_numeric(lb['position'],errors='coerce')
    lb['overall_points'] = pd.to_numeric(lb['total'],errors='coerce')
    lb['display_name'] = lb['name']
    lb['name'] = fix_name_column(lb['name'])
    lb['name_lower'] = lb['name'].str.lower()\
        .str.replace('mr.','').str.strip()

    athletes = get_athlete_names()
    athletes['name_lower'] = athletes['name'].str.lower()

    lb = pd.merge(
        lb.drop(columns=['name']),
        athletes,
        on=['gender','name_lower'],
        how='left'
    ).assign(source_comp_id=comp_id)
    lb['source_athlete_id'] = lb['athlete_id'].astype(str)

    assert not lb['athlete_id'].isna().any()

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
    })
    entrant_df['name'] = fix_name_column(entrant_df['display_name'])
    entrant_df['name_lower'] = entrant_df['name'].str.lower()\
        .replace(
            {
                'ballo oliver': 'oliver ballo',
                'yundov nikita': 'nikita yundov',
                'christoph korner': 'christoph koerner',
                'jonaa muller': 'jonas muller'
            }
        )

    athletes = get_athlete_names()
    athletes['name_lower'] = athletes['name'].str.lower()

    entrant_df = pd.merge(
        entrant_df.drop(columns=['name']),
        athletes,
        on=['gender','name_lower'],
        how='left'
    ).assign(source_comp_id=comp_id)
    entrant_df['source_athlete_id'] = entrant_df['athlete_id'].astype(str)

    assert entrant_df['athlete_id'].isna().sum() == 0
    assert entrant_df.groupby('name_lower')['athlete_id'].nunique().max() == 1

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
    entrants_df = []
    scores_df = []
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
                entrants_df.extend(data['athletes'])
            else:
                scores_df.extend([
                    {'source_workout_id': n, **row}
                    for row in data['athletes']
                ])
    entrants_df = pd.DataFrame(entrants_df)\
        .drop_duplicates().assign(source_comp_id=comp_id)
    entrants_df['gender'] = entrants_df['gender'].str.upper()
    entrants_df['display_name'] = entrants_df[['first_name','last_name']].apply(' '.join, axis=1)
    entrants_df['name_lower'] = entrants_df['display_name'].str.lower().str.strip()
    entrants_df['name_lower'] = fix_name_column(entrants_df['name_lower'])\
        .replace({'paul trembley':'paul tremblay','lee tanner':'tanner lee'})
    scores_df = pd.DataFrame(scores_df).drop_duplicates()

    athletes = get_athlete_names(gcp=gcp)
    athletes['name_lower'] = athletes['name'].str.lower()

    entrants_df = pd.merge(
        entrants_df,
        athletes,
        on=['gender','name_lower'],
        how='left'
    ).rename(
        columns={
            'rank': 'overall_rank',
            'result': 'overall_points'
        }
    )

    assert not entrants_df['athlete_id'].isnull().any()
    assert entrants_df.groupby('name')['athlete_id'].nunique().eq(1).all()
    entrants_df['source_athlete_id'] = entrants_df['athlete_id'].astype(str)

    entrants = [
        Entrant(**row.dropna())
        for _, row in entrants_df.iterrows()
    ]
    entrants_json = "\n".join([e.model_dump_json() for e in entrants])
    dump_data(entrants_json,f"manual/parsed/{comp_id}/entrants.ndjson",gcp)

    scores_df = pd.merge(
        scores_df,
        entrants_df[['source_athlete_id','id']],
        on='id',
        how='left'
    ).assign(source_comp_id=comp_id)
    scores_df['gender'] = scores_df['gender'].str.upper()
    scores_df['score_display'] = scores_df['result']
    scores_df['source_workout_id'] = scores_df['source_workout_id'].astype(str)

    assert not scores_df['source_athlete_id'].isnull().any()

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
    try:
        parse_ri2019_leaderboard(gcp=gcp)
    except Exception as e:
        print(f'Error parsing RI 2019: {e}')
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

if __name__ == '__main__':
    parse_all(gcp=True)
