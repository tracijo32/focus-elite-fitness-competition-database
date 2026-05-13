import json, requests
import pandas as pd
from google.cloud import storage
from parameters import GoogleCloudParameters
from unidecode import unidecode
from models import ManualEntrant, ManualScore, ManualCompetition

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

def parse_southfit_leaderboard(gcp=False):
    lb_json = load_data('manual/raw/scc2019_leaderboard.json',gcp=gcp)
    lb = pd.DataFrame(json.loads(lb_json))
    lb['name'] = fix_name_column(lb['name'])
    lb['name_lower'] = lb['name'].str.lower()

    athletes = get_athlete_names(gcp=gcp)
    athletes['name_lower'] = athletes['name'].str.lower()

    lb = pd.merge(
        lb,
        athletes.drop(columns=['name']),
        on=['gender','name_lower'],how='left')\
        .assign(comp_id='scc2019')

    assert lb.groupby('name')['athlete_id'].nunique().eq(1).all(), \
        "SCC 2019: Some athletes have multiple ids"

    entrants = [
        ManualEntrant(**row.dropna())
        for _, row in lb.iterrows()
    ]
    return entrants, []

def parse_hustleup_lcq_leaderboard(
    gcp=False
):
    male_json = load_data('manual/raw/lcq2025_leaderboard_male.json',gcp=gcp)
    df_m = pd.DataFrame(json.loads(male_json)).assign(gender='M')
    female_json = load_data('manual/raw/lcq2025_leaderboard_female.json',gcp=gcp)
    df_f = pd.DataFrame(json.loads(female_json)).assign(gender='F')

    df = pd.concat([df_m,df_f])
    df['name'] = fix_name_column(df['name'])
    df['name_lower'] = df['name'].str.lower()

    athletes = get_athlete_names(gcp=gcp)
    athletes['name_lower'] = athletes['name'].str.lower()
    df = pd.merge(
        df,
        athletes.drop(columns=['name']),
        on=['gender','name_lower'],how='left')\
        .assign(comp_id='lcq2025')\
            .rename(columns={'total_points': 'overall_points'})

    assert df.groupby('name')['athlete_id'].nunique().eq(1).all(), \
        "LCQ 2025: Some athletes have multiple ids"

    entrants = [
        ManualEntrant(**row.dropna())
        for _, row in df.iterrows()
    ]

    df['scores'] = df['scores'].apply(
        lambda x: [{'ordinal': k, **v} for k,v in x.items()]
    )

    df = df[['athlete_id','scores','comp_id','gender']]\
        .explode('scores').reset_index(drop=True)
    df = pd.merge(
        df.drop(columns=['scores']),
        df['scores'].apply(pd.Series),
        left_index=True,
        right_index=True
    )

    scores = [
        ManualScore(**row.dropna())
        for _, row in df.iterrows()
    ]

    return entrants, scores

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

def parse_rogue19_leaderboard(gcp=False):
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
        .assign(comp_id=comp_id)
    entrants = [
        ManualEntrant(**row.dropna())
        for _, row in idx.iterrows()
    ]

    scores_df = pd.melt(
        lb.reset_index(),
        id_vars=['name_lower','gender'],
        value_vars=lb.columns,
        var_name='ordinal',
        value_name='score',
    )
    scores_df = pd.merge(
        scores_df,
        idx,
        on=['name_lower','gender'],
        how='left'
    ).reindex(columns=[
        'athlete_id','gender','ordinal','score'
    ])

    assert scores_df['athlete_id'].notna().all(), \
        'Some athletes are not in the athlete master'

    scores_df['capped'] = scores_df['score'].str.contains('CAP',case=False)\
        .astype(int)
    scores_df['no_score'] = scores_df['score'].isna()

    v = scores_df['score'].str.split(':',expand=True,n=2)
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
            by=['gender','ordinal','no_score','capped','v1','v2'],
            ascending=True
        ).assign(comp_id='ri2019')

    scores_df['rank'] = scores_df.groupby(['gender','ordinal']).cumcount() + 1

    scores = [
        ManualScore(**row.dropna())
        for _, row in scores_df.iterrows()
    ]

    return entrants, scores

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