import pandas as pd
from google.cloud import storage
import json
from unidecode import unidecode
from parameters import GoogleCloudParameters

gcp_params = GoogleCloudParameters()
PROJECT_ID = gcp_params.project_id
BUCKET_NAME = gcp_params.bucket_name
BUCKET = storage.Client(project=PROJECT_ID).bucket(BUCKET_NAME)

def get_leaderboard_blobs(
    comp_id: int,
    div_id: int,
    comp_type: str,
    pg_hund: int
):
    match_glob_prefix = f'crossfit/api/comp={comp_id}/division={div_id}'
    if comp_type == 'open':
        match_glob_prefix += '/scaled=0'

    if pg_hund > 0:
        match_glob = match_glob_prefix + f'/page={pg_hund}??.json'
        blob_list = list(BUCKET.list_blobs(
            match_glob=match_glob
        ))
    else:
        match_glob_1 = match_glob_prefix + f'/page=?.json'
        blob_list_1 = list( BUCKET.list_blobs(
            match_glob=match_glob_1
        ))
        match_glob_2 = match_glob_prefix + f'/page=??.json'
        blob_list_2 = list(BUCKET.list_blobs(
            match_glob=match_glob_2
        ))
        blob_list = blob_list_1 + blob_list_2
            
    return blob_list

def get_athletes_df(lb_data):
    df = pd.DataFrame(lb_data)

    df = pd.merge(
        df['competition'].apply(pd.Series)[['year','competitionType','competitionId','division']],
        df['leaderboardRows'],
        left_index=True,
        right_index=True
    ).rename(columns={
        'competitionType':'comp_type',
        'competitionId':'comp_id',
        'division': 'div_id'},
    ).explode('leaderboardRows',ignore_index=True)

    df = pd.merge(
        df.drop(columns=['leaderboardRows']),
        df['leaderboardRows'].dropna().apply(pd.Series),
        left_index=True,
        right_index=True
    ).rename(columns={
        'overallRank':'overall_rank',
        'overallScore':'overall_score'
    })

    df = pd.merge(
        df.drop(columns=['ui','entrant','scores']),
        df['entrant'].apply(pd.Series),
        left_index=True,
        right_index=True
    ).rename(
        columns={
            'competitorId': 'cf_id',
            'competitorName': 'name',
            'firstName': 'first_name',
            'lastName': 'last_name',
            'profilePicS3key': 'profile_pic',
            'countryOfOriginCode': 'country_code',
            'countryOfOriginName': 'country_name',
            'regionId': 'region_id',
            'regionName':'region_name',
            'affiliateId': 'affiliate_id',
            'affiliateName': 'affiliate_name'
        }
    ).reindex(
        columns=[
            'cf_id','name','year','comp_type','comp_id','div_id',
            'overall_rank','overall_score','first_name','last_name',
            'gender','age','height','weight',
            'country_name','region_name','affiliate_name','profile_pic'
        ]
    ).astype(str).astype({'cf_id':int,'year':int,'comp_id':int,'div_id':int})
    df['name_clean'] = df['name'].str.replace(r'\s+', ' ', regex=True)\
                        .str.strip().str.lower().apply(unidecode)

    return df

def build_blob_name(
    comp_type: int,
    div_id: int,
    year: int,
    comp_id: int,
    pg_hund: int
):
    if div_id == 1:
        gender = 'M'
    elif div_id == 2:
        gender = 'F'
    else:
        raise ValueError(f'Invalid division ID: {div_id}')
        
    path = [
        'crossfit/athletes',
        f'gender={gender}',
        f'comp_type={comp_type}',
        f'year={year}',
        f'comp={comp_id}_{pg_hund}.parquet'
    ]
    return '/'.join(path)

if __name__ == '__main__':
    blob = BUCKET.blob('crossfit/index.json')
    index = json.loads(blob.download_as_string())

    kwargs_list = [
        {
            **idx,
            'div_id': d
        } for idx in index for d in [1,2]
    ]

    ### skipping a bunch of comps that are already parsed
    kwargs_list = kwargs_list[260:]
    ###

    for i, kwargs in enumerate(kwargs_list):
        print(f'Processing {i+1} of {len(kwargs_list)}: {kwargs["comp_id"]} {kwargs["div_id"]} {kwargs["comp_type"]} {kwargs["year"]}')
        comp_id = kwargs['comp_id']
        div_id = kwargs['div_id']
        comp_type = kwargs['comp_type']
        year = kwargs['year']
        
        pg_hund = 0
        while True:
            s = max(pg_hund*100, 1)
            e = (pg_hund+1)*100 - 1
            print(f'\tProcessing leaderboard pages {s} to {e}')

            blob_name = build_blob_name(
                comp_type=comp_type,
                div_id=div_id,
                year=year,
                comp_id=comp_id,
                pg_hund=pg_hund
            )
            if BUCKET.blob(blob_name).exists():
                print(f'\t\t...already exists, skipping...')
                pg_hund += 1
                continue

            blob_list = get_leaderboard_blobs(
                comp_id=comp_id,
                div_id=div_id,
                comp_type=comp_type,
                pg_hund=pg_hund
            )
            if len(blob_list) == 0:
                print(f'\t\t...no more pages, continuing...')
                break
            print(f'\t\t..converting to parquet...')
            lb_data = [
                json.loads(blob.download_as_string())
                for blob in blob_list
            ]
            df = get_athletes_df(lb_data)
            df.to_parquet(
                'gs://' + BUCKET_NAME + '/' + blob_name
            )
            print(f'\t\t\t...done')
            pg_hund += 1

    print('Done')