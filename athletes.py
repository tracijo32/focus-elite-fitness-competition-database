import pandas as pd
from unidecode import unidecode
from inventory import BUCKET
import json


NAME_LIST_COLUMNS = ['first_name','last_name','nickname']
ALT_ID_LIST_COLUMNS = ['cf_id','si_id','cc_id','str_id','mn_id']
STRING_COLUMNS = ['global_athlete_id','name','gender']

REQUIRED_COLUMNS = STRING_COLUMNS + ALT_ID_LIST_COLUMNS + NAME_LIST_COLUMNS

def format_and_validate(df: pd.DataFrame):
    df = df.reindex(columns=REQUIRED_COLUMNS)
    df['global_athlete_id'] = df['global_athlete_id'].astype(int)
    df['name'] = df['name'].astype(str)
    df['gender'] = df['gender'].astype(str)

    ## check that there are no missing values
    assert df.notnull().all().all(), "Missing required fields"

    ## check that each column is a list
    for col in NAME_LIST_COLUMNS + ALT_ID_LIST_COLUMNS:
        assert df[col].apply(lambda x: isinstance(x, list)).all(), \
            f"Column {col} is not a list"

    ## make sure there is at least one first name and one last name for each row
    assert df[['first_name','last_name']].apply(lambda x: len(x) > 0).all(), \
        "Missing first or last name for some rows"


    ## make sure there are no duplicates strings in the list
    for col in NAME_LIST_COLUMNS + ALT_ID_LIST_COLUMNS:
        df[col] = df[col].apply(
            lambda x: list(sorted(list(set(x))))
        )
 
    ## check that there are no duplicate ids
    assert not df.duplicated(subset=['global_athlete_id']).any(),\
         "Duplicate global_athlete_id"
    
    ## check that each alternate id is unique to one global_athlete_id
    for col in ALT_ID_LIST_COLUMNS:
        alt = df[['global_athlete_id',col]]\
            .explode(col,ignore_index=True)\
            .dropna()
        n_alt = alt.groupby(col)['global_athlete_id'].nunique()
        assert n_alt.eq(1).all(), \
            f"Duplicated {col}: {n_alt[n_alt.gt(1)].index.tolist()}"

    return df

def get_name_variants_frame(
    master_pp: pd.DataFrame, lower=True
):
    ## get the name variants for each athlete
    ## basically cross product of all combinations of 
    ## options first and last name
    df = master_pp.reindex(columns=[
        'global_athlete_id','gender','first_name','last_name'
    ])
    df['name_variant'] = df.apply(
        lambda x: [f'{f} {l}' for f in x['first_name'] for l in x['last_name']],
        axis=1
    )
    df = df.explode('name_variant')
    df = df.drop(columns=['first_name','last_name'])
    if lower:
        df['name_variant'] = df['name_variant'].str.lower()
    return df.reindex(columns=['global_athlete_id','gender','name_variant'])
    
def get_duplicate_ids_on_name(
    master_pp: pd.DataFrame
):
    nm_var = get_name_variants_frame(master_pp)
    sf_jn = pd.merge(
        nm_var, nm_var,
        on=['gender','name_variant']
    )
    dups = sf_jn['global_athlete_id_x'].gt(sf_jn['global_athlete_id_y'])
    if dups.any():
        return sf_jn[dups]
    return

#####
## appending and merging
####
def add_list_value_to_master(
    master: pd.DataFrame,
    global_athlete_id: str | int,
    column: str,
    value: str
):
    assert column in ALT_ID_LIST_COLUMNS + NAME_LIST_COLUMNS, \
        f"Invalid column name: {column}"

    assert column in master.columns, f"Column {column} not in master"
    
    row = master[
        master['global_athlete_id'].eq(int(global_athlete_id))
    ]
    assert len(row) == 1, f"Expected 1 row, got {len(row)}"

    idx = row.index[0]
    x = row.to_dict(orient='records')[0]
    if x[column] is None:
        x[column] = [value]
    else:
        x[column].append(value)
        x[column] = list(sorted(list(set(x[column]))))

    master.loc[idx] = pd.Series(x)
    return master

def format_for_master(df: pd.DataFrame):
    df = df.reindex(columns=REQUIRED_COLUMNS)
    for col in ALT_ID_LIST_COLUMNS + NAME_LIST_COLUMNS:
        df[col] = df[col].apply(
            lambda x: x if isinstance(x,list) else 
            str(x).split(',') if isinstance(x,int) or isinstance(x,str)
            else [])
    return df

## IO functions
def load_master():
    blob = BUCKET.blob('consolidated/athletes_master.ndjson')
    string_data = blob.download_as_string().decode('utf-8')
    data = [json.loads(line) for line in string_data.split('\n') if line]
    df = pd.DataFrame(data)
    df = format_and_validate(df)
    return df

def upload_master(master: pd.DataFrame):
    m = format_and_validate(master)
    m_json = m.to_json(orient='records', lines=True)
    blob = BUCKET.blob('consolidated/athletes_master.ndjson')
    blob.upload_from_string(m_json, content_type='application/ndjson')

def backup_master():
    ts = int(pd.Timestamp.now().timestamp())
    old_blob = BUCKET.blob(f'consolidated/athletes_master.ndjson')
    new_blob = BUCKET.blob(f'consolidated/athletes_master_{ts}.ndjson')
    new_blob.upload_from_string(
        old_blob.download_as_string().decode('utf-8'), 
    content_type='application/ndjson')