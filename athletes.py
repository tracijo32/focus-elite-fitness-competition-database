import pandas as pd
from unidecode import unidecode
from inventory import BUCKET

REQUIRED_COLUMNS = [
    'global_athlete_id','name','gender','first_name',
    'last_name','cf_id','cc_id','si_id','str_id'
]
NAME_LIST_COLUMNS = ['first_name','last_name','nickname']
ALT_ID_LIST_COLUMNS = ['cf_id','si_id','cc_id','str_id']

#### 
## quality control and preprocessing
####

def preprocess_master(master: pd.DataFrame):
    df = master.copy()
    missing_cols = set(REQUIRED_COLUMNS) - set(df.columns)
    assert len(missing_cols) == 0, \
        f"Missing required columns: {','.join(missing_cols)}"

    df = df.reindex(columns=REQUIRED_COLUMNS)\
        .sort_values(by='global_athlete_id')\
            .reset_index(drop=True)

    ## these fields are required for all rows
    req_fields = ['global_athlete_id','name','gender']
    f = df[req_fields].notnull().all()
    assert f.all(), f"Missing required fields: {','.join(f[f.eq(False)].index)}"
    df['global_athlete_id'] = df['global_athlete_id'].astype(int)
    df['name'] = df['name'].astype(str)
    df['gender'] = df['gender'].astype(str)

    ## each of these columns should be a list of strings
    ## let's make sure the strings are all stripped of whitespace, 
    ## don't have duplicates, and are sorted
    ## anything that isn't a list is None
    list_cols = NAME_LIST_COLUMNS + ALT_ID_LIST_COLUMNS
    for col in list_cols:
        df[col] = df[col].apply(
            lambda x: sorted(list({str(s).strip() for s in x}))
            if isinstance(x, list) else None
        )

    ## there must be at least one first name and one last name for each row
    assert df[['first_name','last_name']]\
    .apply(lambda x: len(x) > 0 if x is not None else False)\
        .all(), "Missing first or last name for some rows"
    
    ## unfortunately, we are restricted to the gender binary 
    assert df['gender'].isin(['M','F']).all(), "Invalid gender"

    ## we can't have any duplicated global_athlete_ids
    assert not df['global_athlete_id'].duplicated().any(), \
        "Duplicated global_athlete_id"

    ## each alternate id should be unique to one global_athlete_id
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
    
def check_duplicate_ids_on_name(
    master_pp: pd.DataFrame
):
    nm_var = get_name_variants_frame(master_pp)
    sf_jn = pd.merge(
        nm_var, nm_var,
        on=['gender','name_variant']
    )
    dups = sf_jn['global_athlete_id_x'].eq(sf_jn['global_athlete_id_y'])
    assert not dups.any(), f"Duplicate ids: {sf_jn[dups]['global_athlete_id_x'].tolist()}"
    return True

#####
## appending and merging
####
def add_list_value_to_master(
    master: pd.DataFrame,
    global_athlete_id: str | int,
    column: str,
    value: str
):
    assert column in ['first_name','last_name','si_id', 'cc_id', 'str_id'], \
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

