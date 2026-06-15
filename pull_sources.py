import parse
from sql import CLIENT
from tqdm.auto import tqdm
import pandas as pd

def parse_standard_competition(
    parser: parse.Parser,
    comp_id: str, 
    division_male: str, 
    division_female: str,
    refresh: bool = False
):
    results = {'source': parser.manager.source, 'source_comp_id': comp_id}
    if 'parse_metadata' in dir(parser):
        try:
            parser.parse_metadata(
                comp_id=comp_id,
                refresh=refresh
            )
            results['metadata'] = 1
        except Exception as e:
            results['metadata'] = -1
    else:
        results['metadata'] = 0

    if 'parse_workouts' in dir(parser):
        try:
            parser.parse_workouts(
                comp_id=comp_id,
                division_male=division_male,
                division_female=division_female,
                refresh=refresh
            )
            results['workouts'] = 1
        except Exception as e:
            results['workouts'] = -1
    else:
        results['workouts'] = 0

    if 'parse_leaderboard' in dir(parser):
        try:
            parser.parse_leaderboard(
                comp_id=comp_id,
                division_male=division_male,
                division_female=division_female,
                refresh=refresh
            )
            results['leaderboard'] = 1
        except Exception as e:
            results['leaderboard'] = -1
    else:
        results['leaderboard'] = 0
    return results

def pull_competition():
    query = f"""
    SELECT 
        s.global_comp_id, 
        s.source, 
        s.source_comp_id,
        s.division_male,
        s.division_female,
        s.priority,
        c.season,
        c.stage
    FROM `staging.sources` s
    LEFT JOIN `staging.crossfit_stages` c
    ON s.global_comp_id = c.global_comp_id
    AND s.source = 'crossfit' 
    AND SAFE_CAST(s.source_comp_id AS INTEGER) = SAFE_CAST(c.comp_id AS INTEGER)
    """
    return CLIENT.query(query).to_dataframe()
    
def parse_crossfit_leaderboard(
    parser: parse.CrossFitParser,
    **kwargs
):
    results = []
    try:
        parser.parse_leaderboard_page(**kwargs, page=1)
        total_pages = parser.get_total_pages(**kwargs)
        r = {**kwargs, 'page': 1, 'status': 1}
        results.append(r)
    except Exception as e:
        if '404' in str(e):
            r = {**kwargs, 'page': 1, 'status': -404}
        else:
            r = {**kwargs, 'page': 1, 'status': -1}
        results.append(r)
        return results

    page_iter = range(2, total_pages + 1)
    if total_pages >= 5:
        page_iter = tqdm(
            page_iter,
            total=total_pages,
            initial=1,
            desc=f"{kwargs['comp_id']} div {kwargs['div_id']}",
            leave=False,
        )

    for page in page_iter:
        try:
            parser.parse_leaderboard_page(**kwargs, page=page)
            r = {**kwargs, 'page': page, 'status': 1}
            results.append(r)
        except Exception as e:
            r = {**kwargs, 'page': page, 'status': -1}
            results.append(r)
    return results

def parse_all_competitions(
    comp_df: pd.DataFrame
):
    parsers = {
        'crossfit': parse.CrossFitParser(),
        'competition-corner': parse.CompetitionCornerParser(),
        'strongest': parse.StrongestParser(),
        'score-it': parse.ScoreItParser(),
    }

    req_cols_1 = ['source', 'comp_id', 'division_male', 'division_female']
    assert all(c in comp_df.columns for c in req_cols_1)
    assert comp_df[req_cols_1].notna().all().all(), 'Missing required columns'

    p = comp_df['source'].isin(parsers.keys())

    assert p.any(), 'No competitions are parseable'

    no_parse = comp_df[~p]
    if len(no_parse) > 0:
        print(f"Skipping {len(no_parse)} competitions that are not parseable")
        print(no_parse[['source', 'comp_id']])

    comp_df = comp_df[p].sort_values(by='source')
    unique_sources = comp_df['source'].unique()

    if 'crossfit' in unique_sources:
        req_cols_2 = ['year', 'comp_type']
        assert all(c in comp_df.columns for c in req_cols_2)
        assert comp_df.loc[comp_df['source'].eq('crossfit'), req_cols_2].notna().all().all(), \
            'Missing required columns'

    results = []
    for _, row in tqdm(comp_df.iterrows(), total=len(comp_df), desc='Parsing competitions'):
        source = row['source']
        parser = parsers[source]

        if source == 'crossfit':
            kwargs_list = pd.melt(
                pd.DataFrame([row]),
                id_vars=['comp_id', 'year', 'comp_type'],
                value_vars=['division_male', 'division_female'],
                value_name='div_id',
            ).drop(columns=['variable']).to_dict('records')

            for kwargs in kwargs_list:
                results.extend(parse_crossfit_leaderboard(parser, **kwargs))
        else:
            r = parse_standard_competition(
                parser,
                comp_id=row['comp_id'],
                division_male=row['division_male'],
                division_female=row['division_female'],
            )
            results.append(r)

    other_results = [
        res for res in results if 'page' not in res
    ]
    cf_results = pd.DataFrame([
        res for res in results if 'page' in res
    ])
    cf_results['details'] = cf_results[['div_id','page','status']].to_dict(orient='records')
    cf_results = cf_results.groupby('comp_id').agg(
        leaderboard = ('status','min'),
        details = ('details', list)
    ).reset_index().assign(source='crossfit',metadata=0,workouts=0)\
        .rename(columns={'comp_id':'source_comp_id'})\
            .to_dict(orient='records')

    no_results = no_parse[['source','comp_id']]\
        .assign(metadata=0,workouts=0,leaderboard=0)\
            .rename(columns={'comp_id':'source_comp_id'})\
                .to_dict(orient='records')

    results = other_results + cf_results + no_results
    results = pd.DataFrame(results)\
        .reindex(columns=[
            'source','source_comp_id','metadata',
            'workouts','leaderboard','details'
        ])

    return results

