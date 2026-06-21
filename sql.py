from google.cloud import bigquery
from datetime import datetime
from typing import get_args, get_origin

from google.cloud.storage.client import Client
from pydantic import BaseModel
import models as m
from parameters import GoogleCloudParameters

gcp_params = GoogleCloudParameters()
CLIENT = bigquery.Client(project=gcp_params.project_id)
BUCKET_NAME = gcp_params.bucket_name
SOURCES = [
    'crossfit',
    'competition-corner',
    'strongest',
    'score-it',
    'local-comp',
    'manual'
]

def _create_external_table_from_model(
    model: BaseModel,
    model_name: str,
    source_uris: list[str],
    dataset_name: str = 'staging'
):

    ext_conf = bigquery.ExternalConfig("NEWLINE_DELIMITED_JSON")

    schema = []
    for fn, field in model.model_fields.items():
        origin = get_origin(field.annotation)
        if origin is not None:
            t = [a for a in get_args(field.annotation)
                 if a is not type(None)][0]
        elif isinstance(field.annotation, type):
            t = field.annotation
        else:
            raise ValueError(f'Unsupported field type: {field.annotation}')
            
        if t == str or t == datetime:
            field_type = 'STRING'
        elif t == int:
            field_type = 'INTEGER'
        elif t == float:
            field_type = 'FLOAT'
        elif t == bool:
            field_type = 'BOOLEAN'
        else:
            raise ValueError(f'Unsupported field type: {t}')

        req = 'REQUIRED' if field.is_required() else 'NULLABLE'

        schema_field = bigquery.SchemaField(fn, field_type, mode=req)
        schema.append(schema_field)

    ext_conf.schema = schema
    ext_conf.source_uris = source_uris
    ext_conf.ignore_unknown_values = True

    full_table_id = f'{gcp_params.project_id}.{dataset_name}.{model_name}'
    table = bigquery.Table(full_table_id)
    table.external_data_configuration = ext_conf

    CLIENT.delete_table(full_table_id,not_found_ok=True)
    CLIENT.create_table(table)

    return table

def _create_view(view_name: str, sql: str, dataset: str = "dev"):
    view_id = f"{CLIENT.project}.{dataset}.{view_name}"
    view = bigquery.Table(view_id)
    view.view_query = sql
    CLIENT.delete_table(view_id, not_found_ok=True)
    CLIENT.create_table(view)
    return view

def _create_table(table_name: str, sql: str, dataset: str = "dev"):
    table_id = f"{CLIENT.project}.{dataset}.{table_name}"
    query = f"CREATE OR REPLACE TABLE `{table_id}` AS {sql}"
    CLIENT.query(query).result()
    return CLIENT.get_table(table_id)

###----------------------------------------------------------------------------
### external tables and views in staging

def create_entrants_external_table():
    return _create_external_table_from_model( 
        model = m.Entrant, 
        model_name = 'entrants_raw',
        source_uris = [
            f'gs://{BUCKET_NAME}/{source}/parsed/*/entrants.ndjson'
            for source in SOURCES
        ]
    )

def create_entrants_view():
    view_query = """
    SELECT SPLIT(_FILE_NAME, '/')[SAFE_OFFSET(3)] AS source, * 
    FROM `staging.entrants_raw`
    """
    return _create_view(
        view_name = 'entrants',
        sql = view_query,
        dataset = 'staging'
    )

def create_scores_external_table():
    return _create_external_table_from_model( 
        model = m.Score, 
        model_name = 'scores_raw',
        source_uris = [
            f'gs://{BUCKET_NAME}/{source}/parsed/*/scores.ndjson'
            for source in SOURCES
        ]
    )

def create_scores_view():
    view_query = """
    SELECT SPLIT(_FILE_NAME, '/')[SAFE_OFFSET(3)] AS source, * 
    FROM `staging.scores_raw`
    """
    return _create_view(
        view_name = 'scores',
        sql = view_query,
        dataset = 'staging'
    )

def create_metadata_external_table():
    return _create_external_table_from_model(
        model = m.Metadata, 
        model_name = 'metadata_raw',
        source_uris = [
            f'gs://{BUCKET_NAME}/{source}/parsed/*/metadata.ndjson'
            for source in SOURCES
        ]
    )
def create_metadata_view():
    view_query = """
    SELECT SPLIT(_FILE_NAME, '/')[SAFE_OFFSET(3)] AS source, * 
    FROM `staging.metadata_raw`
    """
    return _create_view(
        view_name = 'metadata',
        sql = view_query,
        dataset = 'staging'
    )

def create_workouts_external_table():
    return _create_external_table_from_model(
        model = m.Workout,
        model_name = 'workouts_raw',
        source_uris = [
            f'gs://{BUCKET_NAME}/{source}/parsed/*/workouts.ndjson'
            for source in SOURCES if source != 'crossfit'
        ] + [
            f'gs://{BUCKET_NAME}/crossfit/parsed/workouts.ndjson'
        ]
    )

def create_workouts_view():
    view_query = """
    SELECT SPLIT(_FILE_NAME, '/')[SAFE_OFFSET(3)] AS source, * 
    FROM `staging.workouts_raw`
    """
    return _create_view(
        view_name = 'workouts',
        sql = view_query,
        dataset = 'staging'
    )

def create_crossfit_stages_external_table():
    return _create_external_table_from_model(
        model = m.CrossFitStage,
        model_name = 'crossfit_stages',
        source_uris = [f'gs://{BUCKET_NAME}/consolidated/crossfit_stages.ndjson']
    )

def create_sources_external_table():
    return _create_external_table_from_model(
        model = m.Source,
        model_name = 'sources',
        source_uris = [f'gs://{BUCKET_NAME}/consolidated/sources.ndjson']
    )

def create_crossfit_athletes_external_table():
    bucket_prefix = f'gs://{gcp_params.bucket_name}'

    ext_conf = bigquery.ExternalConfig("PARQUET")
    ext_conf.source_uris = [f'{bucket_prefix}/crossfit/athletes/*']

    hive_opts = bigquery.HivePartitioningOptions()
    hive_opts.mode = 'AUTO'
    hive_opts.source_uri_prefix = f'{bucket_prefix}/crossfit/athletes/'
    hive_opts.require_partition_filter = False

    ext_conf.hive_partitioning_options = hive_opts

    schema = [
        bigquery.SchemaField('cf_id', 'INTEGER'),
        bigquery.SchemaField('name', 'STRING'),
        bigquery.SchemaField('year', 'INTEGER'),
        bigquery.SchemaField('comp_type', 'STRING'),
        bigquery.SchemaField('comp_id', 'INTEGER'),
        bigquery.SchemaField('div_id', 'INTEGER'),
        bigquery.SchemaField('overall_rank', 'STRING'),
        bigquery.SchemaField('overall_score', 'STRING'),
        bigquery.SchemaField('first_name', 'STRING'),
        bigquery.SchemaField('last_name', 'STRING'),
        bigquery.SchemaField('gender', 'STRING'),
        bigquery.SchemaField('age', 'STRING'),
        bigquery.SchemaField('height', 'STRING'),
        bigquery.SchemaField('weight', 'STRING'),
        bigquery.SchemaField('country_name', 'STRING'),
        bigquery.SchemaField('region_name', 'STRING'),
        bigquery.SchemaField('affiliate_name', 'STRING'),
        bigquery.SchemaField('profile_pic', 'STRING'),
        bigquery.SchemaField('name_clean', 'STRING')
    ]

    table = bigquery.Table(f'{gcp_params.project_id}.staging.cf_athletes')
    table.schema = schema
    table.external_data_configuration = ext_conf

    CLIENT.delete_table(table, not_found_ok=True)
    CLIENT.create_table(table)

    return table

def create_workout_seq_overrides_external_table():
    ext_conf = bigquery.ExternalConfig("NEWLINE_DELIMITED_JSON")
    ext_conf.source_uris = [f'gs://{BUCKET_NAME}/consolidated/workout-seq-overrides.ndjson']

    schema = [
        bigquery.SchemaField('source', 'STRING', 'REQUIRED'),
        bigquery.SchemaField('source_comp_id', 'STRING', 'REQUIRED'),
        bigquery.SchemaField('source_workout_id', 'STRING', 'REQUIRED'),
        bigquery.SchemaField('seq', 'INTEGER', 'REQUIRED'),
    ]
    table = bigquery.Table(f'{gcp_params.project_id}.staging.workout_seq_overrides')
    table.schema = schema
    table.external_data_configuration = ext_conf

    CLIENT.delete_table(table, not_found_ok=True)
    CLIENT.create_table(table)

    return table

def create_location_overrides_external_table():
    ext_conf = bigquery.ExternalConfig("NEWLINE_DELIMITED_JSON")
    ext_conf.source_uris = [f'gs://{BUCKET_NAME}/consolidated/location-overrides.ndjson']

    schema = [
        bigquery.SchemaField('global_comp_id', 'STRING', 'REQUIRED'),
        bigquery.SchemaField('priority', 'INTEGER', 'REQUIRED'),
        bigquery.SchemaField('venue_name', 'STRING', 'REQUIRED'),
        bigquery.SchemaField('address', 'STRING', 'REQUIRED'),
        bigquery.SchemaField('lat', 'FLOAT', 'REQUIRED'),
        bigquery.SchemaField('lng', 'FLOAT', 'REQUIRED'),
    ]
    table = bigquery.Table(f'{gcp_params.project_id}.staging.location_overrides')
    table.schema = schema
    table.external_data_configuration = ext_conf

    CLIENT.delete_table(table, not_found_ok=True)
    CLIENT.create_table(table)

    return table

def create_athletes_master_external_table():
    ext_conf = bigquery.ExternalConfig("NEWLINE_DELIMITED_JSON")
    ext_conf.source_uris = [f'gs://{BUCKET_NAME}/consolidated/athletes_master.ndjson']

    schema = [
        bigquery.SchemaField('global_athlete_id', 'INTEGER', 'REQUIRED'),
        bigquery.SchemaField('name', 'STRING', 'REQUIRED'),
        bigquery.SchemaField('gender', 'STRING', 'REQUIRED'),
        bigquery.SchemaField('first_name', 'STRING', 'REPEATED'),
        bigquery.SchemaField('last_name', 'STRING', 'REPEATED'),
        bigquery.SchemaField('nickname', 'STRING', 'REPEATED'),
        bigquery.SchemaField('cf_id', 'STRING', 'REPEATED'),
        bigquery.SchemaField('si_id', 'STRING', 'REPEATED'),
        bigquery.SchemaField('cc_id', 'STRING', 'REPEATED'),
        bigquery.SchemaField('str_id', 'STRING', 'REPEATED'),
        bigquery.SchemaField('mn_id', 'STRING', 'REPEATED'),
        bigquery.SchemaField('lc_id', 'STRING', 'REPEATED'),
        bigquery.SchemaField('is_not', 'INTEGER', 'REPEATED')
    ]
    table = bigquery.Table(f'{gcp_params.project_id}.staging.athletes_master')
    table.schema = schema
    table.external_data_configuration = ext_conf

    CLIENT.delete_table(table, not_found_ok=True)
    CLIENT.create_table(table)

    return table

def create_athletes_source_id_view():
    source_map = {
        'crossfit': 'cf_id',
        'strongest': 'str_id',
        'manual': 'mn_id',
        'score-it': 'si_id',
        'competition-corner': 'cc_id'
    }

    query = "\nUNION ALL\n".join([
    f"""SELECT 
        global_athlete_id,
        "{source}" as source,
        {col} as source_athlete_id
    FROM `staging.athletes_master`
    INNER JOIN UNNEST({col}) AS {col}"""
        for source, col in source_map.items()
    ])

    view_name = 'athlete_source_id'
    return _create_view(view_name, query, 'dev')

###----------------------------------------------------------------------------
### global mapped tables in dev
def create_source_to_global_metadata_table():
    query = """
    WITH joined AS (
    SELECT 
        s.global_comp_id,
        s.priority AS source_priority,
        m.title,
        m.start_date,
        m.end_date,
        m.venue_name,
        m.address,
        m.lat,
        m.lng,
        m.virtual
    FROM `staging.sources` s
    LEFT JOIN `staging.metadata` m
        ON s.source = m.source
    AND s.source_comp_id = m.source_comp_id
    ), metadata AS (
        SELECT
        global_comp_id,
        1 as priority,
        (ARRAY_AGG(title       IGNORE NULLS ORDER BY source_priority LIMIT 1))[OFFSET(0)] AS title,
        (ARRAY_AGG(start_date  IGNORE NULLS ORDER BY source_priority LIMIT 1))[OFFSET(0)] AS start_date,
        (ARRAY_AGG(end_date    IGNORE NULLS ORDER BY source_priority LIMIT 1))[OFFSET(0)] AS end_date,
        (ARRAY_AGG(venue_name  IGNORE NULLS ORDER BY source_priority LIMIT 1))[OFFSET(0)] AS venue_name,
        (ARRAY_AGG(address     IGNORE NULLS ORDER BY source_priority LIMIT 1))[OFFSET(0)] AS address,
        (ARRAY_AGG(lat         IGNORE NULLS ORDER BY source_priority LIMIT 1))[OFFSET(0)] AS lat,
        (ARRAY_AGG(lng         IGNORE NULLS ORDER BY source_priority LIMIT 1))[OFFSET(0)] AS lng,
        (ARRAY_AGG(virtual     IGNORE NULLS ORDER BY source_priority LIMIT 1))[OFFSET(0)] AS virtual
        FROM joined
        GROUP BY global_comp_id
    ), locations_flat AS (
        SELECT
            COALESCE(l.global_comp_id, m.global_comp_id) AS global_comp_id,
            COALESCE(l.venue_name, m.venue_name) AS venue_name,
            COALESCE(l.address, m.address) AS address,
            COALESCE(l.lat, m.lat) AS lat,
            COALESCE(l.lng, m.lng) AS lng,
            COALESCE(l.priority, m.priority) AS priority
        FROM `staging.location_overrides` l
        RIGHT JOIN metadata m
            ON l.global_comp_id = m.global_comp_id
            AND l.priority = m.priority
    )
    SELECT
        m.global_comp_id,
        m.title,
        m.start_date,
        m.end_date,
        m.virtual,
        ARRAY_AGG(
            IF(
                m.virtual,
                NULL,
                STRUCT(
                    f.venue_name,
                    f.address,
                    f.lat,
                    f.lng
                )
            )
            IGNORE NULLS
            ORDER BY f.priority
        ) AS locations
    FROM metadata m
    LEFT JOIN locations_flat f
        ON m.global_comp_id = f.global_comp_id
    GROUP BY
        m.global_comp_id,
        m.title,
        m.start_date,
        m.end_date,
        m.virtual
    """
    table_name = 'source_to_global_metadata'
    return _create_table(table_name, query, 'dev')



def create_source_to_global_entrants_table():
    query = """
    WITH comps AS (
        SELECT global_comp_id, source, source_comp_id
        FROM `staging.sources`
        WHERE priority = 1
    ), entrants_raw AS (
        SELECT 
        c.global_comp_id, e.gender, e.display_name, 
        c.source, c.source_comp_id, e.source_athlete_id,
        e.overall_points, e.overall_rank, e.dq
        FROM comps c
        JOIN `staging.entrants` e
        ON SPLIT(e._FILE_NAME, '/')[SAFE_OFFSET(3)] = c.source
        AND c.source_comp_id = e.source_comp_id
    ), finals_2020 AS (
        SELECT * FROM entrants_raw
        WHERE global_comp_id = 'games-2020-finals'
        AND overall_points > 0
    ), stage1_2020 AS (
        SELECT 
            e.global_comp_id, e.gender, e.display_name,
            e.source, e.source_comp_id, e.source_athlete_id,
            CAST(s.score_display AS FLOAT64) as overall_points, 
            s.rank as overall_rank, e.dq
        FROM entrants_raw e
        JOIN `staging.scores` s
        ON e.source = SPLIT(s._FILE_NAME, '/')[SAFE_OFFSET(3)]
        AND e.source_comp_id = s.source_comp_id
        AND e.source_athlete_id = s.source_athlete_id
        WHERE global_comp_id = 'games-2020-stage1'
        AND s.source_workout_id = "8"
    ), entrants AS (
        SELECT * FROM finals_2020
        UNION ALL
        SELECT * FROM stage1_2020
        UNION ALL
        SELECT * FROM entrants_raw
        WHERE global_comp_id NOT IN ('games-2020-finals', 'games-2020-stage1')
    )
    SELECT 
        e.global_comp_id, 
        e.source,
        e.source_comp_id,
        e.gender, e.display_name,
        CONCAT(
            e.global_comp_id, '-', e.gender, CAST(ROW_NUMBER() OVER (
                PARTITION BY e.global_comp_id, e.gender 
                ORDER BY e.overall_rank
            ) AS STRING)
        ) AS global_entrant_id,
        a.global_athlete_id,
        e.source_athlete_id,
        e.overall_rank,
        e.overall_points,
        e.dq
    FROM entrants e
    LEFT JOIN `dev.athlete_source_id` a
    ON e.source = a.source
    AND e.source_athlete_id = a.source_athlete_id
    """
    table_name = 'source_to_global_entrants'
    return _create_table(table_name, query, 'dev')

def create_source_to_global_workouts_table():
    query = """
    SELECT
    c.global_comp_id, c.source, w.*,
    CONCAT(c.global_comp_id, '-S', w.seq) AS global_workout_id
    FROM `staging.sources` c
    JOIN `staging.workouts` w
    ON c.source = SPLIT(w._FILE_NAME, '/')[SAFE_OFFSET(3)]
    AND c.source_comp_id = w.source_comp_id
    WHERE c.priority = 1
    AND NOT (c.global_comp_id = 'games-2020-stage1' AND seq > 8) 
    AND NOT (c.global_comp_id = 'games-2020-finals' AND seq < 8)
    """
    table_name = 'source_to_global_workouts'
    return _create_table(table_name, query, 'dev')

def create_source_to_global_scores_table():
    query = """
    SELECT 
        w.global_comp_id, w.source, w.source_comp_id, w.source_workout_id, w.global_workout_id,
        e.source_athlete_id, e.global_entrant_id,
        s.gender, s.score_display, s.tiebreak_display, s.rank, s.points
    FROM `dev.source_to_global_workouts` w
    JOIN `staging.scores` s
    ON w.source = SPLIT(s._FILE_NAME, '/')[SAFE_OFFSET(3)]
    AND w.source_comp_id = s.source_comp_id
    AND w.source_workout_id = s.source_workout_id
    JOIN `dev.source_to_global_entrants` e
    ON e.source = SPLIT(s._FILE_NAME, '/')[SAFE_OFFSET(3)]
    AND s.source_comp_id = e.source_comp_id
    AND s.source_athlete_id = e.source_athlete_id
    """
    table_name = 'source_to_global_scores'
    return _create_table(table_name, query, 'dev')

###----------------------------------------------------------------------------
### api views in dev
def create_api_entrants_view():
    query = """
    SELECT 
        s2g.global_comp_id,
        s2g.gender,
        s2g.global_entrant_id,
        s2g.global_athlete_id,
        s2g.display_name,
        s2g.overall_rank,
        CASE WHEN conf.score_type = 'time' THEN 
        FORMAT(
            '%02d:%02d',
            DIV(CAST(TRUNC(s2g.overall_points) AS INT64), 60),
            MOD(CAST(TRUNC(s2g.overall_points) AS INT64), 60)
        )
        WHEN conf.score_type = 'integer' THEN 
        CAST(TRUNC(s2g.overall_points) AS STRING)
        ELSE CAST(s2g.overall_points AS STRING)
        END AS overall_points
    FROM `dev.source_to_global_entrants` s2g
    JOIN `dev.leaderboard_config` conf
    ON s2g.global_comp_id = conf.global_comp_id
    """
    return _create_view('api_entrants', query, 'dev')

def create_source_leaderboard_url_view():
    replacements = {' ':'%20', '(': '%28', ')': '%29'}
    rep_str = "COALESCE(m.title, '')"
    for k,v in replacements.items():
        rep_str = f"REPLACE({rep_str}, '{k}', '{v}')"

    query = f"""
    SELECT 
        s.global_comp_id, s.source, s.source_comp_id,
        CASE 
            WHEN s.source = 'competition-corner'
            THEN CONCAT(
                'https://competitioncorner.net/ff/',
                s.source_comp_id,'/results'
            )
            WHEN s.source = 'local-comp' 
            THEN CONCAT(
                'https://local-comp.com/controller/event/leaderboard?eventId=',
                s.source_comp_id
            )
            WHEN s.source = 'strongest' AND s.source_comp_id LIKE 'ri20%'
            THEN 'https://roguefitness.com/invitational/leaderboard'
            WHEN s.source = 'strongest'
            THEN CONCAT(
                'https://compete.strongest.com/competitions/',
                s.source_comp_id,'/leaderboard'
            )
            WHEN s.source = 'crossfit' AND c.stage IN ('open','games')
            THEN CONCAT(
                'https://games.crossfit.com/leaderboard/',
                c.stage, '/', c.season
            )
            WHEN s.source = 'crossfit'
            THEN CONCAT(
                'https://games.crossfit.com/leaderboard/',
                c.stage, 's/', c.season, '?', c.stage, '=', s.source_comp_id
            )
            WHEN s.source = 'score-it'
            THEN CONCAT(
                'https://scoreit.co.za/leaderboard/',
                s.source_comp_id, '/',
                {rep_str}
            )
            ELSE NULL
        END AS leaderboard_url
    FROM `staging.sources` AS s
    LEFT JOIN `staging.crossfit_stages` AS c
    ON s.global_comp_id = c.global_comp_id
    AND s.source = 'crossfit'
    AND CAST(s.source_comp_id AS STRING) = CAST(c.comp_id AS STRING)
    LEFT JOIN `staging.metadata` m 
    ON s.source = SPLIT(m._FILE_NAME, '/')[SAFE_OFFSET(3)]
    AND s.source_comp_id = m.source_comp_id
    """
    return _create_view('source_leaderboard_url', query, 'dev')

###----------------------------------------------------------------------------
## helper functions for mass execution
def create_all_external_tables_and_views():
    print('Creating entrants external table & view...')
    create_entrants_external_table()
    create_entrants_view()

    print('Creating scores external table & view...')
    create_scores_external_table()
    create_scores_view()

    print('Creating metadata external table & view...')
    create_metadata_external_table()
    create_metadata_view()

    print('Creating workouts external table & view...')
    create_workouts_external_table()
    create_workouts_view()

    print('Creating crossfit stages external table...')
    create_crossfit_stages_external_table()

    print('Creating sources external table...')
    create_sources_external_table()

    print('Creating crossfit athletes external table...')
    create_crossfit_athletes_external_table()

    print('Creating location overrides external table...')
    create_location_overrides_external_table()

    print('Creating athletes master external table')
    print('& exploded source id view...')
    create_athletes_master_external_table()
    create_athletes_source_id_view()

    print('Done.')

if __name__ == '__main__':
    create_all_external_tables_and_views()