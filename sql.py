from google.cloud import bigquery
from datetime import datetime
from typing import Union, get_args

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
        if isinstance(field.annotation, Union):
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

    full_table_id = f'{gcp_params.project_id}.{dataset_name}.{model_name}'
    table = bigquery.Table(full_table_id)
    table.external_data_configuration = ext_conf

    CLIENT.delete_table(full_table_id,not_found_ok=True)
    CLIENT.create_table(table)

    return table

def create_entrant_external_table():
    return _create_external_table_from_model( 
        model = m.Entrant, 
        model_name = 'entrants',
        source_uris = [
            f'gs://{BUCKET_NAME}/{source}/parsed/*/entrants.ndjson'
            for source in SOURCES
        ]
    )

def create_scores_external_table():
    return _create_external_table_from_model( 
        model = m.Score, 
        model_name = 'scores',
        source_uris = [
            f'gs://{BUCKET_NAME}/{source}/parsed/*/scores.ndjson'
            for source in SOURCES
        ]
    )

def create_metadata_external_table():
    return _create_external_table_from_model(
        model = m.Metadata, 
        model_name = 'metadata',
        source_uris = [
            f'gs://{BUCKET_NAME}/{source}/parsed/*/metadata.ndjson'
            for source in SOURCES if source != 'crossfit'
        ] + [
            f'gs://{BUCKET_NAME}/crossfit/metadata.ndjson'
        ]
    )

def create_athlete_external_table():
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

def create_competition_index_external_table():
    ext_conf = bigquery.ExternalConfig("NEWLINE_DELIMITED_JSON")
    ext_conf.source_uris = [f'gs://{BUCKET_NAME}/consolidated/competition-index.ndjson']

    schema = [
        bigquery.SchemaField('global_comp_id', 'STRING', 'REQUIRED'),
        bigquery.SchemaField('title', 'STRING', 'REQUIRED'),
        bigquery.SchemaField('year', 'INTEGER', 'REQUIRED'),
        bigquery.SchemaField('source', 'STRING', 'REQUIRED'),
        bigquery.SchemaField('source_comp_id', 'STRING', 'REQUIRED'),
        bigquery.SchemaField('is_cf_stage', 'BOOLEAN', 'REQUIRED')
    ]

    table = bigquery.Table(f'{gcp_params.project_id}.staging.competition_index')
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
        bigquery.SchemaField('mn_id', 'STRING', 'REPEATED')
    ]
    table = bigquery.Table(f'{gcp_params.project_id}.staging.athletes_master')
    table.schema = schema
    table.external_data_configuration = ext_conf

    CLIENT.delete_table(table, not_found_ok=True)
    CLIENT.create_table(table)

    return table

if __name__ == '__main__':

    # create_competition_index_external_table()
    # create_metadata_external_table()
    # create_entrant_external_table()
    # create_scores_external_table()
    # create_athlete_external_table()
    # create_location_overrides_external_table()
    create_athletes_master_external_table()