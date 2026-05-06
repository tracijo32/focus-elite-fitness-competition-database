CREATE OR REPLACE EXTERNAL TABLE `focus-elite-comp.staging.cf_scores_external` (
    comp_id INT64 NOT NULL,
    division_id INT64 NOT NULL,
    cf_id INT64 NOT NULL,
    ordinal INT64 NOT NULL,
    rank INT64,
    score_points INT64,
    score_display STRING,
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://elite-fitness-competitions/crossfit/parsed/score_*.ndjson']
);