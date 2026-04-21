CREATE OR REPLACE TABLE `focus-elite-comp.staging.cf_scores` (
    comp_id INT64 NOT NULL,
    division_id INT64 NOT NULL,
    cf_id INT64 NOT NULL,
    ordinal INT64 NOT NULL,
    rank INT64,
    score_points INT64,
    score_display STRING,
    PRIMARY KEY (comp_id, division_id, cf_id, ordinal) NOT ENFORCED
)