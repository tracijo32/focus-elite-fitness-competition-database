CREATE OR REPLACE TABLE `focus-elite-comp.staging.cf_entrants` (
    comp_id INT64 NOT NULL,
    division_id INT64 NOT NULL,
    cf_id INT64 NOT NULL,
    first_name STRING NOT NULL,
    last_name STRING NOT NULL,
    country_code STRING,
    age INT64,
    height_in INT64,
    weight_lb INT64,
    comp_status STRING,
    overall_rank INT64,
    overall_score INT64,
    lb_page INT64,
    PRIMARY KEY (comp_id, division_id, cf_id) NOT ENFORCED
)
