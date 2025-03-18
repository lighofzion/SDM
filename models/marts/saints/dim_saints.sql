{{
  config({
    'schema': 'fds_ch', 
    "materialized": 'table',
    "tags": 'dim_saints'
  })
}}
SELECT
    scj_number AS ID,
    INITCAP(TRIM(REGEXP_REPLACE(name, '[^a-zA-Z ]', '', 'g'))) AS name,
    CASE 
        WHEN SUBSTRING(new_cell_grp, 2, 1) IN ('N') THEN 'North'
        WHEN SUBSTRING(new_cell_grp, 2, 1) IN ('S') THEN 'South'
        ELSE 'Unknown'
    END AS region,
    CASE 
        WHEN SUBSTRING(new_cell_grp, 3, 1) = 'Y' THEN 'Youth'
        WHEN SUBSTRING(new_cell_grp, 3, 1) = 'M' THEN 'Men'
        WHEN SUBSTRING(new_cell_grp, 3, 1) = 'W' THEN 'Women'
        ELSE 'Unknown'
    END AS department,
    REGEXP_SUBSTR(new_cell_grp, '[0-9]+')::INT AS team_id,
    CASE 
        WHEN SUBSTRING(new_cell_grp, 1, 1) IN ('A') THEN 'A'
        WHEN SUBSTRING(new_cell_grp, 1, 1) IN ('B') THEN 'B'
        WHEN SUBSTRING(new_cell_grp, 1, 1) IN ('C') THEN 'C'
        WHEN SUBSTRING(new_cell_grp, 1, 1) IN ('D') THEN 'D'
        ELSE 'Unknown'
    END AS team_name,
    NULL AS Date_of_birth,
    NULL AS Gender,
    NULL AS Phone_number,
    NULL AS City,
    NUll AS State,
    NULL AS Zip_code,
    NULL AS Country,
    {{ sdm_etl_columns() }}
FROM {{ source('public', 'staging_data') }} A
WHERE
(A.TEAM != 'RL')
OR
(new_cell_grp != 'Ins.RED LIST')
