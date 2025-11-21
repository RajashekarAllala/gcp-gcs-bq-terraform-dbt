{# 
  Defaulters Model
  ----------------
  This model reads from a dynamic source (defined in schema.yml)
  and produces a table of loan defaulters.

  Fully dynamic:
    - source_name comes from vars
    - source_table comes from vars
    - no hard-coded database, schema, or table names

  Incremental merge:
    - Handles INSERT, UPDATE, DELETE automatically
#}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'Loan_ID',
    on_schema_change = 'sync_all_columns'
) }}

-- CTE for reading the dynamic source
with source_data as (

    select *
    from {{ source(
            var('source_name', 'cl_staging'),
            var('source_table', 'loans')
        ) }}

)

-- Final selection: only defaulters
select *
from source_data
where lower(Status) = 'default'