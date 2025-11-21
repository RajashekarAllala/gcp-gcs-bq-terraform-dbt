{% test table_exists(model, project=None, dataset=None) %}
{%- set project = project or target.project -%}
{%- set dataset = dataset or model.schema -%}
{%- set table = model.identifier -%}

/*
This generic test returns 1 row (causing the test to fail) when the table does NOT exist.
If the table exists, the subquery returns count > 0 and the WHERE clause is false -> zero rows -> test passes.
*/
select
  '{{ project }}.{{ dataset }}.{{ table }}' as missing_table
where (
  select count(*)
  from `{{ project }}`.{{ dataset }}.INFORMATION_SCHEMA.TABLES
  where table_name = '{{ table }}'
) = 0
{% endtest %}