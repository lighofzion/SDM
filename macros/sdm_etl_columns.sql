{% macro sdm_etl_columns()%}
  'DBT_'||'{{this.name}}' as etl_batch_id,
  '{{target.user}}' as etl_insert_user_id,
  CURRENT_TIMESTAMP as etl_insert_rec_dttm,
  cast(null as varchar(100)) as etl_update_user_id,
  cast(null as timestamp) as etl_update_rec_dttm
{% endmacro %}