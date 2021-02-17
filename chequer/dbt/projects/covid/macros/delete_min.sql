{% macro delete_min(column,ref_table) %}
  where {{column}}!=(select min({{column}}) from {{ref_table}})
{% endmacro %}
