{% macro ddata_acc(from,date) %}
  ({{from}}-(sum({{from}}) over(order by {{date}} rows between 1 preceding and current row))/2)*2
{% endmacro %}
