{{ config( materialized='incremental' )}}
select date(createdt) as date,areanm,nationnm,natdefcnt,natdeathcnt,natdeathrate
from test.covid_raw

{% if is_incremental() %}
  where date > (select max(date) from {{this}})
{% endif %}
order by 1 desc
