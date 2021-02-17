{{ config ( severity='warn', enabled=true)}}


select
	*
from {{ref ('finedust_down')}}
where ("19_count" < "20_count")
order by 1
