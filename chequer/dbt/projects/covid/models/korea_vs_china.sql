{% set temp='tmp_korea_vs_china' %}

with origin as (
select * from {{ref(temp)}}
),

delete_min as (
select * from origin 
{{delete_min('date','origin')}}
)

select * from delete_min

