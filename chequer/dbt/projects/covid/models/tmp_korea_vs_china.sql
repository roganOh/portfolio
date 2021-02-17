{{
config(materialized='ephemeral', transient = true)
}}

with covid_korea as (
select * from {{ref('covid_country')}}
where nationnm = '한국'
),

covid_china as (
select * from {{ref('covid_country')}}
where nationnm = '중국'
),

korea as (
select date,{{ddata_acc('natdefcnt','date')}} as defcnt from covid_korea),

china as (
select date,{{ddata_acc('natdefcnt','date')}} as defcnt from covid_china)

select china.date,korea.defcnt as korea_defcnt,china.defcnt as china_defcnt
from korea
full outer join china on china.date=korea.date

