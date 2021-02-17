{{
  config(materialized='ephemeral', transient=true)
}}
with covid as (
 select * from {{ref('covid')}} 
)

select date,natdefcnt,nationnm from covid
where nationnm in ('한국','중국')
order by nationnm,date desc 
