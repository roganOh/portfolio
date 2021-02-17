with finedust_2019 as (
select * from {{ref('finedust_2019')}}
),
finedust_2020 as (
select * from {{ref('finedust_2020')}}
),

 "19s" as (
select month(date("19".date)) as month, count("19".itemcode) as count from finedust_2019 as "19" group by 1 order by 1
),
"20s" as (
select month(date("20".date)) as month, count("20".itemcode) as count from finedust_2020 as "20" group by 1 order by 1)
,
covid_china as (
select * from {{ref('covid_country')}}
where nationnm = '중국'
),
china as (
select month(date(date)) as month,(natdefcnt-(sum(natdefcnt) over(order by date rows between 1 preceding and current row))/2)*2 as defcnt from covid_china)



select "19s".month, "19s".count as "19_count", "20s".count as "20_count",sum(china.defcnt) as china_defcnt,("19_count"-"20_count")/"19_count"*100 as percentdown from "19s"
join "20s" on "20s".month = "19s".month
join china on china.month = "19s".month   
group by 1,2,3,5
order by 1
