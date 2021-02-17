select date(createdt) as date,gubun as city,defcnt,deathcnt,incdec,isolclearcnt,isolingcnt,qurrate,SEQ
from test.covid_city_raw
order by 1 desc
