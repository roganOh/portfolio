# -*- coding:utf-8 -*-

import pymsteams
from airflow.providers.snowflake.hooks.snowflake import *


def get_message(countries, countries_fullname, countries_shortname, repetition_number, supersets, color, **kwargs):
    year = str(kwargs['execution_date'].year)
    month = str(kwargs['execution_date'].month)
    day = str(kwargs['execution_date'].day)
    hook = SnowflakeHook(snowflake_conn_id='snowflake_chequer')
    for r in range(repetition_number):
        state = 0
        hook.get_first('''Alter warehouse airflow_warehouse resume if suspended;''')
        hook.get_first('''truncate table airflow_database.dbt_querypie.users_''' + countries[r] + ''';''')
        hook.get_first('''insert into "AIRFLOW_DATABASE"."DBT_QUERYPIE".users_''' + countries[r] + '''
        SELECT DATE(created_at) AS DATE, COUNT(id) AS USERS_CNT,sum(users_cnt) over( order by date rows unbounded preceding) as total_user
        FROM QUERYPIE_PRODUCTION_DB.USERS
        WHERE email NOT LIKE '%chequer.io%'
        AND email NOT LIKE '%querypie.com%'
        AND email_confirmed = 'Y'
        AND withdraw_date IS NULL
        and lower(country)=''' + countries_shortname[r] + '''
        group by 1;''')
        hook.get_first('''Alter warehouse airflow_warehouse resume if suspended;''')
        query_result = hook.get_pandas_df('''select sum(pageviews) as pageviews  from airflow_database.GA_WEB.with_country
            where country in (''' + countries_fullname[r] + ''')
            and date=date_from_parts(''' + year + ''',''' + month + ''',+to_number(''' + day + ''')-1)
            ''')
        pageviews = query_result['PAGEVIEWS'][0]
        hook.get_first('''Alter warehouse airflow_warehouse resume if suspended;''')
        query_result = hook.get_pandas_df('''select count(1) as new_user from QUERYPIE_PRODUCTION_DB.USERS
            where lower(country) in (''' + countries_shortname[r] + ''')
            and date(created_at)=date_from_parts(''' + year + ''',''' + month + ''',+to_number(''' + day + ''')-1)
            and lower(company) not like '%chequer%'
            ''')
        new_users = query_result['NEW_USER'][0]

        importantMessage = pymsteams.connectorcard(
            "")
        importantMessage.text(year + " 년  " + month + " 월  " + day + " 일")
        importantMessage.title("!IMPORTANT!")
        importantSection = pymsteams.cardsection()
        importantSection.title(countries_shortname[r].upper()[1:-1])
        importantSection.activityTitle("active user standard")
        if r == 0:
            cut = 90
        elif r == 1:
            cut = 85
        elif r == 2:
            cut = 60
        if pageviews >= cut:
            importantSection.addFact("PageView :", str(pageviews))
            state = 1
        if r == 0:
            cut = 9
        elif r is 1 | 2:
            cut = 8
        if new_users >= cut:
            importantSection.addFact("new_users :", str(new_users))
            state = 1
        if state:
            importantMessage.addSection(importantSection)
            importantMessage.color('ff0000')
            importantMessage.send()

        myTeamsMessage = pymsteams.connectorcard(
            "")
        myTeamsMessage.text(year + " 년  " + month + " 월  " + day + " 일")
        myTeamsMessage.title("Daily_report (DAILY)")
        myTeamsMessage.addLinkButton("Go to superset", supersets[r])
        myMessageSection = pymsteams.cardsection()
        myMessageSection.title(countries_shortname[r].upper()[1:-1])
        myMessageSection.activityTitle("active user standard")
        myMessageSection.addFact("PageView :", str(pageviews))
        myMessageSection.addFact("new_users :", str(new_users))
        myTeamsMessage.addSection(myMessageSection)
        myTeamsMessage.color(color[r])
        myTeamsMessage.send()
