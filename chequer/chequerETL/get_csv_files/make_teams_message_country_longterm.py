# -*- coding:utf-8 -*-

from datetime import datetime, timedelta

import pymsteams
from airflow.providers.snowflake.hooks.snowflake import *


def get_message(countries, countries_fullname, countries_shortname, repetition_number, supersets, color, **kwargs):
    daydelta = '21'
    state = 0
    date = kwargs['execution_date']
    year = str(date.year)
    month = str(date.month)
    day = str(date.day)
    date = str(date)
    hook = SnowflakeHook(snowflake_conn_id='snowflake_chequer')
    for r in range(repetition_number):
        query_result = hook.get_pandas_df('''select sum(pageviews) as pageviews  from airflow_database.GA_WEB.with_country
            where country in (''' + countries_fullname[r] + ''')
            and date>=date_from_parts(''' + year + ''',''' + month + ''',+to_number(''' + day + ''')-''' + daydelta + ''')
            ''')
        pageviews = query_result['PAGEVIEWS'][0]
        hook.get_first('''Alter warehouse airflow_warehouse resume if suspended;''')
        query_result = hook.get_pandas_df('''select count(1) as new_user from QUERYPIE_PRODUCTION_DB.USERS
            where lower(country) in (''' + countries_shortname[r] + ''')
            and created_at>=date_from_parts(''' + year + ''',''' + month + ''',+to_number(''' + day + ''')-''' + daydelta + ''')
            and lower(company) not like '%chequer%'
            ''')
        new_users = query_result['NEW_USER'][0]
        hook.get_first('''Alter warehouse airflow_warehouse resume if suspended;''')
        hook.get_first('''truncate table airflow_database.teams.users_top_company_''' + countries[r] + ''';''')
        hook.get_first('''insert into airflow_database.teams.users_top_company_''' + countries[r] + '''
            select a.company,a.users from
            (select count(distinct(sub.id)) as users,sub.company as company from
            (select distinct(id) as id,name,lower(company) as company ,company_size,date(created_at) as created_at,LAST_ACCESS_AT,roles,email from querypie_production_db.users 
            where lower(country) in (''' + countries_shortname[r] + ''')
            and lower(company) not like '%chequer%'
            and company_size not like '1-49'
            and lower(company) not like 'self'
            and lower(company) not like 'n/a'
            and lower(company) not like 'none'
            order by 5 desc,3,2)sub
            join QUERYPIE_PRODUCTION_DB.WORKSPACE_SUB_ACTION as a on a.user_id=sub.id
            where date(a.action_at) >=date_from_parts(''' + year + ''',''' + month + ''',+to_number(''' + day + ''')-''' + daydelta + ''')
            and a.action_type in ('GET_SUBSCRIPTION','GET_WORKSPACE','CONNECT','GET_COMMENT')
            group by 2)a
            order by 2 desc,1  asc
            ''')
        users_top_companies = hook.get_pandas_df(
            '''select * from airflow_database.teams.users_top_company_''' + countries[r] + ''' where users>=3''')
        count_users_top_companies = len(users_top_companies['COMPANY'])
        hook.get_first('''Alter warehouse airflow_warehouse resume if suspended;''')
        hook.get_first('''truncate table airflow_database.teams.top_action_company_''' + countries[r] + ''';''')
        hook.get_first('''insert into teams.top_action_company_''' + countries[r] + '''
            select company,count(1) as actions from
            (select distinct(id),name,lower(company) as company ,company_size,date(created_at) as created_at from querypie_production_db.users 
            where lower(country) in (''' + countries_shortname[r] + ''')
            and lower(company) not like '%chequer%'
            and company_size not like '1-49'
            and lower(company) not like 'self'
            and lower(company) not like 'n/a'
            and lower(company) not like 'none'
            order by 5 desc,3,2)sub
            join QUERYPIE_PRODUCTION_DB.WORKSPACE_SUB_ACTION as a on a.user_id=sub.id
            where date(a.action_at) >=date_from_parts(''' + year + ''',''' + month + ''',+to_number(''' + day + ''')-''' + daydelta + ''')
            and a.action_type in ('GET_SUBSCRIPTION','GET_WORKSPACE','CONNECT','GET_COMMENT')
            group by 1
            order by 2 desc; 
            ''')
        top_actions_companies = hook.get_pandas_df(
            '''select * from airflow_database.teams.top_action_company_''' + countries[r] + ''' limit 5''')
        hook.get_first('''Alter warehouse airflow_warehouse resume if suspended;''')
        hook.get_first('''truncate table airflow_database.teams.top_action_user_''' + countries[r] + ''';''')
        hook.get_first('''insert into airflow_database.teams.top_action_user_''' + countries[r] + '''
            select sub.id,sub.name,sub.email,sub.company,sub.company_size,sub.created_at,count(1) as actions from
            (select b.id,b.name,b.EMAIL,lower(company) as company ,company_size,date(created_at) as created_at,a.action_at,a.action_type from querypie_production_db.users as b
            join QUERYPIE_PRODUCTION_DB.WORKSPACE_SUB_ACTION as a on a.user_id=b.ID
            where lower(country) in (''' + countries_shortname[r] + ''')
            and lower(company) not like '%chequer%'
            and b.EMAIL_CONFIRMED='Y'
            and a.action_at>=date_from_parts(''' + year + ''',''' + month + ''',+to_number(''' + day + ''')-''' + daydelta + ''')
            and a.action_type in ('GET_SUBSCRIPTION','GET_WORKSPACE','CONNECT','GET_COMMENT'))sub
            group by 1,2,3,4,5,6
            order by 7 desc

            ''')
        top_users = hook.get_pandas_df(
            '''select * from  airflow_database.teams.top_action_user_''' + countries[r] + ''' limit 3''')

        importantMessage = pymsteams.connectorcard(
            "")
        importantMessage.text(year + " 년  " + month + " 월  " + day + " 일")
        importantMessage.title("!IMPORTANT!")
        importantSection = pymsteams.cardsection()
        importantSection.title(countries_shortname[r].upper()[1:-1])
        importantSection.activityTitle("active user standard")
        if r == 0:
            cut = 2700
        elif r == 1:
            cut = 3000
        elif r == 2:
            cut = 2400
        if pageviews >= cut:
            importantSection.addFact("PageView :", str(pageviews))
            state = 1
        if r == 0:
            cut = 140
        elif r == 1:
            cut = 70
        elif r == 2:
            cut = 150
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
        myTeamsMessage.title("long_term_report (" + daydelta + "days)")
        myTeamsMessage.addLinkButton("Go to superset", supersets[r])
        myMessageSection = pymsteams.cardsection()
        myMessageSection.title(countries_shortname[r].upper()[1:-1])
        myMessageSection.activityTitle("active user standard")
        myMessageSection.addFact("PageView :", str(pageviews))
        myMessageSection.addFact("new_users :", str(new_users))
        for i in range(1, count_users_top_companies + 1):
            if i == 1:
                myMessageSection.addFact("users_top_companies : ",
                                         str(users_top_companies['COMPANY'][i - 1]) + "  ->          " + str(
                                             users_top_companies['USERS'][i - 1]) + "  users")
            else:
                myMessageSection.addFact("      ",
                                         str(users_top_companies['COMPANY'][i - 1]) + "  ->          " + str(
                                             users_top_companies['USERS'][i - 1]) + "  users")
        for i in range(1, 6):
            if i == 1:
                myMessageSection.addFact("top_action_companies : ",
                                         str(top_actions_companies['COMPANY'][i - 1]) + "  ->          " + str(
                                             top_actions_companies['ACTIONS'][i - 1]) + "  actions")
            else:
                myMessageSection.addFact("      ",
                                         str(top_actions_companies['COMPANY'][i - 1]) + "  ->          " + str(
                                             top_actions_companies['ACTIONS'][i - 1]) + "  actions")
        for i in range(1, 4):
            if i == 1:
                myMessageSection.addFact("top_action_users : ",
                                         str(top_users['ID'][i - 1]) + "(" + str(
                                             top_users['NAME'][i - 1]) + ")  ->          " + str(
                                             top_users['ACTIONS'][i - 1]) + "  actions")
            else:
                myMessageSection.addFact("      ", str(top_users['ID'][i - 1]) + "(" + str(
                    top_users['NAME'][i - 1]) + ")  ->          " + str(
                    top_users['ACTIONS'][i - 1]) + "  actions")
        myTeamsMessage.addSection(myMessageSection)
        myTeamsMessage.color(color[r])
        myTeamsMessage.send()
    print(date[:10])
    date = date[:10]
    date = datetime.strptime(date, '%Y-%m-%d')
    kwargs['task_instance'].xcom_push(key="teams_long_term", value=date + timedelta(days=21))
