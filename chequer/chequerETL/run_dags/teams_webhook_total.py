# -*- coding:utf-8 -*-

from datetime import datetime

import pymsteams
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import *
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

hook = SnowflakeHook(snowflake_conn_id='snowflake_chequer')
superset = ''
default_args = {
    'owner': 'CHEQUER',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 16),
}

dag = DAG(
    dag_id='teams_webhook_total',
    default_args=default_args,
    catchup=False,
    schedule_interval='00,10 01 * * *')


def which_path(**kwargs):
    min = kwargs['execution_date'].minute
    # min=0
    if min == 0:
        d = 'upload'
    else:
        d = 'pass'
    return d


def check_first(**kwargs):
    date = kwargs['execution_date'].day
    # date=1
    if date == 1:
        d = 'yes'
    else:
        d = 'no'
    return d

def make_daily_message(**kwargs):
    date = kwargs['execution_date']
    year = str(date.year)
    month = str(date.month)
    day = str(date.day)
    hook.get_first('''Alter warehouse airflow_warehouse resume if suspended;''')
    query_result = hook.get_pandas_df('''select new_user,active_user from DBT_QUERYPIE.RAW_DATA
        where date = date_from_parts(''' + year + ''',''' + month + ''',+to_number(''' + day + ''')-1)''')
    new_users = query_result['NEW_USER'][0]
    active_users = query_result['ACTIVE_USER'][0]
    myTeamsMessage = pymsteams.connectorcard(
        "")
    myTeamsMessage.text(year + " 년  " + month + " 월  " + day + " 일")
    myTeamsMessage.title("QUERYPIE total report (DAILY)")
    myTeamsMessage.addLinkButton("Go to superset", superset)
    myMessageSection = pymsteams.cardsection()
    myMessageSection.title('QUERYPIE')
    myMessageSection.activityTitle("to see more - press 'go to superset' button")
    myMessageSection.addFact("DAU :", str(active_users))
    myMessageSection.addFact("new_users :", str(new_users))
    myTeamsMessage.addSection(myMessageSection)
    myTeamsMessage.send()

def make_monthly_message(**kwargs):
    date = kwargs['execution_date']
    year = str(date.year)
    month = str(date.month)
    day = str(date.day)
    hook.get_first('''Alter warehouse airflow_warehouse resume if suspended;''')
    query_result = hook.get_pandas_df('''select new_user,active_user from DBT_QUERYPIE.RAW_DATA_MONTHLY
        where month = (select max(month ) as month from DBT_QUERYPIE.RAW_DATA_MONTHLY)''')
    new_users = query_result['NEW_USER'][0]
    active_users = query_result['ACTIVE_USER'][0]
    myTeamsMessage = pymsteams.connectorcard(
        "")
    myTeamsMessage.text(year + " 년  " + month + " 월  " + day + " 일")
    myTeamsMessage.title("QUERYPIE total report (MONTHLY)")
    myTeamsMessage.addLinkButton("Go to superset", superset)
    myMessageSection = pymsteams.cardsection()
    myMessageSection.title('QUERYPIE')
    myMessageSection.activityTitle("to see more - press 'go to superset' button")
    myMessageSection.addFact("MAU :", str(active_users))
    myMessageSection.addFact("new_users :", str(new_users))
    myTeamsMessage.addSection(myMessageSection)
    myTeamsMessage.send()


options = ['upload', 'pass']
is_first = ['yes', 'no']

is_oclock = BranchPythonOperator(
    task_id='check_is_oclock',
    provide_context=True,
    python_callable=which_path,
    dag=dag
)
send_message_daily = PythonOperator(
    task_id='send_message_daily',
    provide_context=True,
    python_callable=make_daily_message,
    dag=dag
)
check_first= BranchPythonOperator(
    task_id='check_first',
    provide_context=True,
    python_callable=check_first,
    dag=dag
)
send_message_monthly = PythonOperator(
    task_id='send_message_monthly',
    provide_context=True,
    python_callable=make_monthly_message,
    dag=dag
)

finish_send_monthly = DummyOperator(
    task_id='finish_send_monthly',
    trigger_rule='one_success',
    dag=dag)
finish_send = DummyOperator(
    task_id='finish_send',
    trigger_rule='none_failed',
    dag=dag)
end_job = DummyOperator(
    task_id='work_done',
    trigger_rule='one_success',
    dag=dag)

for yesorno in is_first:
    t = DummyOperator(
        task_id=yesorno,
        dag=dag
    )
    if yesorno == 'yes':
        check_first >> t >> send_message_monthly >> finish_send_monthly
    else:
        check_first >> t >> finish_send_monthly

for option in options:
    t = DummyOperator(
        task_id=option,
        dag=dag
    )
    if option == 'upload':
        is_oclock >> t >> [check_first, send_message_daily]
    else:
        is_oclock >> t >> end_job

suspend = SnowflakeOperator(
    task_id='suspend',
    snowflake_conn_id='snowflake_chequer',
    sql="""alter warehouse airflow_warehouse suspend""",
    autocommit=True,
    trigger_rule='none_failed',
    dag=dag
)
finish = DummyOperator(
    task_id='finish',
    trigger_rule='none_skipped',
    dag=dag)

[finish_send_monthly, send_message_daily] >> finish_send >> end_job
end_job >> suspend >> finish
