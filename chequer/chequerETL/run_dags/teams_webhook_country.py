# -*- coding:utf-8 -*-

from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from get_csv_files import make_teams_message_country_daily
from get_csv_files import make_teams_message_country_longterm

default_args = {
    'owner': 'CHEQUER',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 16),
}

dag = DAG(
    dag_id='teams_webhook_country',
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


def check_tri_week(**kwargs):
    scheduled_date = kwargs['task_instance'].xcom_pull(task_ids='send_message_longterm', key='teams_long_term')
    date = str(kwargs['execution_date'])
    date = date[:10]
    print("예정된 날짜 : ", scheduled_date)
    date = datetime.strptime(date, '%Y-%m-%d')
    # date=scheduled_date
    if date == scheduled_date:
        d = 'yes'
    else:
        d = 'no'
    return d


countries = ['us', 'kr', 'br']
options = ['upload', 'pass']
is_tri = ['yes', 'no']
countries_fullname = ["'United States'", "'South Korea'", "'Brazil'"]
countries_shortname = ["'us'", "'kr'", "'br'"]
color = ['0067a3', 'ffffff', '008000']
supersets = ['', '',
             '']
repetition_number = len(countries_fullname)

is_oclock = BranchPythonOperator(
    task_id='check_is_oclock',
    provide_context=True,
    python_callable=which_path,
    dag=dag
)
send_message_longterm = PythonOperator(
    task_id='send_message_longterm',
    provide_context=True,
    python_callable=make_teams_message_country_longterm.get_message,
    op_kwargs={'countries_fullname': countries_fullname, 'countries_shortname': countries_shortname,
               'repetition_number': repetition_number, 'supersets': supersets, 'countries': countries, 'color': color},
    dag=dag
)

is_tri_week = BranchPythonOperator(
    task_id='is_tri_week',
    provide_context=True,
    python_callable=check_tri_week,
    dag=dag
)

send_message_daily = PythonOperator(
    task_id='send_message_daily',
    provide_context=True,
    python_callable=make_teams_message_country_daily.get_message,
    op_kwargs={'countries_fullname': countries_fullname, 'countries_shortname': countries_shortname,
               'repetition_number': repetition_number, 'supersets': supersets, 'countries': countries, 'color': color},
    dag=dag
)

finish_send_long_term = DummyOperator(
    task_id='finish_send_long_term',
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

for yesorno in is_tri:
    t = DummyOperator(
        task_id=yesorno,
        dag=dag
    )
    if yesorno == 'yes':
        is_tri_week >> t >> send_message_longterm >> finish_send_long_term
    else:
        is_tri_week >> t >> finish_send_long_term

for option in options:
    t = DummyOperator(
        task_id=option,
        dag=dag
    )
    if option == 'upload':
        is_oclock >> t >> [is_tri_week, send_message_daily]
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

[finish_send_long_term, send_message_daily] >> finish_send >> end_job
end_job >> suspend >> finish
