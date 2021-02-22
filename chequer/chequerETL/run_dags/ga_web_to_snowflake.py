# -*- coding:utf-8 -*-
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from get_csv_files import get_ga_data

viewid = ''
start_date = '2018-12-01'
DIMS = ['ga:date', 'ga:yearMonth', 'ga:isoYearIsoWeek']
METRICS = ['ga:newUsers', 'ga:pageviews', 'ga:sessions', 'ga:users', 'ga:visitors']
locate = 'ga_web/ga'

default_args = {
    'owner': 'CHEQUER',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 16),
}

dag = DAG(
    dag_id='ga_web_to_snowflake',
    default_args=default_args,
    catchup=False,
    schedule_interval='40 00,06 * * *')


def which_path(**kwargs):
    flag = 0
    if flag == 0:
        d = 'upload'
    else:
        d = 'pass'
    return d


options = ['upload', 'pass']

start_get_data = DummyOperator(
    task_id='start_get_data',
    dag=dag)

is_oclock = BranchPythonOperator(
    task_id='check_is_oclock',
    provide_context=True,
    python_callable=which_path,
    dag=dag
)
end_job = DummyOperator(
    task_id='work_done',
    trigger_rule='one_success',
    dag=dag)

for option in options:
    t = DummyOperator(
        task_id=option,
        dag=dag
    )
    if option == 'upload':
        is_oclock >> t >> start_get_data
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

get_data = PythonOperator(
    task_id='get_data_ga_web',
    python_callable=get_ga_data.get_ga_data,
    op_kwargs={'viewid': viewid, 'start_date': start_date, 'DIMS': DIMS, 'METRICS': METRICS, 'locate': locate},
    dag=dag
)
finish = DummyOperator(
    task_id='fisnish',
    trigger_rule='none_skipped',
    dag=dag)
query = ['Alter warehouse airflow_warehouse resume if suspended;',
         'remove @ga_web.%report;',
         'Alter warehouse airflow_warehouse resume if suspended;',
         'truncate table ga_web.report;',
         'Alter warehouse airflow_warehouse resume if suspended;',
         'put file:///home/ec2-user/airflow/csv_files/ga_web/ga.tsv @ga_web.%report;',
         'Alter warehouse airflow_warehouse resume if suspended;',
         '''copy into airflow_database.ga_web.report from @AIRFLOW_DATABASE.ga_web.%report
        file_format=(type="csv" FIELD_OPTIONALLY_ENCLOSED_BY ="'" SKIP_HEADER=1  );''']
do_tl = SnowflakeOperator(
    task_id='tl_ga_web',
    snowflake_conn_id='snowflake_chequer',
    sql=query,
    autocommit=True,
    dag=dag
)
get_data >> do_tl >> end_job
start_get_data >> get_data

end_job >> suspend >> finish
