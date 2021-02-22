# -*- coding:utf-8 -*-

from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from get_csv_files import get_intercom_data

default_args = {
    'owner': 'CHEQUER',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 16),
}

dag = DAG(
    dag_id='intercom_to_snowflake',
    default_args=default_args,
    catchup=False,
    schedule_interval='00 01,07 * * *')


def which_path(**kwargs):
    flag = 0
    if flag == 0:
        d = 'upload'
    else:
        d = 'pass'
    return d


options = ['upload', 'pass']
tables = ['conversations', 'contacts']
access_token = ''

start_tl = DummyOperator(
    task_id='start_tl',
    dag=dag)

is_oclock = BranchPythonOperator(
    task_id='check_is_oclock',
    provide_context=True,
    python_callable=which_path,
    dag=dag
)

end_copy = DummyOperator(
    task_id='end_copy',
    dag=dag)
end_job = DummyOperator(
    task_id='work_done',
    trigger_rule='one_success',
    dag=dag)
get_data = PythonOperator(
    task_id='get_data',
    python_callable=get_intercom_data.get_data,
    op_kwargs={'access_token': access_token, 'tables': tables},
    dag=dag
)
for option in options:
    t = DummyOperator(
        task_id=option,
        dag=dag
    )
    if option == 'upload':
        is_oclock >> t >> get_data
    else:
        is_oclock >> t >> end_job


finish = DummyOperator(
    task_id='finish',
    trigger_rule='none_failed',
    dag=dag)
for i in range(0, len(tables)):
    args = tables[i]

    query = ['Alter warehouse airflow_warehouse resume if suspended;',
             'remove @intercom.%' + args + ';',
             'Alter warehouse airflow_warehouse resume if suspended;',
             'truncate table intercom.' + args + ';',
             'Alter warehouse airflow_warehouse resume if suspended;',
             'put file:///home/ec2-user/airflow/csv_files/intercom/' + args + '.tsv @intercom.%' + args + ';',
             'Alter warehouse airflow_warehouse resume if suspended;',
             'copy into intercom.' + args + ' from @AIRFLOW_DATABASE.intercom.%' + args +
             ''' file_format=(type="csv" FIELD_OPTIONALLY_ENCLOSED_BY ="'" SKIP_HEADER=1  );''']
    do_tl = SnowflakeOperator(
        task_id='tl_' + args,
        snowflake_conn_id='snowflake_chequer',
        sql=query,
        autocommit=True,
        dag=dag
    )
    start_tl >> do_tl >> end_copy
    get_data >> start_tl

end_copy >> end_job >> finish
