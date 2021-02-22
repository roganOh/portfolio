# -*- coding:utf-8 -*-

from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from get_csv_files import get_zendesk_data

default_args = {
    'owner': 'CHEQUER',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 16),
}

dag = DAG(
    dag_id='zendesk_to_snowflake',
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
tables = ['groups', 'group_memberships', 'macros', 'organizations', 'satisfaction_ratings', 'tags', 'tickets',
          'ticket_audits', 'ticket_fields', 'ticket_forms', 'ticket_metrics', 'users']
user = ''
pwd = ''


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
    python_callable=get_zendesk_data.get_zendesk_data,
    op_kwargs={'user': user, 'pwd': pwd, 'tables': tables},
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
             'remove @zendesk.%' + args + ';',
             'Alter warehouse airflow_warehouse resume if suspended;',
             'truncate table zendesk.'+ args+';',
             'Alter warehouse airflow_warehouse resume if suspended;',
             'put file:///home/ec2-user/airflow/csv_files/zendesk/' + args + '.tsv @zendesk.%' + args + ';',
             'Alter warehouse airflow_warehouse resume if suspended;',
             'copy into zendesk.' + args + ' from @AIRFLOW_DATABASE.zendesk.%' + args +
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

end_copy >> end_job >>  finish
