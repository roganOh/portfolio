# -*- coding:utf-8 -*-

from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator

from get_csv_files import get_csv_mysql_to_mysql

default_args = {
    'owner': 'CHEQUER',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 16),
}

dag = DAG(
    dag_id='sqlgate_mysql_to_mysql',
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
tables = ['ACTION_LOG', 'ESTIMATE_M', 'ORDER_M', 'PRODUCT_M', 'SUB_ACTION_HISTORY_L', 'SUB_M', 'SUB_USER_M',
          'USER_M', 'SUB_EDUCATION_M']

start_etl = DummyOperator(
    task_id='start_etl',
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
    trigger_rule='none_failed',
    dag=dag)
etc = DummyOperator(task_id='for suspend', trigger_rule='one_success', dag=dag)

for option in options:
    t = DummyOperator(
        task_id=option,
        dag=dag
    )
    if option == 'upload':
        is_oclock >> t >> start_etl
    else:
        is_oclock >> t >> end_job

for table in tables:
    do_etl = PythonOperator(
        task_id='do_etl_' + table,
        python_callable=get_csv_mysql_to_mysql.get_csv,
        op_kwargs={'table': table},
        dag=dag
    )
    start_etl >> do_etl >> end_copy

end_copy >> etc >> end_job
