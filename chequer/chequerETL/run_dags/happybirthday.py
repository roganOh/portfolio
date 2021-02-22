# -*- coding:utf-8 -*-

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from get_csv_files import chequer_calender

default_args = {
    'owner': 'CHEQUER',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 16),
}

dag = DAG(
    dag_id='happy_birthday',
    default_args=default_args,
    catchup=False,
    schedule_interval='00 01 * * *')

send_message = PythonOperator(
    task_id='send_mesage',
    python_callable=chequer_calender.happy_birthday,
    dag=dag
)