# -*- coding:utf-8 -*-

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'CHEQUER',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 16),
}

dag = DAG(
    dag_id='test2',
    default_args=default_args,
    catchup=False,
    schedule_interval='@once')


def push(**kwargs):
    date = str(kwargs['execution_date'])
    date = date[:10]
    date = datetime.strptime(date, '%Y-%m-%d')
    print(kwargs['ti'])
    kwargs['task_instance'].xcom_push(key="test", value=date + timedelta(days=95))


def pull(**kwargs):
    goal = kwargs['task_instance'].xcom_pull(task_ids='1', key='test')
    print(goal)


first = PythonOperator(
    task_id='1',
    provide_context=True,
    python_callable=push,
    dag=dag)

second = PythonOperator(
    task_id='2',
    provide_context=True,
    python_callable=pull,
    dag=dag)

first >> second