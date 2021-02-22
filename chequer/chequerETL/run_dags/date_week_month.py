# -*- coding:utf-8 -*-

from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

default_args = {
    'owner': 'CHEQUER',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 16),
}

dag = DAG(
    dag_id='date_week_month',
    default_args=default_args,
    catchup=False,
    schedule_interval='03 01 * * *')


def which_path(**kwargs):
    flag = 0
    if flag == 0:
        d = 'upload'
    else:
        d = 'pass'
    return d


options = ['upload', 'pass']
query = ['''Alter warehouse airflow_warehouse resume if suspended;''',
        '''truncate table "AIRFLOW_DATABASE"."DBT_QUERYPIE".month_week_date;''',
         '''insert into "AIRFLOW_DATABASE"."DBT_QUERYPIE".month_week_date 
        select sub.yearmonth,sub.yearweek,sub.startdate,date(left(a.date,10)) as date from
        (select min(yearmonth) as yearmonth ,yearweek,min(date(left(date,10))) as startdate from GA_WEB.report
        group by 2
        order by 2)sub
        join ga_web.REPORT as a on a.yearweek=sub.yearweek'''
        ]

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

do_tl = SnowflakeOperator(
    task_id='tl',
    snowflake_conn_id='snowflake_chequer',
    sql=query,
    autocommit=True,
    dag=dag
)

for option in options:
    t = DummyOperator(
        task_id=option,
        dag=dag
    )
    if option == 'upload':
        is_oclock >> t >> do_tl
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



do_tl >> end_job
end_job >> suspend >> finish