# -*- coding:utf-8 -*-
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

from get_csv_files.make_daily_sqlgate_report_ga import get_ga_data

viewid = ''
start_date = '2020-01-01'
DIMS = ['ga:date', 'ga:isoYearIsoWeek', 'ga:pagePath', 'ga:country']
METRICS = ['ga:pageviews', 'ga:sessions', 'ga:users']

default_args = {
    'owner': 'CHEQUER',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 16),
}

dag = DAG(
    dag_id='sqlgate_ga_mysql',
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


query = [
    '''
    truncate table REPORT_GA_WEEKLY 
    ''',
    '''
    insert into REPORT_GA_WEEKLY 
    select isoYearIsoWeek as yearweek,pagePath,country,sum(pageviews) as pageviews, sum(sessions) as sessions,sum(users) as users
    from REPORT_GA_DAILY
    group by 1,2,3
    order by 1 desc;
    ''',
    '''
    truncate table REPORT_GA_MONTHLY
    ''',
    '''
    insert into REPORT_GA_MONTHLY 
    select left(date,6) as month,pagePath,country,sum(pageviews) as pageviews, sum(sessions) as sessions,sum(users) as users
    from REPORT_GA_DAILY
    group by 1,2,3
    order by 1 desc;
    ''',
]

options = ['upload', 'pass']

is_oclock = BranchPythonOperator(
    task_id='check_is_oclock',
    provide_context=True,
    python_callable=which_path,
    dag=dag
)
end_etl = DummyOperator(
    task_id='end_etl',
    trigger_rule='one_success',
    dag=dag)
do_etl_daily = PythonOperator(
    task_id='do_etl_daily',
    python_callable=get_ga_data,
    op_kwargs={'viewid': viewid, 'start_date': start_date, 'DIMS': DIMS, 'METRICS': METRICS,
               'tablename': 'REPORT_GA_DAILY'},
    dag=dag
)
do_query_weekly_monthly = MySqlOperator(
    task_id='do_query_weekly_monthly',
    mysql_conn_id='SQLGATE_ETLS',
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
        is_oclock >> t >> do_etl_daily >> end_etl
    else:
        is_oclock >> t >> end_etl

end_etl >> do_query_weekly_monthly
