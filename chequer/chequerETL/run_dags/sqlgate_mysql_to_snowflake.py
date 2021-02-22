# -*- coding:utf-8 -*-

from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from get_csv_files import get_csv

default_args = {
    'owner': 'CHEQUER',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 16),
}

dag = DAG(
    dag_id='sqlgate_mysql_to_snowflake',
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
tables = ['ACTION_LOG', 'ORDER_M', 'SUB_ACTION_HISTORY_L', 'SUB_EDUCATION_M', 'SUB_USER_M']

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
    python_callable=get_csv.get_csv,
    op_kwargs={'conn_id': 'mysql_sqlgate_db', 'directory': 'sqlgate_db', 'tables': tables},
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

suspend = SnowflakeOperator(
    task_id='suspend',
    snowflake_conn_id='snowflake_chequer',
    sql="""alter warehouse airflow_warehouse suspend""",
    trigger_rule='none_failed',
    autocommit=True,
    dag=dag
)
finish = DummyOperator(
    task_id='fisnish',
    trigger_rule='none_skipped',
    dag=dag)
for i in range(0, len(tables)):
    args = tables[i]

    query = ["""Alter warehouse airflow_warehouse resume if suspended;""",
             'remove @SQLGATE_PRODUCTION_DB.%' + args + ';',
             """Alter warehouse airflow_warehouse resume if suspended;""",
             'truncate table SQLGATE_PRODUCTION_DB.'+ args+';',
             'Alter warehouse airflow_warehouse resume if suspended;',
             """put file:///home/ec2-user/airflow/csv_files/sqlgate_db/""" + args + """.tsv @SQLGATE_PRODUCTION_DB.%""" + args + """;""",
             """Alter warehouse airflow_warehouse resume if suspended;""",
             """copy into SQLGATE_PRODUCTION_DB.""" + args + """ from @AIRFLOW_DATABASE.SQLGATE_PRODUCTION_DB.%""" + args +
             """ file_format=(type="csv" FIELD_OPTIONALLY_ENCLOSED_BY ="'"  );"""]
    do_tl = SnowflakeOperator(
        task_id='tl_' + args,
        snowflake_conn_id='snowflake_chequer',
        sql=query,
        autocommit=True,
        dag=dag
    )
    start_tl >> do_tl >> end_copy
    get_data >> start_tl

end_copy >> end_job >> suspend >> finish