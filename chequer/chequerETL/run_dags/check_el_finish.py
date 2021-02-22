# -*- coding:utf-8 -*-

from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

default_args = {
    'owner': 'CHEQUER',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 30),
}

dag = DAG(
    dag_id='check_el_finish_and_suspend_snowflake',
    default_args=default_args,
    catchup=False,
    schedule_interval='00 01,07 * * *')

# intercom = ExternalTaskSensor(
#         task_id="intercom",
#         external_dag_id='intercom_to_snowflake',
#         external_task_id='finish',
#         mode="reschedule",
#         dag=dag
#     )

# jira = ExternalTaskSensor(
#         task_id="jira",
#         external_dag_id='jira_to_snowflake',
#         external_task_id='finish',
#         mode="reschedule",
#         dag=dag
#     )
# querypie = ExternalTaskSensor(
#         task_id="querypie",
#         external_dag_id='querypie_mysql_to_snowflake',
#         external_task_id='finish',
#         mode="reschedule",
#         dag=dag
#     )
querypie = DummyOperator(task_id='querypie',dag=dag)
intercom = DummyOperator(task_id='intercom',dag=dag)
jira = DummyOperator(task_id='jira',dag=dag)
zendesk = ExternalTaskSensor(
        task_id="zendesk",
        external_dag_id='zendesk_to_snowflake',
        external_task_id='finish',
        mode="reschedule",
        dag=dag
    )

el_finish = DummyOperator( task_id="el_finish", dag=dag)

suspend = SnowflakeOperator(
    task_id='suspend',
    snowflake_conn_id='snowflake_chequer',
    sql="""alter warehouse airflow_warehouse suspend""",
    autocommit=True,
    trigger_rule='none_failed',
    dag=dag
)


[intercom,jira,querypie,zendesk] >> el_finish >> suspend