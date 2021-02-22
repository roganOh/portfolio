# -*- coding:utf-8 -*-

from datetime import datetime,timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

default_args = {
    'owner': 'CHEQUER',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 30),
}

dag = DAG(
    dag_id='suspend_snowflake_querypie_after_dbt',
    default_args=default_args,
    catchup=False,
    schedule_interval='00 01 * * *')
# dag2 = DAG(
#     dag_id='suspend_snowflake_querypie_after_source_data',
#     default_args=default_args,
#     catchup=False,
#     schedule_interval='00 07 * * *')

def check_first_day(**kwargs):
    date = datetime.now().date().day
    if date == 1:
        d = 'upload_monthly'
    else:
        d = 'pass_monthly'
    return d


def check_monday(**kwargs):
    weekday = datetime.now().date().weekday()
    if weekday == 0:
        d = 'upload_weekly'
    else:
        d = 'pass_weekly'
    return d


done = DummyOperator(task_id="done", dag=dag, trigger_rule='none_failed')

daily = ExternalTaskSensor(
    task_id="daily",
    external_dag_id='dbt_daily',
    external_task_id='finish',
    mode="reschedule",
    dag=dag
)
daily >> done

is_monday = BranchPythonOperator(
    task_id='is_monday',
    provide_context=True,
    python_callable=check_monday,
    dag=dag
)
weekly = ExternalTaskSensor(
    task_id="weekly",
    external_dag_id='dbt_weekly',
    external_task_id='finish',
    execution_delta=timedelta(days=6),
    mode="reschedule",
    dag=dag
)
options_weekly = ['upload_weekly', 'pass_weekly']
for option in options_weekly:
    t = DummyOperator(
        task_id=option,
        dag=dag
    )
    if option == 'upload_weekly':
        is_monday >> t >> weekly >> done
    else:
        is_monday >> t >> done

is_first_day = BranchPythonOperator(
    task_id='is_first_day',
    provide_context=True,
    python_callable=check_first_day,
    dag=dag
)
monthly = ExternalTaskSensor(
    task_id="monthly",
    external_dag_id='dbt_monthly',
    external_task_id='finish',
    mode="reschedule",
    dag=dag
)
options_monthly = ['upload_monthly', 'pass_monthly']
for option in options_monthly:
    t = DummyOperator(
        task_id=option,
        dag=dag
    )
    if option == 'upload_monthly':
        is_first_day >> t >> monthly >> done
    else:
        is_first_day >> t >> done

suspend = SnowflakeOperator(
    task_id='suspend',
    snowflake_conn_id='snowflake_chequer',
    sql="""alter warehouse airflow_warehouse suspend""",
    autocommit=True,
    trigger_rule='none_failed',
    dag=dag
)

done >> suspend


# ****************************************************************************************
# intercom = ExternalTaskSensor(
#         task_id="intercom",
#         external_dag_id='intercom_to_snowflake',
#         external_task_id='finish',
#         execution_date_fn=lambda dt: dt+timedelta(hours=18),
#         mode="reschedule",
#         dag=dag2
#     )
#
# jira = ExternalTaskSensor(
#         task_id="jira",
#         external_dag_id='jira_to_snowflake',
#         external_task_id='finish',
#         execution_date_fn=lambda dt: dt+timedelta(hours=18),
#         mode="reschedule",
#         dag=dag2
#     )
# querypie = ExternalTaskSensor(
#         task_id="querypie",
#         external_dag_id='querypie_mysql_to_snowflake',
#         external_task_id='finish',
#         execution_date_fn=lambda dt: dt+timedelta(hours=18),
#         mode="reschedule",
#         dag=dag2
#     )
# zendesk = ExternalTaskSensor(
#         task_id="zendesk",
#         external_dag_id='zendesk_to_snowflake',
#         external_task_id='finish',
#         execution_date_fn=lambda dt: dt+timedelta(hours=18),
#         mode="reschedule",
#         dag=dag2
#     )
#
# suspend = SnowflakeOperator(
#     task_id='suspend',
#     snowflake_conn_id='snowflake_chequer',
#     sql="""alter warehouse airflow_warehouse suspend""",
#     autocommit=True,
#     trigger_rule='none_failed',
#     dag=dag2
# )


# [intercom,jira,querypie,zendesk] >> suspend