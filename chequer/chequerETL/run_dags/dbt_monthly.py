# -*- coding:utf-8 -*-

from datetime import datetime,timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from dateutil.relativedelta import relativedelta

default_args = {
    'owner': 'CHEQUER',
    'depends_on_past': False,
    'start_date': datetime(2020, 9, 1),
}

dag = DAG(
    dag_id='dbt_monthly',
    default_args=default_args,
    catchup=False,
    schedule_interval='00 01 01 * *')


def which_path(**kwargs):
    flag = 0
    if flag == 0:
        d = 'upload'
    else:
        d = 'pass'
    return d


options = ['upload', 'pass']

start_t = ExternalTaskSensor(
        task_id="start_t",
        external_dag_id='check_el_finish',
        external_task_id='el_finish',
        mode="reschedule",
        execution_date_fn=lambda dt: dt+relativedelta(months=1),
        dag=dag
    )
is_oclock = BranchPythonOperator(
    task_id='check_is_oclock',
    provide_context=True,
    python_callable=which_path,
    dag=dag
)
start_tl = DummyOperator(
    task_id='start_tl',
    dag=dag)
end_job = DummyOperator(
    task_id='work_done',
    trigger_rule='one_success',
    dag=dag)
for option in options:
    t = DummyOperator(
        task_id=option,
        dag=dag
    )
    if option == 'upload':
        is_oclock >> t >> start_tl
    else:
        is_oclock >> t >> end_job

finish = DummyOperator(
    task_id='finish',
    trigger_rule='none_failed',
    dag=dag)

query = ['''Alter warehouse airflow_warehouse resume if suspended;''',
         '''truncate table "AIRFLOW_DATABASE"."DBT_QUERYPIE"."GA_APP_MONTHLY";''',
         '''insert into "AIRFLOW_DATABASE"."DBT_QUERYPIE"."GA_APP_MONTHLY"
         SELECT date(DATE_TRUNC('MONTH', date)) AS MONTH
            , count(distinct(EVENTACTION)) as Cnt_Connect
            , IFNULL(SUM(CASE WHEN EVENTACTION = 'Connect BigQuery' then UNIQUEEVENTS end), 0) AS Connect_BigQuery
            , IFNULL(SUM(CASE WHEN EVENTACTION = 'Connect MariaDB' then UNIQUEEVENTS end), 0) AS Connect_Maria
            , IFNULL(SUM(CASE WHEN EVENTACTION = 'Connect MySQL' then UNIQUEEVENTS end), 0) AS Connect_MySQL
            , IFNULL(SUM(CASE WHEN EVENTACTION = 'Connect PostgreSQL' then UNIQUEEVENTS end), 0) AS Connect_PostgreSQL
            , IFNULL(SUM(CASE WHEN EVENTACTION = 'Connect RedShift' then UNIQUEEVENTS end), 0) AS Connect_Redshift
            , IFNULL(SUM(CASE WHEN EVENTACTION = 'Connect SAP HANA' then UNIQUEEVENTS end), 0) AS Connect_SAP_HANA
            , IFNULL(SUM(CASE WHEN EVENTACTION = 'Connect Snowflake' then UNIQUEEVENTS end), 0) AS Connect_Snowflake
            , IFNULL(SUM(CASE WHEN EVENTACTION = 'Connect SQLServer' then UNIQUEEVENTS end), 0) AS Connect_SQLServer
            , IFNULL(SUM(CASE WHEN EVENTACTION = 'Connect Presto' then UNIQUEEVENTS end), 0) AS Connect_Presto
            , IFNULL(SUM(CASE WHEN EVENTACTION = 'Connect Azure' then UNIQUEEVENTS end), 0) AS Connect_Azure
            , IFNULL(SUM(CASE WHEN EVENTACTION = 'Connect Oracle' then UNIQUEEVENTS end), 0) AS Connect_Oracle
        FROM "AIRFLOW_DATABASE"."GA_APP"."REPORT"
        GROUP BY 1
        ORDER BY 1;''',
         '''Alter warehouse airflow_warehouse resume if suspended;''',
         '''truncate table "AIRFLOW_DATABASE"."DBT_QUERYPIE"."GA_WEB_MONTHLY";''',
         '''insert into "AIRFLOW_DATABASE"."DBT_QUERYPIE"."GA_WEB_MONTHLY"
         SELECT date(DATE_TRUNC('MONTH', date)) AS MONTH, sum(PAGEVIEWS) as PAGEVIEWS, sum(SESSIONS) as SESSIONS, sum(USERS) as USERS FROM "AIRFLOW_DATABASE"."GA_WEB".REPORT
         group by 1
         ORDER BY 1 ASC;''',
         '''Alter warehouse airflow_warehouse resume if suspended;''',
         '''truncate table "AIRFLOW_DATABASE"."DBT_QUERYPIE"."APP_DOWNLOAD_MONTHLY";''',
         '''insert into "AIRFLOW_DATABASE"."DBT_QUERYPIE"."APP_DOWNLOAD_MONTHLY"
         SELECT date(DATE_TRUNC('MONTH', A.date)) AS MONTH
             ,IFNULL(SUM(CASE WHEN A.OS = 'MAC' then A.DOWNLOAD end), 0) AS MAC_DOWNLOAD
             ,IFNULL(SUM(CASE WHEN A.OS = 'WINDOWS' then A.DOWNLOAD end), 0) AS WINDOWS_DOWNLOAD
             ,IFNULL(SUM(CASE WHEN A.OS = 'LINUX' then A.DOWNLOAD end), 0) AS LINUX_DOWNLOAD
             ,IFNULL(SUM(CASE WHEN A.OS = 'MAC' OR A.OS = 'WINDOWS' OR A.OS = 'LINUX' then A.DOWNLOAD end), 0) AS TOTAL_DOWNLOAD
         FROM (
         SELECT
             DATE(CREATED_AT) as DATE,
             PARSE_JSON(META_DATA)['platform']::TEXT AS OS,
             COUNT(1) AS DOWNLOAD
         FROM QUERYPIE_PRODUCTION_DB.action_logs
         WHERE
             action = 'QUERYPIE_DOWNLOAD'
         GROUP BY
             DATE, OS) A
         GROUP BY 1
         ORDER BY 1 ASC;''',
         '''Alter warehouse airflow_warehouse resume if suspended;''',
         '''truncate table "AIRFLOW_DATABASE"."DBT_QUERYPIE"."APP_GUEST_MONTHLY";''',
         '''insert into "AIRFLOW_DATABASE"."DBT_QUERYPIE"."APP_GUEST_MONTHLY"
         SELECT
             date(DATE_TRUNC('MONTH', created_at)) AS MONTH,
             COUNT(1) AS COUNT
         FROM
             "AIRFLOW_DATABASE"."QUERYPIE_PRODUCTION_DB"."GET_STARTED"
         GROUP BY 1
         ORDER BY 1;''',
         '''Alter warehouse airflow_warehouse resume if suspended;''',
         '''truncate table "AIRFLOW_DATABASE"."DBT_QUERYPIE"."APP_USERS_MONTHLY";''',
         '''insert into "AIRFLOW_DATABASE"."DBT_QUERYPIE"."APP_USERS_MONTHLY"
         SELECT date(DATE_TRUNC('MONTH', created_at)) AS MONTH, COUNT(id) AS USERS_CNT,sum(users_cnt) over( order by month rows unbounded preceding)+95 as total_user
         FROM QUERYPIE_PRODUCTION_DB.USERS
         WHERE email NOT LIKE '%chequer.io%'
         AND email NOT LIKE '%querypie.com%'
         AND email_confirmed = 'Y'
         AND withdraw_date IS NULL
         and date(created_at) > '2019-05-31'
         GROUP BY 1
         order by 1;''',
         '''Alter warehouse airflow_warehouse resume if suspended;''',
         '''truncate table airflow_database.dbt_querypie.guest_os_type_monthly;''',
         '''insert into airflow_database.dbt_querypie.guest_os_type_monthly
         select date(DATE_TRUNC('MONTH', created_at)) as month
             ,IFNULL(SUM(CASE WHEN os_type = 'Mac' then 1 end), 0) AS Mac
             ,IFNULL(SUM(CASE WHEN os_type = 'Linux' then 1 end), 0) AS Linux
             ,IFNULL(SUM(CASE WHEN os_type = 'Windows' then 1 end), 0) AS Windows
             ,IFNULL(SUM(CASE WHEN os_type is NULL then 1 end), 0) as noinfo
         from airflow_database.QUERYPIE_PRODUCTION_DB.GET_STARTED
         group by 1
         order by 1;''',
         '''Alter warehouse airflow_warehouse resume if suspended;''',
         '''truncate table airflow_database.dbt_querypie.active_user_monthly;''',
         '''insert into airflow_database.dbt_querypie.active_user_monthly
         select a.month
            ,IFNULL(SUM(CASE WHEN a.device_os = 'Mac' then 1 end), 0) AS Mac
            ,IFNULL(SUM(CASE WHEN a.device_os = 'Linux' then 1 end), 0) AS Linux
            ,IFNULL(SUM(CASE WHEN a.device_os = 'Windows' then 1 end), 0) AS Windows
            ,IFNULL(SUM(CASE WHEN a.device_os is NULL then 1 end), 0) AS noinfo
            ,count(distinct(user_id)) as grand_total
         from
         (select distinct(user_id),date(DATE_TRUNC('MONTH', action_at)) as month,device_os from "AIRFLOW_DATABASE".QUERYPIE_PRODUCTION_DB.WORKSPACE_SUB_ACTION where month>'2020-05-01' and action_type in ('GET_SUBSCRIPTION','GET_WORKSPACE','CONNECT','GET_COMMENT'))a
         group by 1
         order by 1;''',
         '''Alter warehouse airflow_warehouse resume if suspended;''',
         '''truncate table "AIRFLOW_DATABASE"."DBT_QUERYPIE"."RAW_DATA_MONTHLY";''',
         '''insert into "AIRFLOW_DATABASE"."DBT_QUERYPIE"."RAW_DATA_MONTHLY"

         select distinct(date(active_user_monthly.month)) AS month
            ,ga_web_monthly.PAGEVIEWS as PAGEVIEWS
            ,ga_web_monthly.SESSIONS as VISIT_VIEW
            ,ga_web_monthly.USERS as VISITOR
            ,app_download_monthly.TOTAL_DOWNLOAD as TOTAL_DOWNLOAD
            ,app_download_monthly.LINUX_DOWNLOAD as LINUX_DOWNLOAD
            ,app_download_monthly.MAC_DOWNLOAD as MAC_DOWNLOAD
            ,app_download_monthly.WINDOWS_DOWNLOAD as WINDOWS_DOWNLOAD
            ,app_guest_monthly.COUNT as GUEST
            ,guest_OS_TYPE_monthly.MAC as guest_MAC
            ,guest_OS_TYPE_monthly.LINUX as guest_LINUX
            ,guest_OS_TYPE_monthly.WINDOWS as guest_WINDOWS
            ,guest_OS_TYPE_monthly.NOINFO as guest_NOINFO

            ,app_users_monthly.USERS_CNT as NEW_USER
            ,app_users_monthly.total_user as total_user

            ,active_user_monthly.grand_total as active_user
            ,active_user_monthly.mac as mac
            ,active_user_monthly.linux as linux
            ,active_user_monthly.windows as windows
            ,active_user_monthly.noinfo as noinfo

            ,ga_app_monthly.Connect_BigQuery AS Connect_BigQuery
            ,ga_app_monthly.Connect_Maria AS Connect_Maria
            ,ga_app_monthly.Connect_MySQL AS Connect_MySQL
            ,ga_app_monthly.Connect_PostgreSQL AS Connect_PostgreSQL
            ,ga_app_monthly.Connect_Redshift AS Connect_Redshift
            ,ga_app_monthly.Connect_SAP_HANA AS Connect_SAP_HANA
            ,ga_app_monthly.Connect_Snowflake AS Connect_Snowflake
            ,ga_app_monthly.Connect_SQLServer AS Connect_SQLServer
            ,ga_app_monthly.Connect_Presto AS Connect_Presto
            ,ga_app_monthly.Connect_Azure AS Connect_Azure
            ,ga_app_monthly.Connect_Oracle AS Connect_Oracle

        from  airflow_database.dbt_querypie.active_user_monthly
            join "AIRFLOW_DATABASE"."DBT_QUERYPIE".app_download_monthly on active_user_monthly.month = app_download_monthly.month
            join "AIRFLOW_DATABASE"."DBT_QUERYPIE".app_guest_monthly on app_download_monthly.month = app_guest_monthly.month
            join "AIRFLOW_DATABASE"."DBT_QUERYPIE".app_users_monthly on app_guest_monthly.month = app_users_monthly.month
            join airflow_database.dbt_querypie.guest_OS_TYPE_monthly on guest_OS_TYPE_monthly.month=active_user_monthly.month
            left join "AIRFLOW_DATABASE"."DBT_QUERYPIE".ga_web_monthly on active_user_monthly.month=ga_web_monthly.month
            left join "AIRFLOW_DATABASE"."DBT_QUERYPIE".ga_app_monthly on app_users_monthly.month = ga_app_monthly.month
        WHERE active_user_monthly.month > '2019-05-01'
        order by 1 desc
 ''']
do_tl = SnowflakeOperator(
    task_id='tl_',
    snowflake_conn_id='snowflake_chequer',
    sql=query,
    autocommit=True,
    dag=dag
)
start_t >> is_oclock
start_tl >> do_tl >> end_job
end_job >> finish