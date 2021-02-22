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
    'start_date': datetime(2020, 7, 16),
}

dag = DAG(
    dag_id='dbt_daily',
    default_args=default_args,
    catchup=False,
    schedule_interval='00 01 * * *')


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
        execution_date_fn=lambda dt: dt+timedelta(hours=6),
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
         '''truncate table "AIRFLOW_DATABASE"."DBT_QUERYPIE"."GA_APP";''',
         '''insert into "AIRFLOW_DATABASE"."DBT_QUERYPIE"."GA_APP"
         SELECT date(date) AS date
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
         '''truncate table "AIRFLOW_DATABASE"."DBT_QUERYPIE"."GA_WEB";''',
         '''insert into "AIRFLOW_DATABASE"."DBT_QUERYPIE"."GA_WEB"
         SELECT DATE(DATE) AS DATE, PAGEVIEWS, SESSIONS, USERS FROM "AIRFLOW_DATABASE"."GA_WEB".REPORT
         ORDER BY 1 ASC;''',
         '''Alter warehouse airflow_warehouse resume if suspended;''',
         '''truncate table "AIRFLOW_DATABASE"."DBT_QUERYPIE"."APP_DOWNLOAD";''',
         '''insert into "AIRFLOW_DATABASE"."DBT_QUERYPIE"."APP_DOWNLOAD"
         SELECT A.DATE
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
         GROUP BY A.DATE
         ORDER BY DATE ASC ;''',
         '''Alter warehouse airflow_warehouse resume if suspended;''',
         '''truncate table "AIRFLOW_DATABASE"."DBT_QUERYPIE"."APP_GUEST";''',
         '''insert into "AIRFLOW_DATABASE"."DBT_QUERYPIE"."APP_GUEST"
         SELECT
             DATE(CREATED_AT) AS DATE,
             COUNT(1) AS COUNT
         FROM
             "AIRFLOW_DATABASE"."QUERYPIE_PRODUCTION_DB"."GET_STARTED"
         GROUP BY DATE
         ORDER BY DATE ;''',
         '''Alter warehouse airflow_warehouse resume if suspended;''',
         '''truncate table "AIRFLOW_DATABASE"."DBT_QUERYPIE"."APP_USERS";''',
         '''insert into "AIRFLOW_DATABASE"."DBT_QUERYPIE"."APP_USERS"
         SELECT DATE(created_at) AS DATE, COUNT(id) AS USERS_CNT,sum(users_cnt) over( order by date rows unbounded preceding)+95 as total_user
         FROM QUERYPIE_PRODUCTION_DB.USERS
         WHERE email NOT LIKE '%chequer.io%'
         AND email NOT LIKE '%querypie.com%'
         AND email_confirmed = 'Y'
         AND withdraw_date IS NULL
         and date(created_at) > '2019-05-31'
         group by 1;''',
         '''Alter warehouse airflow_warehouse resume if suspended;''',
         '''truncate table airflow_database.dbt_querypie.guest_os_type;''',
         '''insert into airflow_database.dbt_querypie.guest_os_type
         select DATE(created_at) as date
             ,IFNULL(SUM(CASE WHEN os_type = 'Mac' then 1 end), 0) AS Mac
             ,IFNULL(SUM(CASE WHEN os_type = 'Linux' then 1 end), 0) AS Linux
             ,IFNULL(SUM(CASE WHEN os_type = 'Windows' then 1 end), 0) AS Windows
             ,IFNULL(SUM(CASE WHEN os_type is NULL then 1 end), 0) as noinfo
         from airflow_database.QUERYPIE_PRODUCTION_DB.GET_STARTED
         group by 1
         order by 1;''',
         '''Alter warehouse airflow_warehouse resume if suspended;''',
         '''truncate table airflow_database.dbt_querypie.active_user;''',
         '''insert into airflow_database.dbt_querypie.active_user

            select a.date
            ,IFNULL(SUM(CASE WHEN a.device_os = 'Mac' then 1 end), 0) AS Mac
            ,IFNULL(SUM(CASE WHEN a.device_os = 'Linux' then 1 end), 0) AS Linux
            ,IFNULL(SUM(CASE WHEN a.device_os = 'Windows' then 1 end), 0) AS Windows
            ,IFNULL(SUM(CASE WHEN a.device_os is NULL then 1 end), 0) AS noinfo
            ,count(distinct(user_id)) as grand_total
         from
         (select distinct(user_id),date(action_at) as date,device_os from "AIRFLOW_DATABASE".QUERYPIE_PRODUCTION_DB.WORKSPACE_SUB_ACTION where date>='2020-06-01' and action_type in ('GET_SUBSCRIPTION','GET_WORKSPACE','CONNECT','GET_COMMENT'))a
         group by 1
         order by 1;''',
         '''Alter warehouse airflow_warehouse resume if suspended;''',
         '''truncate table "AIRFLOW_DATABASE"."DBT_QUERYPIE"."RAW_DATA";''',
         '''insert into "AIRFLOW_DATABASE"."DBT_QUERYPIE"."RAW_DATA"
            select distinct(date(active_user.date)) AS DATE
            ,ga_web.PAGEVIEWS as PAGEVIEWS
            ,ga_web.SESSIONS as VISIT_VIEW
            ,ga_web.USERS as VISITOR
            ,app_download.TOTAL_DOWNLOAD as TOTAL_DOWNLOAD
            ,app_download.LINUX_DOWNLOAD as LINUX_DOWNLOAD
            ,app_download.MAC_DOWNLOAD as MAC_DOWNLOAD
            ,app_download.WINDOWS_DOWNLOAD as WINDOWS_DOWNLOAD
            ,app_guest.COUNT as GUEST
            ,guest_OS_TYPE.MAC as guest_MAC
            ,guest_OS_TYPE.LINUX as guest_LINUX
            ,guest_OS_TYPE.WINDOWS as guest_WINDOWS
            ,guest_OS_TYPE.NOINFO as guest_NOINFO

            ,app_users.USERS_CNT as NEW_USER
            ,app_users.total_user as total_user

            ,active_user.grand_total as active_user
            ,active_user.mac as mac
            ,active_user.linux as linux
            ,active_user.windows as windows
            ,active_user.noinfo as noinfo

            ,ga_app.Connect_BigQuery AS Connect_BigQuery
            ,ga_app.Connect_Maria AS Connect_Maria
            ,ga_app.Connect_MySQL AS Connect_MySQL
            ,ga_app.Connect_PostgreSQL AS Connect_PostgreSQL
            ,ga_app.Connect_Redshift AS Connect_Redshift
            ,ga_app.Connect_SAP_HANA AS Connect_SAP_HANA
            ,ga_app.Connect_Snowflake AS Connect_Snowflake
            ,ga_app.Connect_SQLServer AS Connect_SQLServer
            ,ga_app.Connect_Presto AS Connect_Presto
            ,ga_app.Connect_Azure AS Connect_Azure
            ,ga_app.Connect_Oracle AS Connect_Oracle

        from  airflow_database.dbt_querypie.active_user
            join "AIRFLOW_DATABASE"."DBT_QUERYPIE".app_download on active_user.date = app_download.date
            join "AIRFLOW_DATABASE"."DBT_QUERYPIE".app_guest on app_download.date = app_guest.date
            join "AIRFLOW_DATABASE"."DBT_QUERYPIE".app_users on app_guest.date = app_users.date
            join airflow_database.dbt_querypie.guest_OS_TYPE on guest_OS_TYPE.date=active_user.date
            left join "AIRFLOW_DATABASE"."DBT_QUERYPIE".ga_web on active_user.date=ga_web.date
            left join "AIRFLOW_DATABASE"."DBT_QUERYPIE".ga_app on app_users.date = ga_app.date
        WHERE active_user.date > '2019-05-31'
        order by 1 desc
        '''
         ]
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