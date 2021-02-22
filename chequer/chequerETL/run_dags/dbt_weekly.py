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
    dag_id='dbt_weekly',
    default_args=default_args,
    catchup=False,
    schedule_interval='00 01 * * 1')


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
        execution_date_fn=lambda dt: dt+timedelta(hours=150),
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
         '''truncate table "AIRFLOW_DATABASE"."DBT_QUERYPIE"."GA_APP_WEEKLY";''',
         '''insert into "AIRFLOW_DATABASE"."DBT_QUERYPIE"."GA_APP_WEEKLY"
         SELECT sub.yearweek,min(sub.startdate) as startdate
            , IFNULL(SUM(CASE WHEN sub.EVENTACTION = 'Connect BigQuery' then sub.UNIQUEEVENTS end), 0) AS Connect_BigQuery
            , IFNULL(SUM(CASE WHEN sub.EVENTACTION = 'Connect MariaDB' then sub.UNIQUEEVENTS end), 0) AS Connect_Maria
            , IFNULL(SUM(CASE WHEN sub.EVENTACTION = 'Connect MySQL' then sub.UNIQUEEVENTS end), 0) AS Connect_MySQL
            , IFNULL(SUM(CASE WHEN sub.EVENTACTION = 'Connect PostgreSQL' then sub.UNIQUEEVENTS end), 0) AS Connect_PostgreSQL
            , IFNULL(SUM(CASE WHEN sub.EVENTACTION = 'Connect RedShift' then sub.UNIQUEEVENTS end), 0) AS Connect_Redshift
            , IFNULL(SUM(CASE WHEN sub.EVENTACTION = 'Connect SAP HANA' then sub.UNIQUEEVENTS end), 0) AS Connect_SAP_HANA
            , IFNULL(SUM(CASE WHEN sub.EVENTACTION = 'Connect Snowflake' then sub.UNIQUEEVENTS end), 0) AS Connect_Snowflake
            , IFNULL(SUM(CASE WHEN sub.EVENTACTION = 'Connect SQLServer' then sub.UNIQUEEVENTS end), 0) AS Connect_SQLServer
            , IFNULL(SUM(CASE WHEN sub.EVENTACTION = 'Connect Presto' then sub.UNIQUEEVENTS end), 0) AS Connect_Presto
            , IFNULL(SUM(CASE WHEN sub.EVENTACTION = 'Connect Azure' then sub.UNIQUEEVENTS end), 0) AS Connect_Azure
            , IFNULL(SUM(CASE WHEN sub.EVENTACTION = 'Connect Oracle' then sub.UNIQUEEVENTS end), 0) AS Connect_Oracle
        FROM
        (select w.yearweek,w.startdate,"AIRFLOW_DATABASE"."GA_APP"."REPORT".*
        from "AIRFLOW_DATABASE"."GA_APP"."REPORT"
        join DBT_QUERYPIE.MONTH_WEEK_DATE as w on left(report.DATE,10)=w.date)sub
        GROUP BY 1
        ORDER BY 1; ''',
         '''Alter warehouse airflow_warehouse resume if suspended;''',
         '''truncate table "AIRFLOW_DATABASE"."DBT_QUERYPIE"."GA_WEB_WEEKLY";''',
         '''insert into "AIRFLOW_DATABASE"."DBT_QUERYPIE"."GA_WEB_WEEKLY"
         SELECT sub.yearweek,min(sub.startdate) as startdate, sum(sub.PAGEVIEWS) as PAGEVIEWS, sum(sub.SESSIONS) as SESSIONS, sum(sub.USERS) as USERS
        FROM
        (select w.yearweek as yearweek,w.startdate,report.pageviews,report.SESSIONS,report.USERS,report.date
        from "AIRFLOW_DATABASE"."GA_WEB"."REPORT"
        join airflow_database.DBT_QUERYPIE.MONTH_WEEK_DATE as w on left(report.DATE,10)=w.date
        where left(report.date,10)>='2019-07-01')sub
        group by 1
        ORDER BY 1 ;''',
         '''Alter warehouse airflow_warehouse resume if suspended;''',
         '''truncate table "AIRFLOW_DATABASE"."DBT_QUERYPIE"."APP_DOWNLOAD_WEEKLY";''',
         '''insert into "AIRFLOW_DATABASE"."DBT_QUERYPIE"."APP_DOWNLOAD_WEEKLY"
         SELECT concat(yearofweek(date(sub.date)),to_char(weekofyear(date(sub.date)),'09')) as yearweek
				,min(sub.date) as startdate
				,IFNULL(SUM(CASE WHEN sub.OS = 'MAC' then sub.DOWNLOAD end), 0) AS MAC_DOWNLOAD
				,IFNULL(SUM(CASE WHEN sub.OS = 'WINDOWS' then sub.DOWNLOAD end), 0) AS WINDOWS_DOWNLOAD
				,IFNULL(SUM(CASE WHEN sub.OS = 'LINUX' then sub.DOWNLOAD end), 0) AS LINUX_DOWNLOAD
				,IFNULL(SUM(CASE WHEN sub.OS = 'MAC' OR sub.OS = 'WINDOWS' OR sub.OS = 'LINUX' then sub.DOWNLOAD end), 0) AS TOTAL_DOWNLOAD
			FROM
			(select a.date,A.OS,A.DOWNLOAD
			from
				(
			SELECT
				DATE(CREATED_AT) as DATE,
				PARSE_JSON(META_DATA)['platform']::TEXT AS OS,
				COUNT(1) AS DOWNLOAD
			FROM QUERYPIE_PRODUCTION_DB.action_logs
			WHERE
				action = 'QUERYPIE_DOWNLOAD'
			GROUP BY
				DATE, OS) A
			)sub
			GROUP BY 1
			ORDER BY 1 desc;''',
         '''Alter warehouse airflow_warehouse resume if suspended;''',
         '''truncate table "AIRFLOW_DATABASE"."DBT_QUERYPIE"."APP_GUEST_WEEKLY";''',
         '''insert into "AIRFLOW_DATABASE"."DBT_QUERYPIE"."APP_GUEST_WEEKLY"
         SELECT
            sub.yearweek,min(sub.startdate) as startdate,
            COUNT(1) AS COUNT
        FROM
        (SELECT concat(yearofweek(date(created_at)),to_char(weekofyear(date(created_at)),'09')) as yearweek,
            g.id,date(g.created_at) as startdate
        from "AIRFLOW_DATABASE"."QUERYPIE_PRODUCTION_DB"."GET_STARTED" as g
        )sub
        GROUP BY 1
        ORDER BY 1 desc;''',
         '''Alter warehouse airflow_warehouse resume if suspended;''',
         '''truncate table "AIRFLOW_DATABASE"."DBT_QUERYPIE"."APP_USERS_WEEKLY";''',
         '''insert into "AIRFLOW_DATABASE"."DBT_QUERYPIE"."APP_USERS_WEEKLY"
         SELECT sub.yearweek,min(sub.startdate) as startdate, COUNT(sub.id) AS USERS_CNT,sum(users_cnt) over( order by sub.yearweek rows unbounded preceding)+95 as total_user
        FROM
        (SELECT concat(yearofweek(date(created_at)),to_char(weekofyear(date(created_at)),'09')) as yearweek,a.id,date(a.created_at) as startdate
        FROM airflow_database.QUERYPIE_PRODUCTION_DB.USERS as a
        WHERE a.email NOT LIKE '%chequer.io%'
        AND a.email NOT LIKE '%querypie.com%'
        AND a.email_confirmed = 'Y'
        AND a.withdraw_date IS NULL
        AND a.created_at > '2019-05-20')sub
        GROUP BY 1
        order by 1 desc;''',
         '''Alter warehouse airflow_warehouse resume if suspended;''',
         '''truncate table airflow_database.dbt_querypie.guest_os_type_weekly;''',
         '''insert into airflow_database.dbt_querypie.guest_os_type_weekly
         select sub.yearweek,min(sub.startdate) as startdate
            ,IFNULL(SUM(CASE WHEN sub.os_type = 'Mac' then 1 end), 0) AS Mac
            ,IFNULL(SUM(CASE WHEN sub.os_type = 'Linux' then 1 end), 0) AS Linux
            ,IFNULL(SUM(CASE WHEN sub.os_type = 'Windows' then 1 end), 0) AS Windows
            ,IFNULL(SUM(CASE WHEN sub.os_type is NULL then 1 end), 0) as noinfo
        from
        (select  concat(yearofweek(date(created_at)),to_char(weekofyear(date(created_at)),'09')) as yearweek,a.os_type,date(created_at) as startdate
        from airflow_database.QUERYPIE_PRODUCTION_DB.GET_STARTED as a) sub
        group by 1
        order by 1 desc;''',
         '''Alter warehouse airflow_warehouse resume if suspended;''',
         '''truncate table airflow_database.dbt_querypie.active_user_weekly;''',
         '''insert into airflow_database.dbt_querypie.active_user_weekly
		select sub.yearweek,min(date(a.action_at)) as startdate,sub.mac,sub.linux,sub.windows,sub.noinfo,sub.grand_total
		from
		(
		select b.yearweek
					,IFNULL(SUM(CASE WHEN b.device_os = 'Mac' then 1 end), 0) AS Mac
					,IFNULL(SUM(CASE WHEN b.device_os = 'Linux' then 1 end), 0) AS Linux
					,IFNULL(SUM(CASE WHEN b.device_os = 'Windows' then 1 end), 0) AS Windows
					,IFNULL(SUM(CASE WHEN b.device_os is NULL then 1 end), 0) AS noinfo
					,count(distinct(b.user_id)) as grand_total
			from
			(select distinct(user_id),concat(yearofweek(date(action_at)),to_char(weekofyear(date(action_at)),'09')) as yearweek
					,a.device_os
				from "AIRFLOW_DATABASE".QUERYPIE_PRODUCTION_DB.WORKSPACE_SUB_ACTION as a
				where left(a.action_at,7)>'2020-05'
				and action_type in ('GET_SUBSCRIPTION','GET_WORKSPACE','CONNECT','GET_COMMENT')) b
				group by 1
				order by 1 desc) sub
		join  "AIRFLOW_DATABASE".QUERYPIE_PRODUCTION_DB.WORKSPACE_SUB_ACTION as a on concat(yearofweek(date(action_at)),to_char(weekofyear(date(action_at)),'09')) = sub.yearweek
		group by 1,3,4,5,6,7
		order by 1 desc;
		''',
         '''Alter warehouse airflow_warehouse resume if suspended;''',
         '''truncate table "AIRFLOW_DATABASE"."DBT_QUERYPIE"."RAW_DATA_WEEKLY";''',
         '''insert into "AIRFLOW_DATABASE"."DBT_QUERYPIE"."RAW_DATA_WEEKLY"

            select active_user_weekly.yearweek,active_user_weekly.startdate
             ,app_download_weekly.TOTAL_DOWNLOAD as TOTAL_DOWNLOAD
             ,app_download_weekly.LINUX_DOWNLOAD as LINUX_DOWNLOAD
             ,app_download_weekly.MAC_DOWNLOAD as MAC_DOWNLOAD
             ,app_download_weekly.WINDOWS_DOWNLOAD as WINDOWS_DOWNLOAD
             ,app_guest_weekly.COUNT as GUEST
             ,guest_OS_TYPE_weekly.MAC as guest_MAC
             ,guest_OS_TYPE_weekly.LINUX as guest_LINUX
             ,guest_OS_TYPE_weekly.WINDOWS as guest_WINDOWS
             ,guest_OS_TYPE_weekly.NOINFO as guest_NOINFO

             ,app_users_weekly.USERS_CNT as NEW_USER
             ,app_users_weekly.total_user as total_user

             ,active_user_weekly.grand_total as active_user
             ,active_user_weekly.mac as mac
             ,active_user_weekly.linux as linux
             ,active_user_weekly.windows as windows
             ,active_user_weekly.noinfo as noinfo

         from airflow_database.dbt_querypie.active_user_weekly
             join "AIRFLOW_DATABASE"."DBT_QUERYPIE".app_download_weekly on active_user_weekly.yearweek = app_download_weekly.yearweek
             join "AIRFLOW_DATABASE"."DBT_QUERYPIE".app_guest_weekly on app_download_weekly.yearweek = app_guest_weekly.yearweek
             join "AIRFLOW_DATABASE"."DBT_QUERYPIE".app_users_weekly on app_guest_weekly.yearweek = app_users_weekly.yearweek
             join airflow_database.dbt_querypie.guest_OS_TYPE_weekly on guest_OS_TYPE_weekly.yearweek=active_user_weekly.yearweek
         order by 1 desc;
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