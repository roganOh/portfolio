# -*- coding:utf-8 -*-

from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

default_args = {
    'owner': 'CHEQUER',
    'depends_on_past': False,
    'start_date': datetime(2020, 9, 1),
}

dag = DAG(
    dag_id='sqlgate_query_result_mysql',
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


options = ['upload', 'pass']

check_el_finish = ExternalTaskSensor(
    task_id="check_el_finish",
    external_dag_id='sqlgate_mysql_to_mysql',
    external_task_id='work_done',
    mode="reschedule",
    dag=dag
)
is_oclock = BranchPythonOperator(
    task_id='check_is_oclock',
    provide_context=True,
    python_callable=which_path,
    dag=dag
)

start_query = DummyOperator(
    task_id='start_query',
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
        is_oclock >> t >> start_query
    else:
        is_oclock >> t >> end_job

query = ['''
        truncate table R_SUB_TOTAL_MONTHLY_not_trial
        ''',
         '''
         insert into R_SUB_TOTAL_MONTHLY_not_trial
         SELECT
             DATE_FORMAT(CREATED_AT, '%Y-%m') AS month,
             SUM(QTY) AS quantity
         FROM
             SUB_M
         WHERE
             DATE_FORMAT(CREATED_AT, '%Y') = 2020
             AND TYPE != 'TRIAL'
             AND CANCEL_YN != 'Y'
             AND STATUS = 'active'
         GROUP BY month
         ORDER BY month;
         ''',
         '''
         truncate table R_SUB_STANDARD_MONTHLY 
         ''',
         '''
         insert into R_SUB_STANDARD_MONTHLY
         SELECT
             DATE_FORMAT(CREATED_AT, '%Y-%m') AS month,
             SUM(QTY) AS quantity
         FROM
             SUB_M
         WHERE
             DATE_FORMAT(CREATED_AT, '%Y') = 2020
             AND plan = 'standard'
             AND TYPE = 'PAID'
             AND CANCEL_YN != 'Y'
             AND STATUS = 'active'
             
         GROUP BY month
         ORDER BY month;
         ''',
         '''
         truncate table R_SUB_EDU_MONTHLY 
         ''',
         '''
         insert into R_SUB_EDU_MONTHLY 
         SELECT
             DATE_FORMAT(CREATED_AT, '%Y-%m') AS month,
             SUM(QTY) AS quantity
         FROM
             SUB_M
         WHERE
             DATE_FORMAT(CREATED_AT, '%Y') = 2020
             AND plan = 'education'
             AND CANCEL_YN != 'Y'
             AND STATUS = 'active'
         GROUP BY month
         ORDER BY month;
         ''',
         '''
         truncate table R_SUB_GITHUB_MONTHLY 
         ''',
         '''         
         insert into R_SUB_GITHUB_MONTHLY
         SELECT
             DATE_FORMAT(SUB_M.CREATED_AT, '%Y-%m') AS month,
             SUM(QTY) AS quantity
         FROM
             SUB_M
             JOIN SUB_EDUCATION_M ON SUB_M.ID = SUB_EDUCATION_M.SUB_ID
                 AND SUB_EDUCATION_M.DEL_YN != 'Y'
                 AND SUB_EDUCATION_M.STATUS = 'ACCEPT'
                 AND SUB_EDUCATION_M.PLAN_TYPE LIKE 'GITHUB_%'
         WHERE
             DATE_FORMAT(SUB_M.CREATED_AT, '%Y') = 2020
             AND SUB_M.plan = 'education'
             AND SUB_M.CANCEL_YN != 'Y'
             AND SUB_M.STATUS = 'active'
         GROUP BY
             DATE_FORMAT(SUB_M.CREATED_AT, '%Y-%m');
         ''',
         '''
         truncate table R_SUB_FREE_MONTHLY_not_trial 
         ''',
         '''
         insert into R_SUB_FREE_MONTHLY_not_trial
         SELECT
             DATE_FORMAT(CREATED_AT, '%Y-%m') AS month,
             SUM(QTY) AS quantity
         FROM
             SUB_M
         WHERE
             DATE_FORMAT(CREATED_AT, '%Y') = 2020
             AND plan = 'free'
             AND CANCEL_YN != 'Y'
             AND STATUS = 'active'
         GROUP BY
             DATE_FORMAT(CREATED_AT, '%Y-%m');
          ''',
         '''
         truncate table R_SALES_AMONUT_MONTHLY
         ''',
         '''
         insert into R_SALES_AMONUT_MONTHLY
         SELECT
            DATE_FORMAT(ORDER_DATE, '%Y-%m') AS month,
            SUM(KRW_SALE_TOT) AS price
        FROM
            ORDER_M
        WHERE
            ORDER_FROM = 'ONLINE'
            AND DATE_FORMAT(ORDER_DATE, '%Y') = 2020
            AND PAY_STATUS = 200
        GROUP BY month
        ORDER BY month;''',
         '''
          truncate table R_DISTRIBUTOR_SALES_AMOUNT_MONTHLY
          ''',
         '''
        insert into R_DISTRIBUTOR_SALES_AMOUNT_MONTHLY
        SELECT
            DATE_FORMAT(ORDER_DATE, '%Y-%m') AS month,
            SUM(TOTAL_DISTRIBUTOR_AMT) AS price
        FROM
            ORDER_M
        WHERE
            ORDER_FROM = 'PARTNER'
            AND DATE_FORMAT(ORDER_DATE, '%Y') = 2020
            AND PAY_STATUS = 200
        GROUP BY month
        ORDER BY month;
        ''',
         '''
         truncate table R_SALES_PROJECTION_MONTHLY
         ''',
         '''
         insert into R_SALES_PROJECTION_MONTHLY
         SELECT
             DATE_FORMAT(ORDER_DATE, '%Y-%m') AS month,
             SUM(KRW_SALE_TOT) AS price
         FROM
             ORDER_M
         WHERE
             ORDER_FROM IN('ONLINE', 'PARTNER')
             AND DATE_FORMAT(ORDER_DATE, '%Y') = 2020
             AND PAY_STATUS = 200
         GROUP BY month
         ORDER BY month;
         ''']
run_query = MySqlOperator(
    task_id='run_query',
    mysql_conn_id='SQLGATE_ETLS',
    sql=query,
    autocommit=True,
    dag=dag
)
check_el_finish >> is_oclock
start_query >> run_query >> end_job
