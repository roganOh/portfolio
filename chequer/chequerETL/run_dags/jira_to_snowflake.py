# -*- coding:utf-8 -*-
from datetime import datetime

import pandas as pd
import sqlalchemy as sql
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from get_csv_files import get_jira_data

default_args = {
    'owner': 'CHEQUER',
    'depends_on_past': False,
    'start_date': datetime(2021, 2, 16),
}

dag = DAG(
    dag_id='jira_to_snowflake_ver1',
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
# tables = ['project', 'project_type', 'resolution', 'role', 'users', 'issues', 'issue_dates',
#           'project_versions_10000', 'project_versions_10207', 'project_versions_10307', 'project_versions_10400',
#           'project_versions_10700', 'project_versions_10901']
tables = ['project', 'project_type', 'resolution', 'role', 'users', 'issues', 'issue_dates']
admin = ''
pwd = ''
jira_server = ""
access_token = ''
id = 'airflow'
pwd_jira = ''
account = ''
role = 'AIRFLOW_ROLE'
database = 'AIRFLOW_DATABASE'
warehouse = 'AIRFLOW_WAREHOUSE'
schema = 'jira'

engine = sql.create_engine(
    'snowflake://{u}:{p}@{a}/{d}/{s}?warehouse={w}&role={r}'.format(
        u=id,
        p=pwd_jira,
        a=account,
        r=role,
        d=database.lower(),
        w=warehouse,
        s=schema
    )
)

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
    python_callable=get_jira_data.get_data,
    op_kwargs={'admin': admin, 'pwd': pwd, 'jira_server': jira_server, 'access_token': access_token},
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

finish = DummyOperator(
    task_id='finish',
    trigger_rule='none_failed',
    dag=dag)
for i in range(0, len(tables)):
    args = tables[i]
    result = pd.read_csv('/home/ec2-user/airflow/csv_files/jira/' + args + '.tsv', sep=',', quotechar="'")
    result.head(0).to_sql(args, engine, if_exists='replace', index=False)
    query = [
        'Alter warehouse airflow_warehouse resume if suspended;',
        'put file:///home/ec2-user/airflow/csv_files/jira/' + args + '.tsv @jira.%' + args + ';',
        'Alter warehouse airflow_warehouse resume if suspended;',
        '''copy into jira.''' + args + ''' from @jira.%''' + args + '''
            file_format=(type="csv" FIELD_OPTIONALLY_ENCLOSED_BY ="'" SKIP_HEADER=1  );''']
    do_tl2 = SnowflakeOperator(
        task_id='tl_' + args + '2',
        snowflake_conn_id='snowflake_chequer',
        sql=query,
        autocommit=True,
        dag=dag
    )

    start_tl >> do_tl2 >> end_copy
    get_data >> start_tl

end_copy >> end_job >> finish

if __name__ == '__main__':
    dag.clear(reset_dag_runs=True)
    dag.run()
