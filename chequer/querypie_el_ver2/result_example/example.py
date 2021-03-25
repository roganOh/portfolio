# -*- coding:utf-8 -*-

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sqlalchemy as sql
import pandas as pd
import json

from querypie_el.hook import do_extract
from querypie_el.hook import do_load

backend_engine = sql.create_engine('{your backend, it will written automatically}')
ex = pd.read_sql_query(
        "select * from metadata where dag_id='test1' and info_type='extract'",
        backend_engine)
ld = pd.read_sql_query(
        "select * from metadata where dag_id='test1' and info_type='load'",
        backend_engine)
ld_db_information = ld['db_information'][0].replace("'", '"')
for i in range(len(ld_db_information)):
    if ld_db_information[i] == '[':
        i = i + 1
        while (ld_db_information[i] != ']'):
            if ld_db_information[i] == '"':
                ld_db_information = ld_db_information[:i] + "'" + ld_db_information[i + 1:]
            i = i + 1
ld_db_information = json.loads(ld_db_information)
ex_db_information = ex['db_information'][0].replace("'", '"')
for i in range(len(ex_db_information)):
    if ex_db_information[i] == '[':
        i = i + 1
        while (ex_db_information[i] != ']'):
            if ex_db_information[i] == '"':
                ex_db_information = ex_db_information[:i] + "'" + ex_db_information[i + 1:]
            i = i + 1
ex_db_information = json.loads(ex_db_information)

default_args = {
    'owner': ex['owner'][0],
    'depends_on_past': False,
    'start_date': datetime.strptime(ex['start_date'][0],"(%Y,%m,%d)"),
}

dag = DAG(
    dag_id= ex['dag_id'][0],
    default_args=default_args,
    catchup=ex['catchup'][0],
    schedule_interval=ex['schedule_interval'][0])

tables = ex_db_information['tables']

get_data = PythonOperator(
             task_id='get_data',
             python_callable=getattr(do_extract,ex['db_type'][0]),
             op_kwargs = {'id': ex_db_information['id'],
                'pwd': ex_db_information['pwd'],
                'host': ex_db_information['host'],
                'port': ex_db_information['port'],
                'database': ex_db_information['database'],
                'directory': ex['directory'][0],
                'tables': tables,
                'option': ex_db_information['option'],
                'columns': ex_db_information['columns'],
                'pk': ex_db_information['pk'],
                'upsert':ex_db_information['elt_rule'],
                'dag_id':ex['dag_id'][0],
                'updated':ex_db_information['updated_column']},
             dag=dag
         )
do_load = PythonOperator(
            task_id='do_load',
            python_callable=getattr(do_load,ld['db_type'][0]),
            op_kwargs = {'id': ld_db_information['id'],
                'pwd': ld_db_information['pwd'],
                'account': ld_db_information['account'],
                'database': ld_db_information['database'],
                'schema': ld_db_information['schema'],
                'warehouse': ld_db_information['warehouse'],
                'tables': tables,
                'directory' : ld['directory'][0],
                'role' : "AIRFLOW_ROLE",
                'dag_id': "test1",
                'columns' : ld_db_information['columns'],
                'pk': ld_db_information['pk'],
                'upsert' : ld_db_information['elt_rule'],
                'updated' : ld_db_information['updated_column']},
            dag=dag)


get_data >> do_load

if __name__ == '__main__':
  dag.clear(reset_dag_runs=True)
  dag.run()