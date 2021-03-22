# -*- coding:utf-8 -*-

"""Library for make a dag

This Library is execute after el_proto.py LIbrary

Available functions:
- make_a_dag: this function is the only function in this file. It makes a dag with users information
              along db type, make args whose name is get_data and do_load
"""
import subprocess

from get_information_from_user import get_sql_alchemy_conn

# find ~ first
a = subprocess.check_output("echo $HOME", shell=True, )
a = a.decode('utf-8').replace("\n", '')
# cause I made install airflow at ~ airflow home must be ~/airflow
airflow_home = a + '/airflow'
backend = get_sql_alchemy_conn.get_sql_alchemy_conn(airflow_home)


def make_a_dag(ex_db_type, ld_db_type, file_name, file_dir, dag_id):
    """
    :param file_name: file_name is same with dag_id.
    :rtype file_name: str
    :param file_dir: dir for where a dag which this func makes will save at.
    :rtype file_dir: str
    :param dag_id: name for a dag and information & destination file.
    :rtype dag_id: str
    :return: a dag with users information

    :param ex: metadata that users' information about integration db (extraction)
    :rtype: dict
    :param ld: metadata that users' information about destination db (load)
    :rtype: dict
    :param tr: metadata that tables to replicate of user (tables to replicate)
    :rtype: dict
    """

    if (ex_db_type == 'snowflake'):
        get_data = '''PythonOperator(
            task_id='get_data',
            python_callable=getattr(do_extract,{integrate_db_type}),
            op_kwargs = {{'id': {id},
                'pwd': {pwd},
                'account': {account},
                'database': {database},
                'schema': {schema},
                'warehouse': {warehouse},
                'tables': tables,
                'directory' : {directory},
                'role' : {role},
                'columns': {columns},
                'pk': {pk},
                'upsert' : {upsert},
                'dag_id' : {dag_id},
                'updated' : {updated}}},
            dag=dag
        )'''.format(integrate_db_type="ex['db_type'][0]",
                    id="ex_db_information['id']",
                    pwd="ex_db_information['pwd']",
                    account="ex_db_information['account']",
                    database="tr['database'][0]",
                    schema="tr['schema'][0]",
                    warehouse="ex_db_information['warehouse']",
                    directory="ex['directory'][0]",
                    role="ex_db_information['role']",
                    columns="tr['columns'][0]",
                    pk="tr['pk'][0]",
                    upsert="tr['upsert'][0]",
                    dag_id="tr['dag_id'][0]",
                    updated="tr['updated'][0]"
                    )
    elif (ex_db_type == 'mysql'):
        get_data = '''PythonOperator(
             task_id='get_data',
             python_callable=getattr(do_extract,{integrate_db_type}),
             op_kwargs = {{'id': {id},
                'pwd': {pwd},
                'host': {host},
                'port': {port},
                'database': {database},
                'directory': {directory},
                'tables': tables,
                'option': {option}+'mb4',
                'columns': {columns},
                'pk': {pk},
                'upsert' : {upsert},
                'dag_id' : {dag_id},
                'updated' : {updated}}},
             dag=dag
         )'''.format(integrate_db_type="ex['db_type'][0]",
                     id="ex_db_information['id']",
                     pwd="ex_db_information['pwd']",
                     host="ex_db_information['host']",
                     port="ex_db_information['port']",
                     database="tr['database'][0]",
                     directory="ex['directory'][0]",
                     option="ex_db_information['option']",
                     columns="tr['columns'][0]",
                     pk="tr['pk'][0]",
                     upsert="tr['upsert'][0]",
                     dag_id="tr['dag_id'][0]",
                     updated="tr['updated'][0]"
                     )
    elif (ex_db_type in ('redshift','postgresql')):
        get_data = '''PythonOperator(
             task_id='get_data',
             python_callable=getattr(do_extract,{integrate_db_type}),
             op_kwargs = {{'id': {id},
                'pwd': {pwd},
                'host': {host},
                'port': {port},
                'database': {database},
                'schema': {schema},
                'directory': {directory},
                'tables': tables,
                'columns': {columns},
                'pk': {pk},
                'upsert' : {upsert},
                'dag_id' : {dag_id},
                'updated' : {updated}}},
             dag=dag
         )'''.format(integrate_db_type="ex['db_type'][0]",
                     id="ex_db_information['id']",
                     pwd="ex_db_information['pwd']",
                     host="ex_db_information['host']",
                     port="ex_db_information['port']",
                     database="tr['database'][0]",
                     schema="tr['schema'][0]",
                     directory="ex['directory'][0]",
                     option="ex_db_information['option']",
                     columns="tr['columns'][0]",
                     pk="tr['pk'][0]",
                     upsert="tr['upsert'][0]",
                     dag_id="tr['dag_id'][0]",
                     updated="tr['updated'][0]"
                     )

    if (ld_db_type == 'snowflake'):
        do_load = '''PythonOperator(
            task_id='do_load',
            python_callable=getattr(do_load,{detinate_db_type}),
            op_kwargs = {{'id': {id},
                'pwd': {pwd},
                'account': {account},
                'database': {database},
                'schema': {schema},
                'warehouse': {warehouse},
                'tables': tables,
                'directory' : {directory},
                'role' : {role},
                'columns' : {columns},
                'pk': {pk},
                'upsert' : {upsert},
                'dag_id' : {dag_id},
                'updated' : {updated}}},
            dag=dag)
                 '''.format(detinate_db_type="ld['db_type'][0]",
                            id="ld_db_information['id']",
                            pwd="ld_db_information['pwd']",
                            account="ld_db_information['account']",
                            database="ld_db_information['database']",
                            schema="ld_db_information['schema']",
                            warehouse="ld_db_information['warehouse']",
                            directory="ld['directory'][0]",
                            role="ld_db_information['role']",
                            columns="tr['columns'][0]",
                            pk="tr['pk'][0]",
                            upsert="tr['upsert'][0]",
                            dag_id="tr['dag_id'][0]",
                            updated="tr['updated'][0]"
                            )

    elif (ld_db_type == 'mysql'):
        do_load = '''PythonOperator(
             task_id='do_load',
             python_callable=getattr(do_load,{detinate_db_type}),
             op_kwargs = {{'id': {id},
                'pwd': {pwd},
                'host': {host},
                'port': {port},
                'database': {database},
                'directory': {directory},
                'tables': tables,
                'option': {option}+'mb4',
                'columns' : {columns},
                'pk': {pk},
                'upsert' : {upsert},
                'dag_id' : {dag_id},
                'updated' : {updated}}},
             dag=dag)
            '''.format(detinate_db_type="ld['db_type'][0]",
                       id="ld_db_information['id']",
                       pwd="ld_db_information['pwd']",
                       host="ld_db_information['host']",
                       port="ld_db_information['port']",
                       database="ld_db_information['database']",
                       directory="ld['directory'][0]",
                       option="ld_db_information['option']",
                       columns="tr['columns'][0]",
                       pk="tr['pk'][0]",
                       upsert="tr['upsert'][0]",
                       dag_id="tr['dag_id'][0]",
                       updated="tr['updated'][0]"
                       )

    elif (ex_db_type in ('redshift', 'postgresql')):
        do_load = '''PythonOperator(
             task_id='do_load',
             python_callable=getattr(do_load,{detinate_db_type}),
             op_kwargs = {{'id': {id},
                'pwd': {pwd},
                'host': {host},
                'port': {port},
                'database': {database},
                'schema': {schema},
                'directory': {directory},
                'tables': tables,
                'columns' : {columns},
                'pk': {pk},
                'upsert' : {upsert},
                'dag_id' : {dag_id},
                'updated' : {updated}}},
             dag=dag)
            '''.format(detinate_db_type="ld['db_type'][0]",
                       id="ld_db_information['id']",
                       pwd="ld_db_information['pwd']",
                       host="ld_db_information['host']",
                       port="ld_db_information['port']",
                       database="ld_db_information['database']",
                       schema="ld_db_information['schema']",
                       directory="ld['directory'][0]",
                       option="ld_db_information['option']",
                       columns="tr['columns'][0]",
                       pk="tr['pk'][0]",
                       upsert="tr['upsert'][0]",
                       dag_id="tr['dag_id'][0]",
                       updated="tr['updated'][0]"
                       )

    data = """
# -*- coding:utf-8 -*-

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sqlalchemy as sql
import pandas as pd
import json

from querypie_el.hook import do_extract
from querypie_el.hook import do_load

backend_engine = sql.create_engine('{backend}')
ex = pd.read_sql_query(
        "select * from metadata where dag_id='{dag_id}' and info_type='extract'",
        backend_engine)
ld = pd.read_sql_query(
        "select * from metadata where dag_id='{dag_id}' and info_type='load'",
        backend_engine)
tr = pd.read_sql_query(
        "select * from metadata_tables_to_replicate where dag_id='{dag_id}' and status='on'",
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

default_args = {{
    'owner': ex['owner'][0],
    'depends_on_past': False,
    'start_date': datetime.strptime(ex['start_date'][0],"(%Y,%m,%d)"),
}}

dag = DAG(
    dag_id= ex['dag_id'][0],
    default_args=default_args,
    catchup=(ex['catchup'][0]==True),
    schedule_interval=ex['schedule_interval'][0])

tr_tables=tr['tables']
tables=[]
for item in tr_tables.iteritems():
        tables.append(item[1])

get_data = {extract_task}
do_load = {load_task}

get_data >> do_load

if __name__ == '__main__':
  dag.clear(reset_dag_runs=True)
  dag.run()

""".format(
        dag_id=dag_id,
        backend=backend,
        extract_task=get_data,
        load_task=do_load
    )

    with open(file_dir + '/' + file_name, 'w') as f:
        f.write('{}'.format(data))
