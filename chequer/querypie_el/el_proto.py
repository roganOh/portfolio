# mysql : ` snowflake : "
# -*- coding:utf-8 -*-

"""Library for get users information.

Execute this file for get/give information!
We must fix something but this is proto file for get informations.
This file is for QueryPie_ELT. So UI must be exist front of this file.
After this code runs, a python file for elt will be made.

:param owner: a person who made the job. If user didn't enter this info, default will be chequer
:param start_date: start date of user's ELT. it must be like YYYY,m,d and Library will change like (YYYY,m,d)
:param table: tables that user wants to make ELT
:rtype: str | must be like a,b,c,d. As like database,schema
:rtype database,schema,merge rule: length must be same as lenth of table

:param upsert: ELT rule for each table. user can choose only in truncate,merge,increasement.
:rtype: str
:param column: columns that user wnats to make ELT in each table.
:rtype: list | must be like ['a','b,c,d,e,f','1','chquer_columns']

"""

import datetime as date
import os
import subprocess
from datetime import datetime

import get_sql_alchemy_conn
import pandas as pd
import sqlalchemy as sql
from make_a_dag import make_a_dag


def is_job_exists(job_name, engine):
    """
    check if job exists

    :return: 1 when exists and programm will make user to write his job name again & 0 when not exsits
    """
    try:
        before_job = pd.read_sql_query(
            "select job_name from info where job_name='{job_name}' and info_type='load' limit 1".format(
                job_name=job_name), engine
        )['dag_id'][0]
    except:  # job is not exist
        """ job must no exsist so this direction is correct direction. And so, when in this case, code does nothing."""
        _ = None
        return 0
    else:  # job is exist
        print('job_name: ' + before_job + '  is exists. job name must be primary key. please write other job name')
        return 1


yesterday = date.date.today() - date.timedelta(1)
backend = ''
#get backend ulr from airflow.cfg
try:
    a = subprocess.check_output("echo $HOME", shell=True, )
    a = a.decode('utf-8').replace("\n", '')
    airflow_home = a + '/airflow'
    backend = get_sql_alchemy_conn.get_sql_alchemy_conn(airflow_home)
except:
    _ = backend
engine = sql.create_engine(backend)
print("Welcome rogan's ELT TOOL\nplease make inputs first for your ELT")
print("you should not contain space. You can only use english and _ ")
dag_id = input("job name : ")
while (' ' in dag_id):
    print("you should not contain space. You can only use english and _ ")
    dag_id = input("job name : ")
while (is_job_exists(dag_id, engine)):
    dag_id = input("job name : ")
    while (' ' in dag_id):
        print("you should not contain space. You can only use english and _ ")
        dag_id = input("job name : ")

owner = input("owner : ")
if not owner:
    owner='chquer'
#make datetime of yesterday's date
start_date = '({year},{month},{day})'.format(year=yesterday.year, month=yesterday.month, day=yesterday.day)
#if {airflow_home}/csv_files doesn't exist, make dir
if not os.path.exists(airflow_home + '/csv_files'):
    os.makedirs(airflow_home + '/csv_files')
directory = os.path.abspath(airflow_home + '/csv_files')
catchup = 'False'
print("schedule_interval :you must write on cron like * * * * *")
schedule_interval = input('schedule_interval : ')
if not (schedule_interval.replace(" ", "")):
    #if schedule_interval is ''
    schedule_interval = '@once'
print("integreate_db_type :you can choose in mysql postgresql snowflake redshift ")
integrate_db_type = input("integreate_db_type : ")
while integrate_db_type not in ('mysql', 'postgresql', 'snowflake', 'redshift'):
    #user must push in mysql,postgresql,snowflake,redshift
    print("not valuable db")
    print("integreate_db_type :you can choose in mysql postgresql snowflake redshift")
    integrate_db_type = input("integreate_db_type : ")
print('db login id :you can only write with english and _')
id = input('db login id : ')
#id must do not have space
while (' ' in id):
    print("you can only write with english and _")
    id = input('db login id : ')
pwd = input('db login password :')
while (' ' in pwd):
    #pwd must do not have space
    print("you can only write wiith english and _")
    pwd = input('db login password :')
tables = input("tables : ")
while (id == '' or pwd == ''):
    print('you should write both id and password')
    id = input('db login id : ')
    pwd = input('db login password :')
while not (tables):
    #user must input tables
    print("you should select tables")
    tables = input("tables : ")
tables = tables.split(',')  # table 이름 중간에 , 가 들어간다면??
#make pk,update (list)
pk = [''] * len(tables)
updated = [''] * len(tables)
print('rule : you can choose in truncate, increasement, merge')
upsert = input("rule : ")
upsert = upsert.replace(" ", "")
upsert = upsert.split(',')
while (len(tables) != len(upsert)):
    #len(tablees) and len(upsert) must be same
    print('you should write all rules for each table')
    print('rule : you can choose in truncate, increasement, merge')
    upsert = input("rule : ")
    upsert = upsert.replace(" ", "")
    upsert = upsert.split(',')
for i in range(len(upsert)):
    rule = upsert[i]
    for j in range(len(upsert)):
        while upsert[j] not in ('truncate', 'increasement', 'merge'):
            #upsert must be among truncate,increasement,merge
            #if not, user must rewrite corresponding upsert
            print('you can choose only in truncate, increasement, merge')
            print('your elt rule : ' + str(upsert))
            print('error rule : ' + upsert[j])
            upsert[j] = input("rule : ")
    if rule == 'increasement':
        #if rule == increasement, get pk
        print('table: ' + tables[i] + ', rule : ' + rule)
        pk[i] = input("increase column : ")
        while not (pk[i]):
            print('you should write pk')
            pk[i] = input('increase key : ')
    elif rule == 'merge':
        # if rule == merge, get pk,updated_column
        print('table: ' + tables[i] + ', rule : ' + rule)
        pk[i] = input('primary key : ')
        updated[i] = input('updated_at column : ')
        while not (pk[i]) or not (updated[i]):
            print('you should write both of them')
            pk[i] = input('primary key : ')
            updated[i] = input('updated_at column : ')
    else:
        # if rule ==truncate do not get anything
        pk[i] = ''
print("columns : if you want to choose all *, just press enter")
columns = input('columns(list) : ')
# user must write columns above those are for front end and test
if not (columns):
    columns = ['*'] * len(tables)
else:
    #columns must be list like ['a','b','c'] but type is str
    while (columns[0] != '[' or columns[1] != "'" or columns[-1] != ']' or columns[-2] != "'"):
        print('you should just press enter or input list of string')
        columns = input('columns : ')
        if not (columns):
            columns = ['*'] * len(tables)
            break
    # make list columns to str
    columns = columns.replace('[', '')
    columns = columns.replace(']', '')
    # after replace [], make ', to & (', works as split tuples
    columns = columns.replace("',", "&")
    # so split columns with &
    columns = columns.split('&')
    for i in range(len(tables)):
        if not (columns[i]):
            # for unknown exception
            columns[i] = '*'
        columns[i] = columns[i].replace("'", "")
print(columns)
schema = ''
dag_ids = [dag_id] * len(tables)
# default status are all on
# status works as if this dag on or off
status = ['on'] * len(tables)
option = '?charset=utf8'

if integrate_db_type == 'mysql':
    host = input('host : ')
    port = input('port : ')
    database = input('database : ')
    database = database.split(',')
    while len(database) != len(tables):
        database = input('database : ')
        database = database.split(',')
    while (host == '' or port == '' or database == ''):
        print("write everything")
        host = input('host : ')
        port = input('port : ')
        database = input('database : ')
        database = database.split(',')
        while len(database) != len(tables):
            database = input('database : ')
            database = database.split(',')

    raw_data = {'dag_id': [dag_id],
                'owner': [owner],
                'directory': [directory],
                'start_date': [start_date],
                'catchup': [catchup],
                'schedule_interval': [schedule_interval],
                'db_type': [integrate_db_type],
                'db_information': [
                    """{{'id': '{id}', 'pwd': '{pwd}', 'host': '{host}', 'port': '{port}' 'option': '{option}'}}
                    """.format(id=id, pwd=pwd, host=host, port=port, option=option)],
                'info_type': ['extract']}

    data1 = pd.DataFrame(raw_data)

elif integrate_db_type in ('redshift', 'postgresql'):
    host = input('host : ')
    port = input('port : ')
    schema = input('schema : ')
    database = input('database : ')
    database = database.split(',')
    while len(database) != len(tables):
        database = input('database : ')
        database = database.split(',')
    while (host == '' or port == '' or database == ''):
        print("write everything")
        host = input('host : ')
        port = input('port : ')
        database = input('database : ')
        database = database.split(',')
        while len(database) != len(tables):
            database = input('database : ')
            database = database.split(',')

        schema = input('schema : ')
        schema = schema.split(',')
        while len(schema) != len(schema):
            schema = input('schema : ')
            schema = schema.split(',')

    raw_data = {'dag_id': [dag_id],
                'owner': [owner],
                'directory': [directory],
                'start_date': [start_date],
                'catchup': [catchup],
                'schedule_interval': [schedule_interval],
                'db_type': [integrate_db_type],
                'db_information': [
                    """{{'id': '{id}', 'pwd': '{pwd}', 'host': '{host}', 'port': '{port}',  'option': '{option}' }}
                    """.format(id=id, pwd=pwd, host=host, port=port, option=option)],
                'info_type': ['extract']}

    data1 = pd.DataFrame(raw_data)


elif integrate_db_type == 'snowflake':
    account = input('account(host except snowflakecomputing.com) : ')
    database = input('database : ')
    database = database.split(',')
    while len(database) != len(tables):
        database = input('database : ')
        database = database.split(',')
    schema = input('schema : ')
    schema = schema.split(',')
    while len(schema) != len(schema):
        schema = input('schema : ')
        schema = schema.split(',')
    warehouse = input('warehouse : ')
    while (account == '' or database == '' or schema == '' or warehouse == ''):
        print("write everything")
        account = input('account : ')
        database = input('database : ')
        database = database.split(',')
        while len(database) != len(tables):
            database = input('database : ')
            database = database.split(',')
        schema = input('schema : ')
        schema = schema.split(',')
        while len(schema) != len(schema):
            schema = input('schema : ')
            schema = schema.split(',')
        warehouse = input('warehouse : ')

    print('role :\nyou can just skip with press enter')
    role = input()
    if not role: role = ''
    raw_data = {'dag_id': [dag_id],
                'owner': [owner],
                'directory': [directory],
                'start_date': [start_date],
                'catchup': [catchup],
                'schedule_interval': [schedule_interval],
                'db_type': [integrate_db_type],
                'db_information': [
                    """{{'id': '{id}', 'pwd': '{pwd}', 'account': '{account}', 'warehouse': '{warehouse}', 'role': '{role}'}}
                    """.format(id=id, pwd=pwd, account=account, warehouse=warehouse, role=role)],
                'info_type': ['extract']}

    data1 = pd.DataFrame(raw_data)
# make dataframe with tables to replicate
tables_to_replicate_raw_data = {'dag_id': dag_ids, 'database': database, 'schema': schema, 'tables': tables,
                                'columns': columns,
                                'replicate_rule': upsert, 'pk': pk, 'updated_column': updated, 'status': status}
tables_to_replicate = pd.DataFrame(tables_to_replicate_raw_data)


print('destination_db_type :\nyou can choose db type in mysql, snowflake, postgresql, redshift')
destination_ds = input()
while destination_ds not in ('mysql', 'snowflake', 'postgresql', 'redshift'):
    print('not valuable db type')
    destination_ds = input('destination_db_type : ')
id_ds = input('login id : ')
pwd_ds = input('login_password : ')
while (id_ds == '' or pwd_ds == ''):
    print("write everything")
    id_ds = input('id : ')
    pwd_ds = input('pwd : ')

option_ds = '?charset=utf8'

if destination_ds == 'mysql':
    host_ds = input('host : ')
    port_ds = input('port : ')
    database_ds = input('database : ')
    while (host_ds == '' or port_ds == '' or database_ds == ''):
        print("write everything")
        host_ds = input('host : ')
        port_ds = input('port : ')
        database_ds = input('database : ')

    raw_data = {'dag_id': [dag_id],
                'owner': [owner],
                'directory': [directory],
                'start_date': [start_date],
                'catchup': [catchup],
                'schedule_interval': [schedule_interval],
                'db_type': [destination_ds],
                'db_information': [
                    """{{'id': '{id}', 'pwd': '{pwd}', 'host': '{host}', 'port': '{port}', 'database': '{database}',
'option': '{option}', 'tables': "{tables}", 'columns': "{columns}"}}
""".format(id=id_ds, pwd=pwd_ds, host=host_ds, port=port_ds, database=database_ds, option=option_ds)],
                'info_type': ['load']}

    data = pd.DataFrame(raw_data)

elif destination_ds == 'snowflake':
    account_ds = input('account(host except snowflakecomputing.com) : ')
    database_ds = input('database : ')
    schema_ds = input('schema : ')
    warehouse_ds = input('warehouse : ')
    while (account_ds == '' or database_ds == '' or schema_ds == '' or warehouse_ds == ''):
        print("write everything")
        account_ds = input('account : ')
        database_ds = input('database : ')
        schema_ds = input('schema : ')
        warehouse_ds = input('warehouse : ')
    print('role :\nyou can just skip with press enter')
    role_ds = input('role: ')

    raw_data = {'dag_id': [dag_id],
                'owner': [owner],
                'directory': [directory],
                'start_date': [start_date],
                'catchup': [catchup],
                'schedule_interval': [schedule_interval],
                'db_type': [destination_ds],
                'db_information': [
                    """{{'id': '{id}', 'pwd': '{pwd}', 'account': '{account}', 'warehouse': '{warehouse}', 'database': '{database}',
'schema':'{schema}', 'role': "{role}"}}""".format(id=id_ds, pwd=pwd_ds, account=account_ds, warehouse=warehouse_ds,
                                          database=database_ds, schema=schema_ds, role=role_ds)],
                'info_type': ['load']}

    data = pd.DataFrame(raw_data)

elif destination_ds in ('redshift', 'postgresql'):
    host_ds = input('host : ')
    port_ds = input('port : ')
    database_ds = input('database : ')
    schema_ds = input('schema : ')
    while (host_ds == '' or database_ds == '' or schema_ds == '' or host_ds == ''):
        print("write everything")
        host_ds = input('host : ')
        port_ds = input('port : ')
        database_ds = input('database : ')
        schema_ds = input('schema : ')

    raw_data = {'dag_id': [dag_id],
                'owner': [owner],
                'directory': [directory],
                'start_date': [start_date],
                'catchup': [catchup],
                'schedule_interval': [schedule_interval],
                'db_type': [destination_ds],
                'db_information': [
                    """{{'id': '{id}', 'pwd': '{pwd}', 'host': '{host}', 'port': '{port}', 'database': '{database}', 
'schema':'{schema}', 'option': '{option}'}}""".format(id=id_ds, pwd=pwd_ds, host=host_ds, port=port_ds,
                                                      database=database_ds, schema=schema_ds, option=option_ds)],
                'info_type': ['load']}

    data = pd.DataFrame(raw_data)
# data1 is data of extraction
# data is data of load
# tables_to_replicate is data of tables that user will extract
data1.to_sql('el_metadata', engine, if_exists='append', index=False)
data.to_sql('el_metadata', engine, if_exists='append', index=False)
tables_to_replicate.to_sql('metadata_tables_to_replicate', engine, if_exists='append', index=False)

make_a_dag(ex_db_type=integrate_db_type, ld_db_type=destination_ds, file_name=dag_id + '.py', file_dir='./',
           dag_id=dag_id)
