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

:param upsert_rule: ELT rule for each table. user can choose only in truncate,merge,increasement.
:rtype: str
:param column: columns that user wnats to make ELT in each table.
:rtype: list | must be like ['a','b,c,d,e,f','1','chquer_columns']

"""

import datetime as date
import os
import subprocess
from dataclasses import dataclass
from enum import Enum

import get_sql_alchemy_conn
import pandas as pd
import sqlalchemy as sql
from make_a_dag import make_a_dag


@dataclass
class dag_info:
    yesterday: date = date.date.today() - date.timedelta(1)
    airflow_home: str = None
    backend_url: str = ''
    dag_id: str = None
    owner: str = 'chequer'
    start_date: str = None
    catchup = 'False'
    schedule_interval: str = '@once'
    csv_files_directory: str = None


@dataclass
class db_info:
    db_type: int = None
    host: str = None
    port: str = None
    account: str = None
    id: str = None
    pwd: str = None
    warehouse: str = None
    option: str = None
    role: str = None


@dataclass
class user_data_carrier:
    columns: list = None
    pk: list = None
    updated: list = None
    schema: list = None
    dag_ids: list = None
    status: list = None
    upsert_rule: list = None
    tables: list = None
    status: list = None
    database: list = None
    schema: list = None


class db(Enum):
    mysql = 0
    snowflake = 1
    redshift = 2
    postgresql = 3


class error_message(Enum):

    def about_pep8(self): print("you should not contain space. You can only use english and _.")

    def unuseable_dag_id(self):
        error_message.about_pep8()
        print("Or this id is exist")

    def unuseable_db_type(self): print("not valuable db")

    def no_table(self): print("you should select tables")

    def len_of_rules_are_not_correct_with_len_of_tables(self): print('you should write all rules for each table')

    def columns_type_is_not_list(self): print('you should just press enter or input list of string')

    def wrong_upsert_rule(self): print('you can choose only in truncate, increasement, merge')

    def no_pk(self): print('you should write pk')

    def no_updated_at(self): print('you should write updated_at column')

    def len_of_tables_and_len_of_database_are_not_correct(self): print(
        "len of tables must be same with number of databases")

    def no_value(thing): print("you should write {thing}".format(thing))

    def dag_id_exists(name):
        print('job_name: {name} is exist. job name must be primary key. please write other job name'.foramt(name))

    def write_everything(self): print("write everything")


class about_querypie_elt(Enum):
    welcome = "Welcome rogan's ELT TOOL\nplease make inputs first for your ELT"
    about_dag_id = "Welcome rogan's ELT TOOL\nplease make inputs first for your ELT"
    about_schedule_interval = "schedule_interval :you must write on cron like * * * * *"
    about_db_type = "integreatedb_type :you can choose in mysql postgresql snowflake redshift "
    about_upsert_rule = "upsert_rule : you can choose in truncate, increasement, merge"
    about_columns = "columns must be like ['a,b,c,d','zz','123123,22']"
    skip_choose_columns = "columns : if you want to choose all *, just press enter"
    db_range = ['mysql', 'postgresql', 'snowflake', 'redshift']
    upsert_rule_range = ['truncate', 'increasement', 'merge']
    start_get_destination = 'destination_db_type :\nyou can choose db type in mysql, snowflake, postgresql, redshift'


def is_in_db_range(thing):
    return thing in about_querypie_elt.db_range


def is_in_upsert_rule_range(thing):
    return thing in about_querypie_elt.upsert_rule_range


def is_job_exists(job_name, engine):
    """
    check if job exists

    :return: 1 when exists and programm will make user to write his job name again & 0 when not exsits
    """
    try:
        _ = pd.read_sql_query(
            "select job_name from info where job_name='{job_name}' and info_type='load' limit 1".format(
                job_name=job_name), engine
        )['dag_id'][0]
    except:  # job is not exist
        """ job must not exsist so this direction is correct direction. And so, when in this case, code does nothing."""
        return False
    else:  # job is exist
        error_message.dag_id_exists(job_name)
        return True


def print_programe_start_comment():
    print(about_querypie_elt.welcome)
    print(about_querypie_elt.about_dag_id)


def get_home_dir():
    return subprocess.check_output("echo $HOME", shell=True, ).decode('utf-8').replace("\n", '')


def get_airflow_home_dir():
    home = get_home_dir()
    airflow_home = home + '/airflow'
    return airflow_home


def get_dag_id(engine):
    dag_id = input("job name: ")
    if ' ' in dag_id:
        return '', False
    if is_job_exists(dag_id, engine):
        return '', False
    return dag_id, True


def get_dag_id_for_python(engine):
    while True:
        dag_id, useable = get_dag_id(engine)
        if useable:
            break
        error_message.unuseable_dag_id()
    return dag_id


def get_owner():
    owner = input("owner : ")
    return owner


def get_airflow_home_and_backend_url():
    try:
        airflow_home = get_airflow_home_dir()
        backend_url = get_sql_alchemy_conn.get_sql_alchemy_conn(airflow_home)
    except:
        _ = backend_url
    return airflow_home, backend_url


def dir_exists(dir):
    return os.path.exists(dir)


def make_dir(dir):
    os.makedirs(dir)


def check_csv_dir(airflow_home):
    csv_files_dir = airflow_home + '/csv_files'
    if not dir_exists(csv_files_dir):
        make_dir(csv_files_dir)
    return csv_files_dir


def def_csv_dir(airflow_home):
    csv_files_dir = check_csv_dir(airflow_home)
    return os.path.abspath(csv_files_dir)


def get_schedule_interval():
    print(about_querypie_elt.about_schedule_interval)
    schedule_interval = input('schedule_interval : ')
    return schedule_interval


def get_db_type():
    print(about_querypie_elt.about_db_type)
    while True:
        db_type = input("db_type : ")
        if is_in_db_range(db_type): break
        error_message.unuseable_db_type()
        print(about_querypie_elt.about_db_type)
    return db_type


def get_id():
    while True:
        id = input('db login id : ')
        if (' ' in id):
            error_message.about_pep8()
            continue
        if not id:
            error_message.no_value(id)
            continue
        break
    return id


def get_pwd():
    while True:
        pwd = input('db login pwd : ')
        if (' ' in pwd):
            error_message.about_pep8()
            continue
        if not pwd:
            error_message.no_value(pwd)
            continue
        break
    return pwd


def get_auth():
    id = get_id()
    pwd = get_pwd()
    return id, pwd


def init_pk_and_updated(tables):
    pk = [''] * len(tables)
    updated = [''] * len(tables)
    return pk, updated


def get_database_schema_mysql():
    while not database:
        database = input('database : ').split(',')
    return database


def get_database_schema_snowflake():
    while not database:
        database = input('database : ').split(',')
    while True:
        schema = input('schema : ').split(',')
        if len(schema) == len(database):
            break
    return database, schema


def get_database_schema_amazon():
    while not database:
        database = input('database : ').split(',')
    while True:
        schema = input('schema : ').split(',')
        if len(database) == len(schema):
            break
    return database, schema


def get_database_schema(db_type):
    integrate_carrier = user_data_carrier()
    if db_type == db.mysql:
        integrate_carrier = get_database_schema_mysql()
    elif db_type == db.snowflake:
        integrate_carrier = get_database_schema_snowflake()
    elif db_type in (db.redshift, db.postgresql):
        integrate_carrier = get_database_schema_amazon()
    return integrate_carrier


def get_tables(db_type):
    while True:
        tables = input("tables : ").split(',')  # table 이름 중간에 , 가 들어간다면??
        if tables:
            break
        error_message.no_table()
        integrate_carrier = get_database_schema(db_type)
        if len(integrate_carrier.database) != len(tables):
            error_message.len_of_tables_and_len_of_database_are_not_correct()
            continue
    integrate_carrier.tables = tables
    integrate_carrier.pk, integrate_carrier.updated = init_pk_and_updated(tables)
    return integrate_carrier


def get_correct_upsert_rule(tables):
    while True:
        print(about_querypie_elt.about_upsert_rule)
        upsert_rule = input("rule : ").replace(" ", "").split(',')
        if len(tables) == len(upsert_rule):
            break
        error_message.len_of_rules_are_not_correct_with_len_of_tables()
    upsert_rule_correcter(upsert_rule)
    return upsert_rule


def set_columns_as_all(tables):
    return ['*'] * len(tables)


def columns_is_not_list(columns):
    if (columns[0] != '[' or columns[1] != "'" or columns[-1] != ']' or columns[-2] != "'"):
        return True
    else:
        return False


def get_columns(tables):
    while True:
        columns = input('columns : ')
        if not (columns):
            columns = set_columns_as_all(tables)
            break
        if (columns_is_not_list(columns)):
            error_message.columns_type_is_not_list()
            continue
    return columns


def make_columns_str_to_columns_list(columns_str, tables):
    columns = columns_str.replace('[', '').replace(']', '').replace("',", "&").split('&')

    for i in range(len(tables)):
        if not (columns[i]):
            # for unknown exception
            columns[i] = '*'
        columns[i] = columns[i].replace("'", "")
    return columns


def fill_columns(columns, tables):
    for i in range(len(tables)):
        if not (columns[i]):
            columns[i] = '*'
        columns[i] = columns[i].replace("'", "")


def get_useable_columns(tables):
    print(about_querypie_elt.about_columns)
    print(about_querypie_elt.skip_choose_columns)
    columns_str = get_columns(tables)
    columns_list = make_columns_str_to_columns_list(columns_str, tables)
    fill_columns(columns_list, tables)
    return columns_list


def upsert_rule_correcter(upsert_rule):
    for i in range(len(upsert_rule)):
        while not is_in_upsert_rule_range(upsert_rule[i]):
            error_message.wrong_upsert_rule()
            print('error rule : ' + upsert_rule[i])
            upsert_rule[i] = input("rule : ")


def get_pk(tables, upsert_rule, pk, i, column_type):
    print('table: ' + tables[i] + ', rule : ' + upsert_rule[i])
    pk[i] = input(column_type + " column : ")
    while not (pk[i]):
        error_message.no_pk()
        pk[i] = input(column_type + ' column : ')


def get_updated(tables, upsert_rule, updated_at, i, column_type):
    print('table: ' + tables[i] + ', rule : ' + upsert_rule[i])
    updated_at[i] = input(column_type + " column : ")
    while not (updated_at[i]):
        error_message.no_updated_at()
        updated_at[i] = input(column_type + ' column : ')


def get_pk_and_updated(integrate_carrier):
    pk = integrate_carrier.pk
    upsert_rule = integrate_carrier.upsert_rule
    updated = integrate_carrier.updated
    for i in range(len(upsert_rule)):
        rule = upsert_rule[i]
        if rule == 'increasement':
            get_pk(pk, i, "increase")
        elif rule == 'merge':
            get_pk(pk, i, "primary")
            get_updated(updated, i, 'updated_at')
        else:
            continue
    return pk, updated


def get_dag_info():
    dag = dag_info()
    dag.airflow_home, dag.backend_url = get_airflow_home_and_backend_url()
    print_programe_start_comment()
    dag.dag_id = get_dag_id_for_python(engine)
    dag.owner = get_owner()
    # make datetime of yesterday's date
    dag.start_date = '({year},{month},{day})'.format(year=dag.yesterday.year, month=dag.yesterday.month,
                                                     day=dag.yesterday.day)
    dag.csv_files_directory = def_csv_dir(dag.airflow_home)
    dag.schedule_interval = get_schedule_interval()
    return dag


def db_flag_maker(db_type):
    if db_type == 'mysql':
        return db.mysql
    elif db_type == 'snowflake':
        return db.snowflake
    elif db_type == 'redshift':
        return db.redshift
    elif db_type == 'postgresql':
        return db.postgresql


def get_mysql_info():
    while True:
        mysql = db_info()
        mysql.host = input('host : ')
        mysql.port = input('port : ')
        if not mysql.host or not mysql.port:
            error_message.write_everything()
            continue
        mysql.id, mysql.pwd = get_auth()
        break
    mysql.option = '?charset=utf8'
    return mysql


def get_snowflake_info():
    while True:
        snowflake = db_info()
        snowflake.account = input('account(host except snowflakecomputing.com) : ')
        if not snowflake.account:
            error_message.write_everything()
            continue
        snowflake.id, snowflake.pwd = get_auth()
        snowflake.warehouse = input('warehouse : ')
    snowflake.option = '?charset=utf8'

    return snowflake


def get_amazon_info():
    while True:
        amazon = db_info()
        amazon.host = input('host : ')
        amazon.port = input('port : ')
        if not amazon.host or not amazon.port:
            error_message.write_everything()
            continue
        amazon.id, amazon.pwd = get_auth()

    return amazon


def get_db_info(flag):
    if flag == db.mysql:
        db_url_component = get_mysql_info()
    elif flag == db.snowflake:
        db_url_component = get_snowflake_info()
    elif flag in (db.redshift, db.postgresql):
        db_url_component = get_amazon_info()
    db_url_component.option = '?charset=utf8'
    db_url_component.db_type = flag
    return db_url_component


def mysql_raw_code_maker(db_url_component, dag, type):
    raw_data = {'dag_id': [db_url_component.dag_id],
                'owner': [dag.owner],
                'directory': [dag.csv_files_directory],
                'start_date': [dag.start_date],
                'catchup': [dag.catchup],
                'schedule_interval': [dag.schedule_interval],
                'db_type': [db_url_component.integrate_db_type],
                'db_information': [
                    """{{'id': '{id}', 'pwd': '{pwd}', 'host': '{host}', 'port': '{port}' 'option': '{option}'}}
                    """.format(id=db_url_component.id, pwd=db_url_component.pwd, host=db_url_component.host,
                               port=db_url_component.port, option=db_url_component.option)],
                'info_type': [type]}
    return raw_data


def snowflake_raw_code_maker(db_url_component, dag, type):
    raw_data = {'dag_id': [db_url_component.dag_id],
                'owner': [dag.owner],
                'directory': [dag.csv_files_directory],
                'start_date': [dag.start_date],
                'catchup': [dag.catchup],
                'schedule_interval': [dag.schedule_interval],
                'db_type': [db_url_component.integrate_db_type],
                'db_information': [
                    """{{'id': '{id}', 'pwd': '{pwd}', 'account': '{account}', 'warehouse': '{warehouse}', 'role': '{role}'}}
                    """.format(id=db_url_component.id, pwd=db_url_component.pwd, account=db_url_component.account,
                               warehouse=db_url_component.warehouse, role=db_url_component.role)],
                'info_type': [type]}
    return raw_data


def amazon_raw_code_maker(db_url_component, dag, type):
    raw_data = {'dag_id': [db_url_component.dag_id],
                'owner': [dag.owner],
                'directory': [dag.csv_files_directory],
                'start_date': [dag.start_date],
                'catchup': [dag.catchup],
                'schedule_interval': [dag.schedule_interval],
                'db_type': [db_url_component.integrate_db_type],
                'db_information': [
                    """{{'id': '{id}', 'pwd': '{pwd}', 'host': '{host}', 'port': '{port}',  'option': '{option}' }}
                    """.format(id=db_url_component.id, pwd=db_url_component.pwd, host=db_url_component.host,
                               port=db_url_component.port, option=db_url_component.option)],
                'info_type': [type]}
    return raw_data


def raw_code_maker(db_url_component, dag, type):
    if db_url_component.db_type == db.mysql:
        raw_data = mysql_raw_code_maker(db_url_component, dag, type)

    elif db_url_component.db_type == db.snowflake:
        raw_data = snowflake_raw_code_maker(db_url_component, dag, type)

    elif db_url_component.db_type in (db.redshift, db.postgresql):
        raw_data = amazon_raw_code_maker(db_url_component, dag, type)

    return raw_data


def create_integrate_carrier(db_type):
    integrate_carrier = get_tables(db_type)
    integrate_carrier.upsert_rule = get_correct_upsert_rule(integrate_carrier.tables)
    # user must write columns above those are for front end and test
    integrate_carrier.columns = get_useable_columns(integrate_carrier.tables)
    integrate_carrier.pk, integrate_carrier.updated = get_pk_and_updated(integrate_carrier)
    integrate_carrier.dag_ids = [dag.dag_id] * len(integrate_carrier.tables)
    # default status are all on
    # status works as if this dag on or off
    integrate_carrier.status = ['on'] * len(integrate_carrier.tables)

    return integrate_carrier


def get_db_info(dag, type):
    db_type = get_db_type()
    db_flag = db_flag_maker(db_type)
    db_url_component = get_db_info(db_flag)
    raw_data = raw_code_maker(db_url_component, dag, type)
    metadata = pd.DataFrame(raw_data)
    return metadata, db_url_component.db_type


def make_tables_to_replicate(db_type):
    integrate_carrier = create_integrate_carrier(db_type)
    tables_to_replicate_raw_data = {'dag_id': integrate_carrier.dag_ids, 'database': integrate_carrier.database,
                                    'schema': integrate_carrier.schema, 'tables': integrate_carrier.tables,
                                    'columns': integrate_carrier.columns,
                                    'replicate_rule': integrate_carrier.upsert_rule, 'pk': integrate_carrier.pk,
                                    'updated_column': integrate_carrier.updated, 'status': integrate_carrier.status}
    tables_to_replicate = pd.DataFrame(tables_to_replicate_raw_data)
    return tables_to_replicate


def metadata_to_sql(integrate_metadata, destination_metadata, tables_to_replicate):
    integrate_metadata.to_sql('el_metadata', engine, if_exists='append', index=False)
    destination_metadata.to_sql('el_metadata', engine, if_exists='append', index=False)
    tables_to_replicate.to_sql('metadata_tables_to_replicate', engine, if_exists='append', index=False)


# ///////////////////main//////////////////
dag = get_dag_info()
engine = sql.create_engine(dag.backend_url)
integrate_metadata, integrate_db_type = get_db_info(dag, 'extract')
tables_to_replicate = make_tables_to_replicate(integrate_db_type)
destination_metadata, destination_db_type = get_db_info(dag, 'load')
metadata_to_sql(integrate_metadata, destination_metadata, tables_to_replicate)
make_a_dag(ex_db_type=integrate_db_type, ld_db_type=destination_db_type, file_name=dag.dag_id + '.py', file_dir='./',
           dag_id=dag.dag_id)

