import pandas as pd

from get_information_from_user.messages import *
from get_information_from_user.struct_N_const import db, db_info


def get_db_info(dag, type):
    db_type = get_db_type()
    db_flag = db_flag_maker(db_type)
    db_url_component = get_db_url(db_flag)
    raw_data = raw_code_maker(db_url_component, dag, type)
    metadata = pd.DataFrame(raw_data)
    return metadata, db_url_component.db_type


def get_db_type():
    print(about_querypie_elt.about_db_type)
    while True:
        db_type = input("db_type : ")
        if is_in_db_range(db_type): break
        error_message.unuseable_db_type()
        print(about_querypie_elt.about_db_type)
    return db_type


def db_flag_maker(db_type):
    if db_type == 'mysql':
        return db.mysql
    elif db_type == 'snowflake':
        return db.snowflake
    elif db_type == 'redshift':
        return db.redshift
    elif db_type == 'postgresql':
        return db.postgresql


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


def is_in_db_range(thing):
    return thing in about_querypie_elt.db_range


def get_db_url(flag):
    if flag == db.mysql:
        db_url_component = get_mysql_info()
    elif flag == db.snowflake:
        db_url_component = get_snowflake_info()
    elif flag in (db.redshift, db.postgresql):
        db_url_component = get_amazon_info()
    db_url_component.option = '?charset=utf8'
    db_url_component.db_type = flag
    return db_url_component


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


def get_id():
    while True:
        id = input('db login id : ')
        if ' ' in id:
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
        if ' ' in pwd:
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
