# -*- coding:utf-8 -*-

"""Library for extract data from integration db.

This library is made to run a DAG that user made in make_a_dag.py
This library and do_load.py are core of querypie_el

Available functions:
- snowflake: get data from snowflake
- mysql: get data from mysql
- redshift: get data from mysql
- postgresql:get data from postgresql

When merge and increasement, check if tables exist at load db
if not, works like truncate and
else, works get max of updated_at(merge)/pk(increasement) and add sql like 'where updated >= max_updated_at_from_load'
in every function
"""

import sqlalchemy as sql

from querypie_el_ver2.get_information_from_user.structs import user_all_data
from querypie_el_ver2.hook.extract.get_data_from_max_val import get_data_from_max_val
from querypie_el_ver2.hook.extract.get_db_info import get_db_info
from querypie_el_ver2.hook.extract.get_full_table import *
from get_information_from_user.make_a_dag import get_backend

backend_engine = sql.create_engine(get_backend())


def snowflake(dag_id, id, pwd, account, database, schema, warehouse, tables, directory, columns, pk, upsert, updated,
              role=''):
    """
    :param dag_id: name of user's dag
    :param id: login_id
    :param pwd: login_password
    :param account: host without .snowflakecomputing.com
    :param database: user's database
    :param schema: user's schema
    :param warehouse: user's warehouse
    :param tables: user's tables
    :rtype: list
    :param directory: directory where csv file will be saved.
    :param columns: user's columns
    :rtype: list
    :param upsert: by this param, this function will run differently.
        :param pk: if upsert=increasement, pk=increase column elif upsert=merg, pk=primary key
        :param updated: if upsert=increasement, updated=date column which will be benchmark
        :rtype: list
    :rtype: list
    :param role: user's role in snowflake
    :return: csv file that user want to execute from db.
    """

    metadatas = user_all_data(dag_id, id, pwd, columns, pk, updated, upsert, tables, database, directory,
                              warehouse, role=role, schema=schema, acount=account)

    ds, db_information = get_db_info(dag_id, backend_engine)

    if role == '':
        url_role = role
    else:
        url_role = '&role={}'.format(role)
    engine = sql.create_engine(
        'snowflake://{u}:{p}@{a}/{d}/{s}?warehouse={w}&role={r}'.format(
            u=id,
            p=pwd,
            a=account,
            r=url_role,
            d=database[0].lower(),
            s=schema[0].lower(),
            w=warehouse
        )
    )
    for i in range(len(tables)):
        if upsert[i] == 'truncate':
            get_full_table_snowflake(i, metadatas, engine)

        elif upsert[i] in ('increasement', 'merge'):
            get_data_from_max_val(engine, i, ds, db_information, metadatas)


def postgresql(dag_id, id, pwd, host, port, database, schema, tables, directory, pk, upsert, updated, columns):
    """
    :param dag_id: id for dag which made in make_a_dag.py
    :param id: login id
    :param pwd: login password
    :param host: db's host
    :param port: db's port
    :param database: user's database
    :param tables: user's talbes
    :rtype: list
    :param directory: directory for csv files will be saved.
    :param upsert: by this param, this function will run differently.
        :param pk: if upsert=increasement, pk=increase column elif upsert=merg, pk=primary key
        :param updated: if upsert=increasement, updated=date column which will be benchmark
        :rtype: list
    :rtype: list
    :param columns: user's columns
    :return: csv file of data
    """
    metadatas = user_all_data(dag_id, id, pwd, columns, pk, updated, upsert, tables, database, directory, schema=schema,
                              host=host, port=port)
    ds, db_information = get_db_info(dag_id, backend_engine)

    url = 'postgresql://{u}:{p}@{h}:{port}/{d}'.format(
        u=id,
        p=pwd,
        h=host,
        port=port,
        d=database[0]
    )
    engine = sql.create_engine(url, client_encoding='utf8')
    for i in range(len(tables)):
        if upsert[i] == 'truncate':
            get_full_table_amazon(i, metadatas, engine)

        elif upsert[i] in ('increasement', 'merge'):
            get_data_from_max_val(engine, i, ds, db_information, metadatas)


def redshift(dag_id, id, pwd, host, port, database, schema, tables, directory, pk, upsert, updated, columns):
    """
    :param dag_id: id for dag which made by make_a_dag.py
    :param id: login id
    :param pwd: login password
    :param host: db's host
    :param port: db's port
    :param database: user's database
    :param tables: user's tables
    :rtype: list
    :param directory: directory where csv files will be saved
    :param columns: user's columns
    :param upsert: by this param, this function will run differently.
        :param pk: if upsert=increasement, pk=increase column elif upsert=merg, pk=primary key
        :param updated: if upsert=increasement, updated=date column which will be benchmark
        :rtype: list
    :rtype: list
    :param columns: user's columns
    :rypte: list
    :return: csv files for data
    """
    metadatas = user_all_data(dag_id, id, pwd, columns, pk, updated, upsert, tables, database, directory, schema=schema,
                              host=host, port=port)
    ds, db_information = get_db_info(dag_id, backend_engine)

    url = 'redshift+psycopg2://{u}:{p}@{h}:{port}/{d}'.format(
        u=id,
        p=pwd,
        h=host,
        port=port,
        d=database[0]
    )
    engine = sql.create_engine(url, client_encoding='utf8')
    for i in range(len(tables)):
        if upsert[i] == 'truncate':
            get_full_table_amazon(i, metadatas, engine)

        elif upsert[i] in ('increasement', 'merge'):
            get_data_from_max_val(engine, i, ds, db_information, metadatas)


def mysql(dag_id, id, pwd, host, port, database, tables, directory, columns, pk, upsert, updated, option):
    """

    :param dag_id: user's dag_id which made in make_a_dag.py
    :param id: login id
    :param pwd: login password
    :param host: db's host
    :param port: db's port
    :param database: user's database
    :param tables: user's tables
    :rtype: list
    :param directory: directory where csv files will be saved.
    :param columns: user's columns
        :param upsert: by this param, this function will run differently.
        :param pk: if upsert=increasement, pk=increase column elif upsert=merg, pk=primary key
        :param updated: if upsert=increasement, updated=date column which will be benchmark
        :rtype: list
    :rtype: list
    :param option: user's option when connect db
    :return:
    """
    metadatas = user_all_data(dag_id, id, pwd, columns, pk, updated, upsert, tables, database, directory, option=option,
                              host=host, port=port)
    ds, db_information = get_db_info(dag_id, backend_engine)
    engine = sql.create_engine('mysql+pymysql://{u}:{p}@{h}:{port}/{d}{option}'.format(
        u=id,
        p=pwd,
        h=host,
        port=port,
        d=database[0],
        option=option,
    ), encoding='utf-8')
    for i in range(len(tables)):
        if upsert[i] == 'truncate':
            get_full_table_amazon(i, metadatas, engine)

        elif upsert[i] in ('increasement', 'merge'):
            get_data_from_max_val(engine, i, ds, db_information, metadatas)
