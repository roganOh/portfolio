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
import pandas as pd
import sqlalchemy as sql
import sys
import os
import json
import subprocess

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import get_sql_alchemy_conn

# cause I made install airflow at ~ , airlfow home may be ~/airflow
a=subprocess.check_output("echo $HOME",shell=True,)
a=a.decode('utf-8').replace("\n",'')
airflow_home=a+'/airflow'
backend = get_sql_alchemy_conn.get_sql_alchemy_conn(airflow_home)
backend_engine = sql.create_engine(backend)
def snowflake( dag_id, id, pwd, account, database, schema, warehouse, tables, directory, columns, pk, upsert, updated,
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
    ds = pd.read_sql_query(
        "select * from metadata where dag_id='{dag_id}' and info_type='load'".format(dag_id=dag_id.lower()),
        backend_engine)
    # if send dataframe to sql, data type become str and surround by '
    # I must magage with json but json is srrounded by " so I must do preprocess
    db_information = ds['db_information'][0]
    db_information = db_information.replace("'", "\"")
    for i in range(len(db_information)):
        if db_information[i] == '[':
            i = i + 1
            while (db_information[i] != ']'):
                if db_information[i] == "\"":
                    db_information = db_information[:i] + "'" + db_information[i + 1:]
                i = i + 1
    # now I make db_information as json type
    db_information = json.loads(db_information)
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
        filename = tables[i]
        database=database[i]
        schema=schema[i]
        if upsert[i] == 'truncate':
            indata = pd.read_sql_query(
                "select {column} from {database}.{schema}.{dag_id}_{table}".format(column=columns[i].lower(), table=filename.lower(),
                                                                                   database=database.lower(), schema=schema.lower(),
                                                                                   dag_id=dag_id.lower()), engine)
            indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'", na_rep='NaN',
                        index=False)
        elif upsert[i] in ('increasement','merge'):
            if upsert[i] == 'merge':
                pk[i]=updated[i]

            if (ds['db_type'][0] == 'snowflake'):
                if db_information['role'] == '':
                    url_role = ''
                else:
                    url_role = '&role={}'.format(db_information['role'])
                engine_ds = sql.create_engine(
                    'snowflake://{u}:{p}@{a}/{d}/{s}?warehouse={w}&role={r}'.format(
                        u=db_information['id'],
                        p=db_information['pwd'],
                        a=db_information['account'],
                        r=url_role,
                        d=db_information['database'],
                        s=db_information['schema'],
                        w=db_information['warehouse']
                    ))
            elif (ds['db_type'][0] == 'mysql'):
                engine_ds = sql.create_engine('mysql+pymysql://{u}:{p}@{h}:{port}/{d}{option}'.format(
                    u=db_information['id'],
                    p=db_information['pwd'],
                    h=db_information['host'],
                    port=db_information['port'],
                    d=db_information['database'],
                    option=db_information['option']
                ), encoding='utf-8')
            elif (ds['db_type'][0] == 'redshift'):
                url = 'redshift+psycopg2://{u}:{p}@{h}:{port}/{d}'.format(
                    u=db_information['id'],
                    p=db_information['pwd'],
                    h=db_information['host'],
                    port=db_information['port'],
                    d=db_information['database']
                )
                engine_ds = sql.create_engine(url, client_encoding='utf8')
            elif (ds['db_type'][0] == 'postgresql'):
                url = 'postgresql://{u}:{p}@{h}:{port}/{d}'.format(
                    u=db_information['id'],
                    p=db_information['pwd'],
                    h=db_information['host'],
                    port=db_information['port'],
                    d=db_information['database']
                )
                engine_ds = sql.create_engine(url, client_encoding='utf8')

            if (ds['db_type'][0] in ('postgresql','redshift')):
                try:
                    max_value = \
                        engine_ds.execute(
                            'select max({pk}) from {database}.{schema}.{dag_id}_{filename}'.format(pk=pk[i],
                                                                                                   database=db_information['database'],
                                                                                                   schema=db_information['schema'],
                                                                                                   filename=filename,
                                                                                                   dag_id=dag_id)).first()[0]
                except:  # same as truncate if table doesn't exist at load_db
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{schema}.{dag_id}_{table}".format(column=columns[i],
                                                                                           table=filename,
                                                                                           database=database,
                                                                                           schema=schema,
                                                                                           dag_id=dag_id),
                        engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'", na_rep='NaN', index=False)
                else:
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{schema}.{dag_id}_{table} where {pk}>'{max_date}'".format(
                            column=columns[i], database=database, schema=schema,
                            dag_id=dag_id, table=filename, pk=pk[i], max_date=max_value), engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'", na_rep='NaN',index=False)

            elif (ds['db_type'][0] == 'snowflake'):
                try:
                    max_value = \
                        engine_ds.execute(
                            'select max({pk}) from {database}.{schema}.{dag_id}_{filename}'.format(pk=pk[i],
                                                                                                   database=
                                                                                                   db_information[
                                                                                                       'database'],
                                                                                                   schema=
                                                                                                   db_information[
                                                                                                       'schema'],
                                                                                                   filename=filename,
                                                                                                   dag_id=dag_id)).first()[
                            0]
                except:  # same as truncate if table doesn't exist at load_db
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{schema}.{dag_id}_{table}".format(column=columns[i].lower(),
                                                                                           table=filename.lower(),
                                                                                           database=database.lower(),
                                                                                           schema=schema.lower(),
                                                                                           dag_id=dag_id.lower()),
                        engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'", na_rep='NaN', index=False)
                else:
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{schema}.{dag_id}_{table} where {pk}>'{max_date}'".format(
                            column=columns[i].lower(), database=database.lower(), schema=schema.lower(),
                            dag_id=dag_id.lower(), table=filename.lower(), pk=pk[i].lower(), max_date=max_value), engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'", na_rep='NaN',index=False)

            elif (ds['db_type'][0] == 'mysql'):
                try:
                    max_value = \
                        engine_ds.execute(
                            'select max({pk}) from {database}.{dag_id}_{filename}'.format(pk=pk[i],
                                                                                                   database=
                                                                                                   db_information[
                                                                                                       'database'],
                                                                                                   filename=filename,
                                                                                                   dag_id=dag_id)).first()[
                            0]
                except:  # same as truncate if table doesn't exist at load_db
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{schema}.{dag_id}_{table}".format(column=columns[i],
                                                                                           table=filename,
                                                                                           database=database,
                                                                                           schema=schema,
                                                                                           dag_id=dag_id),
                        engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'",
                                  na_rep='NaN', index=False)
                else:
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{schema}.{dag_id}_{table} where {pk}>'{max_date}'".format(
                            column=columns[i], database=database, schema=schema,
                            dag_id=dag_id, table=filename, pk=pk[i], max_date=max_value), engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'", na_rep='NaN',
                                index=False)



def postgresql(dag_id, id, pwd, host, port, database,schema, tables, directory, pk, upsert, updated, columns):
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
    ds = pd.read_sql_query(
        "select * from metadata where dag_id='{dag_id}' and info_type='load'".format(dag_id=dag_id),
        backend_engine)
    db_information = ds['db_information'][0]
    db_information = db_information.replace("'", "\"")
    for i in range(len(db_information)):
        if db_information[i] == '[':
            i = i + 1
            while (db_information[i] != ']'):
                if db_information[i] == "\"":
                    db_information = db_information[:i] + "'" + db_information[i + 1:]
                i = i + 1
    db_information = json.loads(db_information)
    url = 'postgresql://{u}:{p}@{h}:{port}/{d}'.format(
        u=id,
        p=pwd,
        h=host,
        port=port,
        d=database[0]
    )
    engine = sql.create_engine(url, client_encoding='utf8')
    for i in range(len(tables)):
        filename = tables[i]
        database = database[i]
        schema = schema[i]
        if upsert[i] == 'truncate':
            indata = pd.read_sql_query(
                "select {column} from {database}.{schema}.{dag_id}_{table}".format(column=columns[i],
                                                                                   table=filename,
                                                                                   database=database,
                                                                                   schema=schema,
                                                                                   dag_id=dag_id), engine)
            indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'", na_rep='NaN',
                          index=False)
        elif upsert[i] in ('increasement', 'merge'):
            if upsert[i] == 'merge':
                pk[i] = updated[i]

            if (ds['db_type'][0] == 'snowflake'):
                if db_information['role'] == '':
                    url_role = ''
                else:
                    url_role = '&role={}'.format(db_information['role'])
                engine_ds = sql.create_engine(
                    'snowflake://{u}:{p}@{a}/{d}/{s}?warehouse={w}&role={r}'.format(
                        u=db_information['id'],
                        p=db_information['pwd'],
                        a=db_information['account'],
                        r=url_role,
                        d=db_information['database'],
                        s=db_information['schema'],
                        w=db_information['warehouse']
                    ))
            elif (ds['db_type'][0] == 'mysql'):
                engine_ds = sql.create_engine('mysql+pymysql://{u}:{p}@{h}:{port}/{d}{option}'.format(
                    u=db_information['id'],
                    p=db_information['pwd'],
                    h=db_information['host'],
                    port=db_information['port'],
                    d=db_information['database'],
                    option=db_information['option']
                ), encoding='utf-8')
            elif (ds['db_type'][0] == 'redshift'):
                url = 'redshift+psycopg2://{u}:{p}@{h}:{port}/{d}'.format(
                    u=db_information['id'],
                    p=db_information['pwd'],
                    h=db_information['host'],
                    port=db_information['port'],
                    d=db_information['database']
                )
                engine_ds = sql.create_engine(url, client_encoding='utf8')
            elif (ds['db_type'][0] == 'postgresql'):
                url = 'postgresql://{u}:{p}@{h}:{port}/{d}'.format(
                    u=db_information['id'],
                    p=db_information['pwd'],
                    h=db_information['host'],
                    port=db_information['port'],
                    d=db_information['database']
                )
                engine_ds = sql.create_engine(url, client_encoding='utf8')

            if (ds['db_type'][0] in ('postgresql', 'redshift')):
                try:
                    max_value = \
                        engine_ds.execute(
                            'select max({pk}) from {database}.{schema}.{dag_id}_{filename}'.format(pk=pk[i],
                                                                                                   database=
                                                                                                   db_information[
                                                                                                       'database'],
                                                                                                   schema=
                                                                                                   db_information[
                                                                                                       'schema'],
                                                                                                   filename=filename,
                                                                                                   dag_id=dag_id)).first()[
                            0]
                except:  # same as truncate
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{schema}.{dag_id}_{table}".format(column=columns[i],
                                                                                           table=filename,
                                                                                           database=database,
                                                                                           schema=schema,
                                                                                           dag_id=dag_id),
                        engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'",
                                  na_rep='NaN', index=False)
                else:
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{schema}.{dag_id}_{table} where {pk}>'{max_date}'".format(
                            column=columns[i], database=database, schema=schema,
                            dag_id=dag_id, table=filename, pk=pk[i], max_date=max_value), engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'",
                                  na_rep='NaN', index=False)

            elif (ds['db_type'][0] == 'snowflake'):
                try:
                    max_value = \
                        engine_ds.execute(
                            'select max({pk}) from {database}.{schema}.{dag_id}_{filename}'.format(pk=pk[i],
                                                                                                   database=
                                                                                                   db_information[
                                                                                                       'database'],
                                                                                                   schema=
                                                                                                   db_information[
                                                                                                       'schema'],
                                                                                                   filename=filename,
                                                                                                   dag_id=dag_id)).first()[
                            0]
                except:  # same as truncate
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{schema}.{dag_id}_{table}".format(column=columns[i],
                                                                                           table=filename,
                                                                                           database=database,
                                                                                           schema=schema,
                                                                                           dag_id=dag_id),
                        engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'",
                                  na_rep='NaN', index=False)
                else:
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{schema}.{dag_id}_{table} where {pk}>'{max_date}'".format(
                            column=columns[i], database=database, schema=schema,
                            dag_id=dag_id, table=filename, pk=pk[i], max_date=max_value),
                        engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'",
                                  na_rep='NaN', index=False)

            elif (ds['db_type'][0] == 'mysql'):
                try:
                    max_value = \
                        engine_ds.execute(
                            'select max({pk}) from {database}.{dag_id}_{filename}'.format(pk=pk[i],
                                                                                                   database=
                                                                                                   db_information[
                                                                                                       'database'],
                                                                                                   filename=filename,
                                                                                                   dag_id=dag_id)).first()[
                            0]
                except:  # same as truncate
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{schema}.{dag_id}_{table}".format(column=columns[i],
                                                                                           table=filename,
                                                                                           database=database,
                                                                                           schema=schema,
                                                                                           dag_id=dag_id),
                        engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'",
                                  na_rep='NaN', index=False)
                else:
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{schema}.{dag_id}_{table} where {pk}>'{max_date}'".format(
                            column=columns[i], database=database, schema=schema,
                            dag_id=dag_id, table=filename, pk=pk[i], max_date=max_value), engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'", na_rep='NaN',
                                index=False)


def redshift(dag_id, id, pwd, host, port, database,schema, tables, directory, pk, upsert, updated, columns):
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
    ds = pd.read_sql_query(
        "select * from metadata where dag_id='{dag_id}' and info_type='load'".format(dag_id=dag_id),
        backend_engine)
    db_information = ds['db_information'][0]
    db_information = db_information.replace("'", "\"")
    for i in range(len(db_information)):
        if db_information[i] == '[':
            i = i + 1
            while (db_information[i] != ']'):
                if db_information[i] == "\"":
                    db_information = db_information[:i] + "'" + db_information[i + 1:]
                i = i + 1
    db_information = json.loads(db_information)
    url = 'redshift+psycopg2://{u}:{p}@{h}:{port}/{d}'.format(
        u=id,
        p=pwd,
        h=host,
        port=port,
        d=database[0]
    )
    engine = sql.create_engine(url, client_encoding='utf8')
    for i in range(len(tables)):
        filename = tables[i]
        database = database[i]
        schema = schema[i]
        if upsert[i] == 'truncate':
            indata = pd.read_sql_query(
                "select {column} from {database}.{schema}.{dag_id}_{table}".format(column=columns[i],
                                                                                   table=filename,
                                                                                   database=database,
                                                                                   schema=schema,
                                                                                   dag_id=dag_id), engine)
            indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'", na_rep='NaN',
                          index=False)
        elif upsert[i] in ('increasement', 'merge'):
            if upsert[i] == 'merge':
                pk[i] = updated[i]

            if (ds['db_type'][0] == 'snowflake'):
                if db_information['role'] == '':
                    url_role = ''
                else:
                    url_role = '&role={}'.format(db_information['role'])
                engine_ds = sql.create_engine(
                    'snowflake://{u}:{p}@{a}/{d}/{s}?warehouse={w}&role={r}'.format(
                        u=db_information['id'],
                        p=db_information['pwd'],
                        a=db_information['account'],
                        r=url_role,
                        d=db_information['database'],
                        s=db_information['schema'],
                        w=db_information['warehouse']
                    ))
            elif (ds['db_type'][0] == 'mysql'):
                engine_ds = sql.create_engine('mysql+pymysql://{u}:{p}@{h}:{port}/{d}{option}'.format(
                    u=db_information['id'],
                    p=db_information['pwd'],
                    h=db_information['host'],
                    port=db_information['port'],
                    d=db_information['database'],
                    option=db_information['option']
                ), encoding='utf-8')
            elif (ds['db_type'][0] == 'redshift'):
                url = 'redshift+psycopg2://{u}:{p}@{h}:{port}/{d}'.format(
                    u=db_information['id'],
                    p=db_information['pwd'],
                    h=db_information['host'],
                    port=db_information['port'],
                    d=db_information['database']
                )
                engine_ds = sql.create_engine(url, client_encoding='utf8')
            elif (ds['db_type'][0] == 'postgresql'):
                url = 'postgresql://{u}:{p}@{h}:{port}/{d}'.format(
                    u=db_information['id'],
                    p=db_information['pwd'],
                    h=db_information['host'],
                    port=db_information['port'],
                    d=db_information['database']
                )
                engine_ds = sql.create_engine(url, client_encoding='utf8')

            if (ds['db_type'][0] in ('postgresql', 'redshift')):
                try:
                    max_value = \
                        engine_ds.execute(
                            'select max({pk}) from {database}.{schema}.{dag_id}_{filename}'.format(pk=pk[i],
                                                                                                   database=
                                                                                                   db_information[
                                                                                                       'database'],
                                                                                                   schema=
                                                                                                   db_information[
                                                                                                       'schema'],
                                                                                                   filename=filename,
                                                                                                   dag_id=dag_id)).first()[
                            0]
                except:  # same as truncate
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{schema}.{dag_id}_{table}".format(column=columns[i],
                                                                                           table=filename,
                                                                                           database=database,
                                                                                           schema=schema,
                                                                                           dag_id=dag_id),
                        engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'",
                                  na_rep='NaN', index=False)
                else:
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{schema}.{dag_id}_{table} where {pk}>'{max_date}'".format(
                            column=columns[i], database=database, schema=schema,
                            dag_id=dag_id, table=filename, pk=pk[i], max_date=max_value), engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'",
                                  na_rep='NaN', index=False)

            elif (ds['db_type'][0] == 'snowflake'):
                try:
                    max_value = \
                        engine_ds.execute(
                            'select max({pk}) from {database}.{schema}.{dag_id}_{filename}'.format(pk=pk[i],
                                                                                                   database=
                                                                                                   db_information[
                                                                                                       'database'],
                                                                                                   schema=
                                                                                                   db_information[
                                                                                                       'schema'],
                                                                                                   filename=filename,
                                                                                                   dag_id=dag_id)).first()[
                            0]
                except:  # same as truncate
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{schema}.{dag_id}_{table}".format(column=columns[i],
                                                                                           table=filename,
                                                                                           database=database,
                                                                                           schema=schema,
                                                                                           dag_id=dag_id),
                        engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'",
                                  na_rep='NaN', index=False)
                else:
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{schema}.{dag_id}_{table} where {pk}>'{max_date}'".format(
                            column=columns[i], database=database, schema=schema,
                            dag_id=dag_id, table=filename, pk=pk[i], max_date=max_value),
                        engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'",
                                  na_rep='NaN', index=False)

            elif (ds['db_type'][0] == 'mysql'):
                try:
                    max_value = \
                        engine_ds.execute(
                            'select max({pk}) from {database}.{dag_id}_{filename}'.format(pk=pk[i],
                                                                                                   database=
                                                                                                   db_information[
                                                                                                       'database'],
                                                                                                   filename=filename,
                                                                                                   dag_id=dag_id)).first()[
                            0]
                except:  # same as truncate
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{schema}.{dag_id}_{table}".format(column=columns[i],
                                                                                           table=filename,
                                                                                           database=database,
                                                                                           schema=schema,
                                                                                           dag_id=dag_id),
                        engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'",
                                  na_rep='NaN', index=False)
                else:
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{schema}.{dag_id}_{table} where {pk}>'{max_date}'".format(
                            column=columns[i], database=database, schema=schema,
                            dag_id=dag_id, table=filename, pk=pk[i], max_date=max_value), engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'", na_rep='NaN',
                                index=False)


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
    ds = pd.read_sql_query(
        "select * from metadata where dag_id='{dag_id}' and info_type='load'".format(dag_id=dag_id),
        backend_engine)
    db_information = ds['db_information'][0]
    db_information = db_information.replace("'", "\"")
    for i in range(len(db_information)):
        if db_information[i] == '[':
            i = i + 1
            while (db_information[i] != ']'):
                if db_information[i] == "\"":
                    db_information = db_information[:i] + "'" + db_information[i + 1:]
                i = i + 1
    db_information = json.loads(db_information)
    engine = sql.create_engine('mysql+pymysql://{u}:{p}@{h}:{port}/{d}{option}'.format(
        u=id,
        p=pwd,
        h=host,
        port=port,
        d=database[0],
        option=option,
    ), encoding='utf-8')
    for i in range(len(tables)):
        filename = tables[i]
        database = database[i]
        if upsert[i] == 'truncate':
            indata = pd.read_sql_query(
                "select {column} from {database}.{dag_id}_{table}".format(column=columns[i],
                                                                                   table=filename,
                                                                                   database=database,
                                                                                   dag_id=dag_id), engine)
            indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'", na_rep='NaN',
                          index=False)
        elif upsert[i] in ('increasement', 'merge'):
            if upsert[i] == 'merge':
                pk[i] = updated[i]

            if (ds['db_type'][0] == 'snowflake'):
                if db_information['role'] == '':
                    url_role = ''
                else:
                    url_role = '&role={}'.format(db_information['role'])
                engine_ds = sql.create_engine(
                    'snowflake://{u}:{p}@{a}/{d}/{s}?warehouse={w}&role={r}'.format(
                        u=db_information['id'],
                        p=db_information['pwd'],
                        a=db_information['account'],
                        r=url_role,
                        d=db_information['database'],
                        s=db_information['schema'],
                        w=db_information['warehouse']
                    ))
            elif (ds['db_type'][0] == 'mysql'):
                engine_ds = sql.create_engine('mysql+pymysql://{u}:{p}@{h}:{port}/{d}{option}'.format(
                    u=db_information['id'],
                    p=db_information['pwd'],
                    h=db_information['host'],
                    port=db_information['port'],
                    d=db_information['database'],
                    option=db_information['option']
                ), encoding='utf-8')
            elif (ds['db_type'][0] == 'redshift'):
                url = 'redshift+psycopg2://{u}:{p}@{h}:{port}/{d}'.format(
                    u=db_information['id'],
                    p=db_information['pwd'],
                    h=db_information['host'],
                    port=db_information['port'],
                    d=db_information['database']
                )
                engine_ds = sql.create_engine(url, client_encoding='utf8')
            elif (ds['db_type'][0] == 'postgresql'):
                url = 'postgresql://{u}:{p}@{h}:{port}/{d}'.format(
                    u=db_information['id'],
                    p=db_information['pwd'],
                    h=db_information['host'],
                    port=db_information['port'],
                    d=db_information['database']
                )
                engine_ds = sql.create_engine(url, client_encoding='utf8')

            if (ds['db_type'][0] in ('postgresql', 'redshift')):
                try:
                    max_value = \
                        engine_ds.execute(
                            'select max({pk}) from {database}.{schema}.{dag_id}_{filename}'.format(pk=pk[i],
                                                                                                   database=
                                                                                                   db_information[
                                                                                                       'database'],
                                                                                                   schema=
                                                                                                   db_information[
                                                                                                       'schema'],
                                                                                                   filename=filename,
                                                                                                   dag_id=dag_id)).first()[
                            0]
                except:  # same as truncate
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{dag_id}_{table}".format(column=columns[i],
                                                                                           table=filename,
                                                                                           database=database,
                                                                                           dag_id=dag_id),
                        engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'",
                                  na_rep='NaN', index=False)
                else:
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{dag_id}_{table} where {pk}>'{max_date}'".format(
                            column=columns[i], database=database,
                            dag_id=dag_id, table=filename, pk=pk[i], max_date=max_value), engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'",
                                  na_rep='NaN', index=False)

            elif (ds['db_type'][0] == 'snowflake'):
                try:
                    max_value = \
                        engine_ds.execute(
                            'select max({pk}) from {database}.{schema}.{dag_id}_{filename}'.format(pk=pk[i],
                                                                                                   database=
                                                                                                   db_information[
                                                                                                       'database'],
                                                                                                   schema=
                                                                                                   db_information[
                                                                                                       'schema'],
                                                                                                   filename=filename,
                                                                                                   dag_id=dag_id)).first()[
                            0]
                except:  # same as truncate
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{dag_id}_{table}".format(column=columns[i],
                                                                                           table=filename,
                                                                                           database=database,
                                                                                           dag_id=dag_id),
                        engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'",
                                  na_rep='NaN', index=False)
                else:
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{dag_id}_{table} where {pk}>'{max_date}'".format(
                            column=columns[i], database=database,
                            dag_id=dag_id, table=filename, pk=pk[i], max_date=max_value),
                        engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'",
                                  na_rep='NaN', index=False)

            elif (ds['db_type'][0] == 'mysql'):
                try:
                    max_value = \
                        engine_ds.execute(
                            'select max({pk}) from {database}.{dag_id}_{filename}'.format(pk=pk[i],
                                                                                                   database=
                                                                                                   db_information[
                                                                                                       'database'],
                                                                                                   filename=filename,
                                                                                                   dag_id=dag_id)).first()[
                            0]
                except:  # same as truncate
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{dag_id}_{table}".format(column=columns[i],
                                                                                  table=filename,
                                                                                  database=database,
                                                                                  dag_id=dag_id),
                        engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'",
                                  na_rep='NaN', index=False)
                else:
                    indata = pd.read_sql_query(
                        "select {column} from {database}.{dag_id}_{table} where {pk}>'{max_date}'".format(
                            column=columns[i], database=database,
                            dag_id=dag_id, table=filename, pk=pk[i], max_date=max_value),
                        engine)
                    indata.to_csv(directory + '/' + dag_id + '_' + filename + '.csv', sep=',', quotechar="'",
                                  na_rep='NaN', index=False)