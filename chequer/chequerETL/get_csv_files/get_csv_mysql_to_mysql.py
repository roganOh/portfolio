# -*- coding:utf-8 -*-

import sqlalchemy as db
from airflow.hooks.mysql_hook import *
import pandas as pd

engine = db.create_engine(
    'mysql+pymysql://id:pwd@host:port/db?charset=utf8',
    encoding='utf-8')
mysql_hook1 = MySqlHook(mysql_conn_id='mysql_sqlgate_db')
mysql_hook2 = MySqlHook(mysql_conn_id='SQLGATE_ETLS')


def get_csv(table):
    if table == "":
        result = mysql_hook1.get_pandas_df("select * from " + table)
        result.to_csv('/home/ec2-user/airflow/csv_files/sqlgate_db/' + table + '.csv', sep=',', quotechar="'",
                      na_rep='NaN')
        result = pd.read_csv('/home/ec2-user/airflow/csv_files/sqlgate_db/' + table + '.csv', sep=',', quotechar="'",
                             dtype={"VERSION": str, "TYPE": str, "DEVICE_OS": str})
        result.to_sql(table, engine, if_exists='replace', index=False, chunksize = 200000)
    else:
        result = mysql_hook1.get_pandas_df("select * from " + table)
        result.to_csv('/home/ec2-user/airflow/csv_files/sqlgate_db/' + table + '.csv', sep=',', quotechar="'",
                      na_rep='NaN')
        result = pd.read_csv('/home/ec2-user/airflow/csv_files/sqlgate_db/' + table + '.csv', sep=',', quotechar="'")
        result.to_sql(table, engine, if_exists='replace', index=False,chunksize=200000)
    # if row number is over 100 0000, it will be killed please use chunksize
