# -*- coding:utf-8 -*-

import csv

from airflow.providers.mysql.hooks.mysql import *


def get_csv(conn_id, directory, tables, **kwargs):
    hook = MySqlHook(mysql_conn_id=conn_id)
    for tablename in tables:
        query_result = hook.get_records("select * from " + tablename)
        with open('/home/ec2-user/airflow/csv_files/' + directory + '/' + tablename + '.tsv', 'w') as new_csv:
            csv_writer = csv.writer(new_csv, delimiter=',', quotechar='"')
            for line in query_result:
                csv_writer.writerow(line)

