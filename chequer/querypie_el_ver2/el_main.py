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
import sqlalchemy as sql

from get_information_from_user.get_dag import get_dag_info
from get_information_from_user.get_db_info import get_db_info
from get_information_from_user.make_a_dag import make_a_dag
from get_information_from_user.manage_users_tables import make_tables_to_replicate, metadata_to_sql

dag = get_dag_info()
engine = sql.create_engine(dag.backend_url)
integrate_metadata, integrate_db_type = get_db_info(dag, 'extract')
tables_to_replicate = make_tables_to_replicate(integrate_db_type, dag.dag_id)
destination_metadata, destination_db_type = get_db_info(dag, 'load')
metadata_to_sql(integrate_metadata, destination_metadata, tables_to_replicate, engine)
make_a_dag(ex_db_type=integrate_db_type, ld_db_type=destination_db_type, file_name=dag.dag_id + '.py', file_dir='./',
           dag_id=dag.dag_id)
