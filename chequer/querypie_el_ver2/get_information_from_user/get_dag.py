import os
import subprocess

import pandas as pd
import sqlalchemy as sql

from get_information_from_user import get_sql_alchemy_conn
from get_information_from_user.messages import *
from get_information_from_user.struct_N_const import dag_info


def print_programe_start_comment():
    print(about_querypie_elt.welcome)
    print(about_querypie_elt.about_dag_id)


def get_dag_info():
    dag = dag_info()
    dag.airflow_home, dag.backend_url = get_airflow_home_and_backend_url()
    engine = sql.create_engine(dag.backend_url)
    print_programe_start_comment()
    dag.dag_id = get_dag_id_for_python(engine)
    dag.owner = get_owner()
    # make datetime of yesterday's date
    dag.start_date = '({year},{month},{day})'.format(year=dag.yesterday.year, month=dag.yesterday.month,
                                                     day=dag.yesterday.day)
    dag.csv_files_directory = def_csv_dir(dag.airflow_home)
    dag.schedule_interval = get_schedule_interval()
    return dag


def get_airflow_home_and_backend_url():
    try:
        airflow_home = get_airflow_home_dir()
        backend_url = get_sql_alchemy_conn.get_sql_alchemy_conn(airflow_home)
    except:
        _ = backend_url
    return airflow_home, backend_url


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


def def_csv_dir(airflow_home):
    csv_files_dir = check_csv_dir(airflow_home)
    return os.path.abspath(csv_files_dir)


def check_csv_dir(airflow_home):
    csv_files_dir = airflow_home + '/csv_files'
    if not dir_exists(csv_files_dir):
        make_dir(csv_files_dir)
    return csv_files_dir


def dir_exists(dir):
    return os.path.exists(dir)


def make_dir(dir):
    os.makedirs(dir)


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
        _ = None
        """ job must not exsist so this direction is correct direction. And so, when in this case, code does nothing."""
        return False
    else:  # job is exist
        error_message.dag_id_exists(job_name)
        return True


def get_schedule_interval():
    print(about_querypie_elt.about_schedule_interval)
    schedule_interval = input('schedule_interval : ')
    return schedule_interval


def get_home_dir():
    return subprocess.check_output("echo $HOME", shell=True, ).decode('utf-8').replace("\n", '')


def get_airflow_home_dir():
    home = get_home_dir()
    airflow_home = home + '/airflow'
    return airflow_home
