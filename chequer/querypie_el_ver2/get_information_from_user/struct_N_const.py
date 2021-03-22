import datetime as date
from dataclasses import dataclass
from enum import Enum
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