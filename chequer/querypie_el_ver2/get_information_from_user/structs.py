import datetime as date
from dataclasses import dataclass


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


@dataclass
class user_all_data:
    def __init__(self, dag_id, id, pwd, columns, pk, updated, upsert_rule, tables, database,
                 csv_file_directory, warehouse='', option='', role='', schema=[], host='', port='',account=''):
        self.dag_id = dag_id
        self.host = host
        self.port = port
        self.id = id
        self.pwd = pwd
        self.columns = columns
        self.pk = pk
        self.updated = updated
        self.upsert_rule = upsert_rule
        self.tables = tables
        self.database = database
        self.csv_files_directory = csv_file_directory
        self.warehouse = warehouse
        self.option = option
        self.role = role
        self.schema = schema
        self.account = account

    dag_id: str = None
    host: str = None
    port: str = None
    account: str = None
    id: str = None
    pwd: str = None
    warehouse: str = None
    option: str = None
    role: str = None
    columns: list = None
    pk: list = None
    updated: list = None
    schema: list = None
    upsert_rule: list = None
    tables: list = None
    database: list = None
    csv_files_directory: str = None
