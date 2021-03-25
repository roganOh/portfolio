import pandas as pd

from get_information_from_user.const_value import *
from get_information_from_user.structs import user_data_carrier


def create_integrate_carrier(db_type, dag_id):
    integrate_carrier = get_tables(db_type)
    integrate_carrier.upsert_rule = get_correct_upsert_rule(integrate_carrier.tables)
    # user must write columns above those are for front end and test
    integrate_carrier.columns = get_useable_columns(integrate_carrier.tables)
    integrate_carrier.pk, integrate_carrier.updated = get_pk_and_updated(integrate_carrier)
    integrate_carrier.dag_ids = [dag_id] * len(integrate_carrier.tables)
    # default status are all on
    # status works as if this dag on or off
    integrate_carrier.status = ['on'] * len(integrate_carrier.tables)

    return integrate_carrier


def make_tables_to_replicate(db_type, dag_id):
    integrate_carrier = create_integrate_carrier(db_type, dag_id)
    tables_to_replicate_raw_data = {'dag_id': integrate_carrier.dag_ids, 'database': integrate_carrier.database,
                                    'schema': integrate_carrier.schema, 'tables': integrate_carrier.tables,
                                    'columns': integrate_carrier.columns,
                                    'replicate_rule': integrate_carrier.upsert_rule, 'pk': integrate_carrier.pk,
                                    'updated_column': integrate_carrier.updated, 'status': integrate_carrier.status}
    tables_to_replicate = pd.DataFrame(tables_to_replicate_raw_data)
    return tables_to_replicate


def metadata_to_sql(integrate_metadata, destination_metadata, tables_to_replicate, engine):
    integrate_metadata.to_sql('el_metadata', engine, if_exists='append', index=False)
    destination_metadata.to_sql('el_metadata', engine, if_exists='append', index=False)
    tables_to_replicate.to_sql('metadata_tables_to_replicate', engine, if_exists='append', index=False)


def get_correct_upsert_rule(tables):
    while True:
        print(about_querypie_elt.about_upsert_rule)
        upsert_rule = input("rule : ").replace(" ", "").split(',')
        if len(tables) == len(upsert_rule):
            break
        print(error_message.len_of_rules_are_not_correct_with_len_of_tables)
    upsert_rule_correcter(upsert_rule)
    return upsert_rule


def upsert_rule_correcter(upsert_rule):
    for i in range(len(upsert_rule)):
        while not is_in_upsert_rule_range(upsert_rule[i]):
            print(error_message.wrong_upsert_rule)
            print('error rule : ' + upsert_rule[i])
            upsert_rule[i] = input("rule : ")


def is_in_upsert_rule_range(thing):
    return thing in about_querypie_elt.upsert_rule_range


def get_tables(db_type):
    integrate_carrier = get_database_schema(db_type)
    while True:
        tables = input("tables : ").split(',')  # table 이름 중간에 , 가 들어간다면??
        if len(integrate_carrier.database) != len(tables):
            print(error_message.len_of_tables_and_len_of_database_are_not_correct)
            continue
        else:
            break
        print(error_message.no_table)

    integrate_carrier.tables = tables
    integrate_carrier.pk, integrate_carrier.updated = init_pk_and_updated(tables)
    return integrate_carrier


def get_database_have_no_schema():
    database = []
    while not database:
        database = input('database : ').split(',')
    return database


def get_database_have_schema():
    database = []
    while not database:
        database = input('database : ').split(',')
    while True:
        schema = input('schema : ').split(',')
        if len(schema) == len(database):
            break
    return database, schema


def get_database_schema(db_type):
    integrate_carrier = user_data_carrier()
    if db_type == db.mysql:
        integrate_carrier.database = get_database_have_no_schema()
    elif db_type in (db.snowflake, db.redshift, db.postgresql):
        integrate_carrier.database, integrate_carrier.schema = get_database_have_schema()
    return integrate_carrier


def set_columns_as_all(tables):
    return str(['*'] * len(tables))


def columns_is_not_list(columns):
    if columns[0] != '[' or columns[1] != "'" or columns[-1] != ']' or columns[-2] != "'":
        return True
    else:
        return False


def get_columns(tables):
    while True:
        columns = input('columns : ')
        if not (columns):
            columns = set_columns_as_all(tables)
            break
        if len(columns) == len(tables):
            break
        if columns_is_not_list(columns):
            print(error_message.columns_type_is_not_list)
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


def get_pk(tables, upsert_rule, pk, i, column_type):
    print('table: ' + tables[i] + ', rule : ' + upsert_rule[i])
    pk[i] = input(column_type + " column : ")
    while not (pk[i]):
        print(error_message.no_pk)
        pk[i] = input(column_type + ' column : ')


def get_updated(tables, upsert_rule, updated_at, i, column_type):
    print('table: ' + tables[i] + ', rule : ' + upsert_rule[i])
    updated_at[i] = input(column_type + " column : ")
    while not (updated_at[i]):
        print(error_message.no_updated_at)
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


def init_pk_and_updated(tables):
    pk = [''] * len(tables)
    updated = [''] * len(tables)
    return pk, updated
