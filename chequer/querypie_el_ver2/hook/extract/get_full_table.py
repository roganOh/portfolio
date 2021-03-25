import pandas as pd

def get_full_table_snowflake(i, metadatas, engine):
    filename = metadatas.dag_id[i]
    indata = pd.read_sql_query(
        "select {column} from {database}.{schema}.{dag_id}_{table}".format(column=metadatas.columns[i].lower(),
                                                                           table=filename.lower(),
                                                                           database=metadatas.database.lower(),
                                                                           schema=metadatas.schema.lower(),
                                                                           dag_id=metadatas.dag_id.lower()), engine)
    indata.to_csv(metadatas.csv_files_directory + '/' + metadatas.dag_id + '_' + filename + '.csv', sep=',',
                  quotechar="'", na_rep='NaN', index=False)


def get_full_table_amazon(i, metadatas, engine):
    filename = metadatas.dag_id[i]
    indata = pd.read_sql_query(
        "select {column} from {database}.{schema}.{dag_id}_{table}".format(column=metadatas.columns[i],
                                                                           table=filename,
                                                                           database=metadatas.database,
                                                                           schema=metadatas.schema,
                                                                           dag_id=metadatas.dag_id), engine)
    indata.to_csv(metadatas.csv_files_directory + '/' + metadatas.dag_id + '_' + filename + '.csv', sep=',',
                  quotechar="'", na_rep='NaN', index=False)


def get_full_table_mysql(i, metadatas, engine):
    filename = metadatas.dag_id[i]
    indata = pd.read_sql_query(
        "select {column} from {database}.{dag_id}_{table}".format(column=metadatas.columns[i],
                                                                  table=filename,
                                                                  database=metadatas.database,
                                                                  dag_id=metadatas.dag_id), engine)
    indata.to_csv(metadatas.csv_files_directory + '/' + metadatas.dag_id + '_' + metadatas.filename + '.csv', sep=',',
                  quotechar="'", na_rep='NaN', index=False)