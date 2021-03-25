import pandas as pd
import json

def make_db_info_into_json(db_raw):
    db_information = db_raw.replace("'", "\"")
    for i in range(len(db_information)):
        if db_information[i] == '[':
            i = i + 1
            while (db_information[i] != ']'):
                if db_information[i] == "\"":
                    db_information = db_information[:i] + "'" + db_information[i + 1:]
                i = i + 1
    return db_information


def get_db_info(dag_id,backend_engine):
    ds = pd.read_sql_query(
        "select * from metadata where dag_id='{dag_id}' and info_type='load'".format(dag_id=dag_id.lower()),
        backend_engine)
    # if send dataframe to sql, data type become str and surround by '
    # I must magage with json but json is srrounded by " so I must do preprocess
    db_raw = ds['db_information'][0]
    db_information = make_db_info_into_json(db_raw)

    return ds, json.loads(db_information)
