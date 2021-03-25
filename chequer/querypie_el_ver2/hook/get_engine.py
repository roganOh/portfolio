import sqlalchemy as sql

def get_snowflake_engine(db_information):
    if db_information['role'] == '':
        url_role = ''
    else:
        url_role = '&role={}'.format(db_information['role'])
    engine = sql.create_engine(
        'snowflake://{u}:{p}@{a}/{d}/{s}?warehouse={w}&role={r}'.format(
            u=db_information['id'],
            p=db_information['pwd'],
            a=db_information['account'],
            r=url_role,
            d=db_information['database'],
            s=db_information['schema'],
            w=db_information['warehouse']
        ))
    return engine


def get_mysql_engine(db_information):
    engine = sql.create_engine('mysql+pymysql://{u}:{p}@{h}:{port}/{d}{option}'.format(
        u=db_information['id'],
        p=db_information['pwd'],
        h=db_information['host'],
        port=db_information['port'],
        d=db_information['database'],
        option=db_information['option']
    ), encoding='utf-8')
    return engine


def get_redshift_engine(db_information):
    url = 'redshift+psycopg2://{u}:{p}@{h}:{port}/{d}'.format(
        u=db_information['id'],
        p=db_information['pwd'],
        h=db_information['host'],
        port=db_information['port'],
        d=db_information['database']
    )
    engine = sql.create_engine(url, client_encoding='utf8')
    return engine


def get_postgresql_engine(db_information):
    url = 'postgresql://{u}:{p}@{h}:{port}/{d}'.format(
        u=db_information['id'],
        p=db_information['pwd'],
        h=db_information['host'],
        port=db_information['port'],
        d=db_information['database']
    )
    engine = sql.create_engine(url, client_encoding='utf8')
    return engine


def get_engine(db_type, db_information):
    if db_type == 'snowflake':
        return get_snowflake_engine(db_information)
    elif db_type == 'mysql':
        return get_mysql_engine(db_information)
    elif db_type == 'redshift':
        return get_redshift_engine(db_information)
    elif db_type == 'postgresql':
        return get_postgresql_engine(db_information)