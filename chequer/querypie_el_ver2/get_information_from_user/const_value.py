class error_message:
    about_pep8 = "you should not contain space. You can only use english and _."
    unuseable_dag_id = about_pep8 + "Or this id is exist"
    unuseable_db_type = "not valuable db"
    no_table = "you should select tables"
    len_of_rules_are_not_correct_with_len_of_tables = 'you should write all rules for each table'
    columns_type_is_not_list = 'you should just press enter or input list of string'
    wrong_upsert_rule = 'you can choose only in truncate, increasement, merge'
    no_pk = 'you should write pk'
    no_updated_at = 'you should write updated_at column'
    len_of_tables_and_len_of_database_are_not_correct = "len of tables must be same with number of databases"
    write_everything = "write everything"


def error_no_value(thing): print("you should write {thing}".format(thing=thing))


def error_dag_id_exists(name):
    print('job_name: {name} is exist. job name must be primary key. please write other job name'.foramt(name=name))


class about_querypie_elt:
    welcome = "Welcome rogan's ELT TOOL\nplease make inputs first for your ELT"
    about_dag_id = "Please write your job name"
    about_schedule_interval = "schedule_interval :you must write on cron like * * * * *"
    about_db_type = "db_type :you can choose in mysql postgresql snowflake redshift "
    about_upsert_rule = "upsert_rule : you can choose in truncate, increasement, merge"
    about_columns = "columns must be like ['a,b,c,d','zz','123123,22']"
    skip_choose_columns = "columns : if you want to choose all *, just press enter"
    db_range = ['mysql', 'postgresql', 'snowflake', 'redshift']
    upsert_rule_range = ['truncate', 'increasement', 'merge']

class db:
    mysql = 0
    snowflake = 1
    redshift = 2
    postgresql = 3