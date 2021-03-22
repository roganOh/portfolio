from enum import Enum


class error_message(Enum):

    def about_pep8(self): print("you should not contain space. You can only use english and _.")

    def unuseable_dag_id(self):
        error_message.about_pep8()
        print("Or this id is exist")

    def unuseable_db_type(self): print("not valuable db")

    def no_table(self): print("you should select tables")

    def len_of_rules_are_not_correct_with_len_of_tables(self): print('you should write all rules for each table')

    def columns_type_is_not_list(self): print('you should just press enter or input list of string')

    def wrong_upsert_rule(self): print('you can choose only in truncate, increasement, merge')

    def no_pk(self): print('you should write pk')

    def no_updated_at(self): print('you should write updated_at column')

    def len_of_tables_and_len_of_database_are_not_correct(self): print(
        "len of tables must be same with number of databases")

    def no_value(thing): print("you should write {thing}".format(thing))

    def dag_id_exists(name):
        print('job_name: {name} is exist. job name must be primary key. please write other job name'.foramt(name))

    def write_everything(self): print("write everything")


class about_querypie_elt(Enum):
    welcome = "Welcome rogan's ELT TOOL\nplease make inputs first for your ELT"
    about_dag_id = "Welcome rogan's ELT TOOL\nplease make inputs first for your ELT"
    about_schedule_interval = "schedule_interval :you must write on cron like * * * * *"
    about_db_type = "integreatedb_type :you can choose in mysql postgresql snowflake redshift "
    about_upsert_rule = "upsert_rule : you can choose in truncate, increasement, merge"
    about_columns = "columns must be like ['a,b,c,d','zz','123123,22']"
    skip_choose_columns = "columns : if you want to choose all *, just press enter"
    db_range = ['mysql', 'postgresql', 'snowflake', 'redshift']
    upsert_rule_range = ['truncate', 'increasement', 'merge']
    start_get_destination = 'destination_db_type :\nyou can choose db type in mysql, snowflake, postgresql, redshift'
