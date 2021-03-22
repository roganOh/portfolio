# -*- coding:utf-8 -*-

""" Library for get_sql_alchemy_conn from airflow.cfg

I made this library with airflow.configuration.py
As a result, if version of airflow change, this may not work. This airflow version is 1.14.10

Available functions:
- get_airflow_config: get {airflow_home}
- get_sql_alchemy_conn: get sql_alchemy_conn from airflow.cfg
"""
import os

from backports.configparser import ConfigParser


def get_airflow_config(airflow_home):
    if 'AIRFLOW_CONFIG' not in os.environ:
        return os.path.join(airflow_home, 'airflow.cfg')


class AirflowConfigParser(ConfigParser):
    def __init__(self, default_config=None, *args, **kwargs):
        super(AirflowConfigParser, self).__init__(*args, **kwargs)

        self.airflow_defaults = ConfigParser(*args, **kwargs)
        if default_config is not None:
            self.airflow_defaults.read_string(default_config)

        self.is_validated = False

    def _validate(self):

        for section, replacement in self.deprecated_values.items():
            for name, info in replacement.items():
                old, new, version = info
                if self.get(section, name, fallback=None) == old:
                    # Make sure the env var option is removed, otherwise it
                    # would be read and used instead of the value we set
                    env_var = self._env_var_name(section, name)
                    os.environ.pop(env_var, None)

                    self.set(section, name, new)

        self.is_validated = True


conf = AirflowConfigParser()


def expand_env_var(env_var):
    """
    Expands (potentially nested) env vars by repeatedly applying
    `expandvars` and `expanduser` until interpolation stops having
    any effect.
    """
    if not env_var:
        return env_var
    while True:
        interpolated = os.path.expanduser(os.path.expandvars(str(env_var)))
        if interpolated == env_var:
            return interpolated
        else:
            env_var = interpolated


def get_airflow_config(airflow_home):
    if 'AIRFLOW_CONFIG' not in os.environ:
        return os.path.join(airflow_home, 'airflow.cfg')
    return expand_env_var(os.environ['AIRFLOW_CONFIG'])

def get_sql_alchemy_conn(airflow_home):
    AIRFLOW_HOME = os.path.abspath(airflow_home)
    AIRFLOW_CONFIG = get_airflow_config(AIRFLOW_HOME)
    conf.read(AIRFLOW_CONFIG)
    if conf.has_option('logging', 'sql_alchemy_conn'):
        return (conf.get('logging', 'sql_alchemy_conn'))


