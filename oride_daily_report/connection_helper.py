# coding=utf-8
from impala.dbapi import connect
from airflow.hooks.base_hook import BaseHook


def get_hive_cursor(conf_name='hive_cli_default'):
    conn = BaseHook.get_connection(conf_name)
    conn_hive = connect(host=conn.host,
                        port=conn.port,
                        timeout=3600,
                        auth_mechanism='PLAIN',
                        user=conn.login,
                        password=conn.password)
    cursor = conn_hive.cursor()
    return cursor