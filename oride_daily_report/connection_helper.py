# coding=utf-8
from impala.dbapi import connect
from airflow.hooks.base_hook import BaseHook
import MySQLdb

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


def get_db_conn(conf_name='mysql_default'):
    mysql_conn_conf = BaseHook.get_connection(conf_name)
    conn_mysql = MySQLdb.connect(host=mysql_conn_conf.host,
                             port=mysql_conn_conf.port,
                             db=mysql_conn_conf.schema,
                             user=mysql_conn_conf.login,
                             passwd=mysql_conn_conf.password,
                             charset='utf8mb4',
                             use_unicode=True,
                             autocommit=True)
    return conn_mysql