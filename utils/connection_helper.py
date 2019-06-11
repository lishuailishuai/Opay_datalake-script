# coding=utf-8
from impala.dbapi import connect
from airflow.hooks.base_hook import BaseHook
from redis import StrictRedis
from ufile import config, filemanager
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


def get_redis_connection(conf_name='redis'):
    conn = BaseHook.get_connection(conf_name)
    return StrictRedis(host=conn.host, port=conn.port)


def get_pika_connection(conf_name='pika'):
    conn = BaseHook.get_connection(conf_name)
    return StrictRedis(host=conn.host, port=conn.port)


def get_ucloud_file_manager(conf_name='ucloud_oride_resource'):
    conn = BaseHook.get_connection(conf_name)
    config.set_default(uploadsuffix=conn.host)
    config.set_default(connection_timeout=60)
    config.set_default(expires=60)
    return filemanager.FileManager(conn.login, conn.password)


def get_google_map_js_api_key(conf_name='google_map_js'):
    conn = BaseHook.get_connection(conf_name)
    return conn.password

def get_db_conf(conf_name='mysql_default'):
    conf = BaseHook.get_connection(conf_name)
    return conf.host, conf.port, conf.schema, conf.login, conf.password