# -*- coding: utf-8 -*-
"""
对比mysql 与 hive 表结构，更新hive表结构
"""
import airflow
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from utils.connection_helper import get_hive_cursor, get_db_conn
import logging
from airflow.exceptions import AirflowException


class SqoopSchemaUpdate(object):
    """
    mysql 与 hive 字段类型对应关系
    """
    mysql_type_to_hive = {
        "TINYINT": "int",
        "SMALLINT": "int",
        "MEDIUMINT": "int",
        "INT": "int",
        "INTEGER": "int",
        "BIGINT": "bigint",
        "FLOAT": "float",
        "DOUBLE": "double",
        "DECIMAL": "decimal(38,2)"
    }

    hive_cursor = None
    mysql_cursor = {}

    """
    @:param mysql_conn
    """

    def __init__(self, mysql_conn=None):
        self.hive_cursor = get_hive_cursor()
        if mysql_conn:
            sqlconn = get_db_conn(mysql_conn)
            self.mysql_cursor[mysql_conn] = sqlconn.cursor()

    """
    关闭hive，mysql连接
    """

    def __del__(self):
        logging.info("析构函数")
        if self.hive_cursor:
            self.hive_cursor.close()
            logging.info("close hive connect")
        for k in self.mysql_cursor:
            c = self.mysql_cursor.get(k, None)
            if c:
                c.close()
                # del self.mysql_cursor[k]
                logging.info("close mysql connect")

    """
    获取hive指定表的结构
    @:param hive_db     hive表数据库
    @:param hive_table  hive数据表
    """

    def __get_hive_table_schema(self, hive_db, hive_table):
        hql = '''
            DESCRIBE FORMATTED {db}.{table} 
        '''.format(
            db=hive_db,
            table=hive_table
        )
        logging.info(hql)
        self.hive_cursor.execute(hql)
        res = self.hive_cursor.fetchall()
        # logging.info(res)
        hive_schema = []
        for (column_name, column_type, column_comment) in res:
            col_name = column_name.lower().strip()
            if col_name == '# col_name' or col_name == '':
                continue
            if col_name == '# partition information':
                break
            hive_schema.append(column_name)

        logging.info(hive_schema)
        return hive_schema

    """
    获取mysql指定表的结构
    @:param mysql_db    mysql数据库
    @:param mysql_table mysql数据表
    @:param mysql_conn  mysql数据库连接
    """

    def __get_mysql_table_schema(self, mysql_db, mysql_table, mysql_conn):
        mcursor = self.mysql_cursor.get(mysql_conn, None)
        if not mcursor:
            sqlconn = get_db_conn(mysql_conn)
            mcursor = self.mysql_cursor[mysql_conn] = sqlconn.cursor()

        sql = '''
            SELECT 
                COLUMN_NAME, 
                DATA_TYPE, 
                COLUMN_COMMENT,
                COLUMN_TYPE 
            FROM information_schema.COLUMNS 
            WHERE TABLE_SCHEMA='{db}' AND 
                TABLE_NAME='{table}' 
            ORDER BY ORDINAL_POSITION
        '''.format(
            db=mysql_db,
            table=mysql_table
        )
        logging.info(sql)
        mcursor.execute(sql)
        res = mcursor.fetchall()
        # logging.info(res)
        mysql_schema = []
        for (column_name, data_type, column_comment, column_type) in res:
            mysql_schema.append({
                'column': column_name,
                'column_info': "`%s` %s comment '%s'" % (
                    column_name, self.mysql_type_to_hive.get(data_type.upper(), 'string'), column_comment)
            })

        logging.info(mysql_schema)
        return mysql_schema

    """
        获取mysql指定表的结构 (返回tuple)
        @:param mysql_db    mysql数据库
        @:param mysql_table mysql数据表
        @:param mysql_conn  mysql数据库连接
        """

    def __get_mysql_table_schema_and_column_name(self, mysql_db, mysql_table, mysql_conn):
        mcursor = self.mysql_cursor.get(mysql_conn, None)
        if not mcursor:
            sqlconn = get_db_conn(mysql_conn)
            mcursor = self.mysql_cursor[mysql_conn] = sqlconn.cursor()

        sql = '''
                SELECT 
                    COLUMN_NAME, 
                    DATA_TYPE, 
                    COLUMN_COMMENT,
                    COLUMN_TYPE 
                FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA='{db}' AND 
                    TABLE_NAME='{table}' 
                ORDER BY ORDINAL_POSITION
            '''.format(
            db=mysql_db,
            table=mysql_table
        )
        logging.info(sql)
        mcursor.execute(sql)
        res = mcursor.fetchall()
        # logging.info(res)
        mysql_schema = []
        mysql_column = []
        for (column_name, data_type, column_comment, column_type) in res:
            mysql_schema.append({
                'column': column_name,
                'column_info': "`%s` %s comment '%s'" % (
                    column_name, self.mysql_type_to_hive.get(data_type.upper(), 'string'), column_comment)
            })
            mysql_column.append(column_name)

        logging.info(mysql_schema)
        return (mysql_schema, mysql_column)

    """
    对比并更新hive数据表结构
    @:param **info 
    {
        'hive_db': hive_db,
        'hive_table': hive_table,
        'mysql_db': mysql_db,
        'mysql_table': mysql_table,
        'mysql_conn': mysql_conn
    }
    """

    def update_hive_schema(self, **info):
        try:
            hive_db = info.get('hive_db', None)
            hive_table = info.get('hive_table', None)
            mysql_db = info.get('mysql_db', None)
            mysql_table = info.get('mysql_table', None)
            mysql_conn = info.get('mysql_conn', None)

            if hive_db is None or hive_table is None or mysql_db is None or mysql_table is None or mysql_conn is None:
                return None

            hive_schema = self.__get_hive_table_schema(hive_db, hive_table)
            mysql_schema = self.__get_mysql_table_schema(mysql_db, mysql_table, mysql_conn)
            if len(hive_schema) >= len(mysql_schema):
                return True

            columns = []
            for i in range(len(hive_schema), len(mysql_schema)):
                column_info = mysql_schema[i].get('column_info', None)
                if column_info:
                    columns.append(column_info)

            hql = '''
                ALTER TABLE {db}.{table} ADD COLUMNS ({columns})
            '''.format(
                db=hive_db,
                table=hive_table,
                columns=",\n".join(columns)
            )

            logging.info(hql)
            self.hive_cursor.execute(hql)
            return True
        except BaseException as e:
            logging.info("Exception Info::")
            logging.info(e)
            raise AirflowException('sqoop update hive schema error')

    def append_hive_schema(self, **info):
        try:
            hive_db = info.get('hive_db', None)
            hive_table = info.get('hive_table', None)
            mysql_db = info.get('mysql_db', None)
            mysql_table = info.get('mysql_table', None)
            mysql_conn = info.get('mysql_conn', None)

            if hive_db is None or hive_table is None or mysql_db is None or mysql_table is None or mysql_conn is None:
                return None

            hive_schema = self.__get_hive_table_schema(hive_db, hive_table)
            mysql_info = self.__get_mysql_table_schema_and_column_name(mysql_db, mysql_table, mysql_conn)
            mysql_schema = mysql_info[0]
            mysql_column = mysql_info[1]
            add_column = set(mysql_column).difference(set(hive_schema))

            if len(add_column) == 0:
                return True

            columns = []
            for i in add_column:
                for j in mysql_schema:
                    if mysql_column[j].get('column', None) == add_column[i]:
                        column_info = mysql_schema[j].get('column_info', None)
                        if column_info:
                            columns.append(column_info)

            hql = '''
                ALTER TABLE {db}.{table} ADD COLUMNS ({columns})
            '''.format(
                db=hive_db,
                table=hive_table,
                columns=",\n".join(columns)
            )

            self.hive_cursor.execute(hql)
            logging.info(hql)
            return True
        except BaseException as e:
            logging.info("Exception Info::")
            logging.info(e)
            raise AirflowException('sqoop append hive schema error')

        pass
