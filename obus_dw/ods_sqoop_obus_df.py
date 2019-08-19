﻿"""
obus 业务数据采集
"""
import airflow
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.impala_plugin import ImpalaOperator
from datetime import datetime, timedelta
from utils.connection_helper import get_hive_cursor, get_db_conn, get_db_conf
from utils.validate_metrics_utils import *
import logging


args = {
    'owner': 'wuduo',
    'start_date': datetime(2019, 8, 16),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'obus_source_sqoop',
    schedule_interval="00 01 * * *",
    concurrency=5,
    max_active_runs=1,
    default_args=args
)

obus_table_list = [
    {"db": "obus_data", "table": "conf_capped_price",                "conn": "obus_db"},
    {"db": "obus_data", "table": "conf_city",                        "conn": "obus_db"},
    {"db": "obus_data", "table": "conf_cycle",                       "conn": "obus_db"},
    {"db": "obus_data", "table": "conf_line",                        "conn": "obus_db"},
    {"db": "obus_data", "table": "conf_line_points",                 "conn": "obus_db"},
    {"db": "obus_data", "table": "conf_line_stations",               "conn": "obus_db"},
    {"db": "obus_data", "table": "conf_recharge",                    "conn": "obus_db"},
    {"db": "obus_data", "table": "conf_recharge_options",            "conn": "obus_db"},
    {"db": "obus_data", "table": "conf_station",                     "conn": "obus_db"},
    {"db": "obus_data", "table": "conf_system",                      "conn": "obus_db"},
    {"db": "obus_data", "table": "data_bus",                         "conn": "obus_db"},
    {"db": "obus_data", "table": "data_device",                      "conn": "obus_db"},
    {"db": "obus_data", "table": "data_device_extend",               "conn": "obus_db"},
    {"db": "obus_data", "table": "data_driver",                      "conn": "obus_db"},
    {"db": "obus_data", "table": "data_driver_balance_extend",       "conn": "obus_db"},
    {"db": "obus_data", "table": "data_driver_balance_records",      "conn": "obus_db"},
    {"db": "obus_data", "table": "data_driver_fee_blacklist",        "conn": "obus_db"},
    {"db": "obus_data", "table": "data_driver_feedback",             "conn": "obus_db"},
    {"db": "obus_data", "table": "data_driver_operation_log",        "conn": "obus_db"},
    {"db": "obus_data", "table": "data_driver_pay_opay",             "conn": "obus_db"},
    {"db": "obus_data", "table": "data_driver_pay_records",          "conn": "obus_db"},
    {"db": "obus_data", "table": "data_driver_recharge_records",     "conn": "obus_db"},
    {"db": "obus_data", "table": "data_driver_recharge_type",        "conn": "obus_db"},
    {"db": "obus_data", "table": "data_driver_records_day",          "conn": "obus_db"},
    {"db": "obus_data", "table": "data_driver_records_detail",       "conn": "obus_db"},
    {"db": "obus_data", "table": "data_driver_records_type",         "conn": "obus_db"},
    {"db": "obus_data", "table": "data_driver_trip",                 "conn": "obus_db"},
    {"db": "obus_data", "table": "data_driver_trip_log",             "conn": "obus_db"},
    {"db": "obus_data", "table": "data_driver_work_log",             "conn": "obus_db"},
    {"db": "obus_data", "table": "data_fcm_template",                "conn": "obus_db"},
    {"db": "obus_data", "table": "data_opay_transaction",            "conn": "obus_db"},
    {"db": "obus_data", "table": "data_order",                       "conn": "obus_db"},
    {"db": "obus_data", "table": "data_order_copy",                  "conn": "obus_db"},
    {"db": "obus_data", "table": "data_order_payment",               "conn": "obus_db"},
    {"db": "obus_data", "table": "data_scanner",                     "conn": "obus_db"},
    {"db": "obus_data", "table": "data_sms_template",                "conn": "obus_db"},
    {"db": "obus_data", "table": "data_ticket",                      "conn": "obus_db"},
    {"db": "obus_data", "table": "data_ticket_batch",                "conn": "obus_db"},
    {"db": "obus_data", "table": "data_ticket_log",                  "conn": "obus_db"},
    {"db": "obus_data", "table": "data_translation",                 "conn": "obus_db"},
    {"db": "obus_data", "table": "data_user",                        "conn": "obus_db"},
    {"db": "obus_data", "table": "data_user_feedback",               "conn": "obus_db"},
    {"db": "obus_data", "table": "data_user_recharge",               "conn": "obus_db"},
    {"db": "obus_data", "table": "data_ussd",                        "conn": "obus_db"},
    {"db": "obus_data", "table": "log_balance",                      "conn": "obus_db"},
    {"db": "obus_data", "table": "log_ticket_bind",                  "conn": "obus_db"}
]

hive_db = 'obus_dw'
hive_table = 'ods_sqoop_{bs}_df'
s3path = 's3a://opay-bi/obus_dw/ods_sqoop_{bs}_df'
ods_create_table_hql = '''
    create EXTERNAL table if not exists {db_name}.{table_name} (
        {columns}
    )
    PARTITIONED BY (
        `country_code` string COMMENT '二位国家码',
        `dt` string comment '日期'
    )
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION
      '{s3path}'
'''

mysql_type_to_hive = {
    "TINYINT": "int",
    "SMALLINT": "int",
    "MEDIUMINT": "int",
    "INT": "int",
    "INTEGER": "int",
    "BIGINT": "bigint",
    "FLOAT": "float",
    "DOUBLE": "double",
    "DECIMAL": "decimal"
}

hive_cursor = get_hive_cursor()
def create_hive_external_table(db, table, conn, **op_kwargs):
    mysql_conn = get_db_conn(conn)
    mcursor = mysql_conn.cursor()
    sql = '''
        select 
            COLUMN_NAME, 
            DATA_TYPE, 
            COLUMN_COMMENT 
        from information_schema.COLUMNS 
        where TABLE_SCHEMA='{db}' and 
            TABLE_NAME='{table}' 
        order by ORDINAL_POSITION
    '''.format(db=db, table=table)
    logging.info(sql)
    mcursor.execute(sql)
    res = mcursor.fetchall()
    logging.info(res)
    columns = []
    for (name, type, comment) in res:
        columns.append("%s %s comment '%s'" % (name, mysql_type_to_hive.get(type, 'string'), comment))
    #创建hive数据表的sql
    hql = ods_create_table_hql.format(
        db_name=hive_db,
        table_name=hive_table.format(bs=table),
        columns=",\n".join(columns),
        s3path=s3path.format(bs=table)
    )
    logging.info(hql)
    hive_cursor.execute(hql)


for obus_table in obus_table_list:
    logging.info(obus_table)
    host, port, schema, login, password = get_db_conf(obus_table.get('conn'))
    '''
    使用sqoop导入mysql数据到hive
    '''
    import_from_mysql = BashOperator(
        task_id='import_from_mysql_{}'.format(obus_table.get('db')+"_"+obus_table.get('table')),
        bash_command='''
            #!/usr/bin/env bash
            sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
            -D mapred.job.queue.name=root.collects \
            --connect "jdbc:mysql://{host}:{port}/{schema}?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8" \
            --username {username} \
            --password \'{password}\' \
            --table {table} \
            --target-dir {table_path}/country_code=nal/dt={{{{ ds }}}}/ \
            --fields-terminated-by "\\001" \
            --lines-terminated-by "\\n" \
            --hive-delims-replacement " " \
            --delete-target-dir \
            --compression-codec=snappy
        '''.format(
            host=host,
            port=3306,
            schema=schema,
            username=login,
            password=password,
            table=obus_table.get('table'),
            table_path=s3path.format(bs=obus_table.get('table'))
        ),
        dag=dag
    )

    '''
    创建hive数据表任务
    '''
    create_table = PythonOperator(
        task_id='create_table_{}'.format(hive_table.format(bs=obus_table.get('table'))),
        python_callable=create_hive_external_table,
        provide_context=True,
        op_kwargs={
            'db': obus_table.get('db'),
            'table': obus_table.get('table'),
            'conn': obus_table.get('conn')
        },
        dag=dag
    )

    '''
    添加hive数据表分区
    '''
    add_partitions = HiveOperator(
        task_id='add_partitions_{}'.format(hive_table.format(bs=obus_table.get('table'))),
        hql='''
            ALTER TABLE {hive_db}.{table} ADD IF NOT EXISTS PARTITION (country_code='nal', dt='{{{{ ds }}}}')
        '''.format(
            hive_db=hive_db,
            table=hive_table.format(bs=obus_table.get('table'))
        ),
        schema=hive_db,
        dag=dag
    )

    '''
    检查数据导入是否正确
    '''
    validate_all_data = PythonOperator(
        task_id='validate_data_{}'.format(hive_table.format(bs=obus_table.get('table'))),
        python_callable=validata_data,
        provide_context=True,
        op_kwargs={
            'db': hive_db,
            'table_name': hive_table.format(bs=obus_table.get('table')),
            'table_format': hive_table,
            'table_core_list': [],
            'table_not_core_list': []
        },
        dag=dag
    )

    '''
    刷新impala数据库
    '''
    refresh_impala = ImpalaOperator(
        task_id='refresh_impala_{}'.format(hive_table.format(bs=obus_table.get('table'))),
        hql="""
            REFRESH {table};
        """.format(table=hive_table.format(bs=obus_table.get('table'))),
        schema=hive_db,
        priority_weight=50,
        dag=dag
    )

    #加入调度队列
    import_from_mysql >> create_table >> add_partitions >> validate_all_data >> refresh_impala
