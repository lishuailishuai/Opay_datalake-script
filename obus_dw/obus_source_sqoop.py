# -*- coding: utf-8 -*-
"""
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
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from utils.connection_helper import get_hive_cursor, get_db_conn, get_db_conf
from utils.validate_metrics_utils import *
import logging
from plugins.SqoopSchemaUpdate import SqoopSchemaUpdate
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from utils.util import on_success_callback



args = {
    'owner': 'wuduo',
    'start_date': datetime(2019, 8, 25),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'on_success_callback':on_success_callback,
}

dag = airflow.DAG(
    'obus_source_sqoop',
    schedule_interval="05 04 * * *",
    concurrency=15,
    max_active_runs=1,
    default_args=args
)

##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, db_name, table_name, **op_kwargs):
    tb = [
        {"db": db_name, "table":table_name, "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "7200"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


# 忽略数据量检查的table
IGNORED_TABLE_LIST = [
    'data_driver_feedback',
    'data_order_copy',
    'log_balance',
    'data_driver_recharge_records',
    'data_device_extend',
]

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

hive_db = 'obus_dw_ods'
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


def create_hive_external_table(db, table, conn, **op_kwargs):
    sqoopSchema = SqoopSchemaUpdate()
    response = sqoopSchema.update_hive_schema(
        hive_db=hive_db,
        hive_table=hive_table.format(bs=table),
        mysql_db=db,
        mysql_table=table,
        mysql_conn=conn
    )
    if response:
        return True

    mysql_conn = get_db_conn(conn)
    mcursor = mysql_conn.cursor()
    sql = '''
        select 
            COLUMN_NAME, 
            DATA_TYPE, 
            COLUMN_COMMENT,
            COLUMN_TYPE 
        from information_schema.COLUMNS 
        where TABLE_SCHEMA='{db}' and 
            TABLE_NAME='{table}' 
        order by ORDINAL_POSITION
    '''.format(db=db, table=table)
    # logging.info(sql)
    mcursor.execute(sql)
    res = mcursor.fetchall()
    # logging.info(res)
    columns = []
    for (name, type, comment, co_type) in res:
        if type.upper() == 'DECIMAL':
            columns.append("`%s` %s comment '%s'" % (name, co_type.replace('unsigned', '').replace('signed', ''), comment))
        else:
            columns.append("`%s` %s comment '%s'" % (name, mysql_type_to_hive.get(type.upper(), 'string'), comment))
    # 创建hive数据表的sql
    hql = ods_create_table_hql.format(
        db_name=hive_db,
        table_name=hive_table.format(bs=table),
        columns=",\n".join(columns),
        s3path=s3path.format(bs=table)
    )
    # logging.info(hql)
    hive_cursor = get_hive_cursor()
    hive_cursor.execute(hql)
    mcursor.close()
    hive_cursor.close()


success = DummyOperator(dag=dag, task_id='success')
conn_conf_dict = {}
for obus_table in obus_table_list:
    # logging.info(obus_table)
    conn_id = obus_table.get('conn')
    if conn_id not in conn_conf_dict:
        conn_conf_dict[conn_id] = get_db_conf(conn_id)

    host, port, schema, login, password = conn_conf_dict[conn_id]
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

    if obus_table.get('table') in IGNORED_TABLE_LIST:
        add_partitions >> validate_all_data
    else:
        # 数据量监控
        volume_monitoring = PythonOperator(
            task_id='volume_monitorin_{}'.format(hive_table.format(bs=obus_table.get('table'))),
            python_callable=data_volume_monitoring,
            provide_context=True,
            op_kwargs={
                'db_name': hive_db,
                'table_name': hive_table.format(bs=obus_table.get('table')),
            },
            dag=dag
        )
        add_partitions >> volume_monitoring >> validate_all_data


    # 超时监控
    task_timeout_monitor= PythonOperator(
        task_id='task_timeout_monitor_{}'.format(hive_table.format(bs=obus_table.get('table'))),
        python_callable=fun_task_timeout_monitor,
        provide_context=True,
        op_kwargs={
            'db_name': hive_db,
            'table_name': hive_table.format(bs=obus_table.get('table')),
        },
        dag=dag
    )

    # 加入调度队列
    import_from_mysql >> create_table >> add_partitions
    validate_all_data >> success

