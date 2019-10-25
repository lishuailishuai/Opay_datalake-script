import airflow
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from utils.validate_metrics_utils import *
import logging
from plugins.SqoopSchemaUpdate import SqoopSchemaUpdate
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from utils.util import on_success_callback

args = {
    'owner': 'linan',
    'start_date': datetime(2019, 10, 20),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'on_success_callback': on_success_callback,
}

dag = airflow.DAG(
    'opos_source_sqoop_di',
    schedule_interval="20 01 * * *",
    concurrency=15,
    max_active_runs=1,
    default_args=args)


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, db_name, table_name, **op_kwargs):
    tb = [
        {"db": db_name, "table": table_name, "partition": "dt={pt}".format(pt=ds), "timeout": "7200"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


'''
导入数据的列表
db_name,table_name,conn_id,prefix_name,priority_weight
'''
#

table_list = [
    ("ptsp_db", "opos_payment_order", "opos_ptsp_db", "base", 3),
]

HIVE_DB = 'opos_dw_ods'
HIVE_TABLE = 'ods_sqoop_%s_%s_di'
UFILE_PATH = 'ufile://opay-datalake/opos_dw_sqoop_di/%s/%s'
ODS_CREATE_TABLE_SQL = '''
    CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.`{table_name}`(
        {columns}
    )
    PARTITIONED BY (
      `dt` string)
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION
      '{ufile_path}';
'''

# 需要验证的核心业务表
table_core_list = [
    # ("oride_data", "data_order", "sqoop_db", "base", "create_time","priority_weight")
]

# 不需要验证的维度表，暂时为null
table_dim_list = []

# 需要验证的非核心业务表，根据需求陆续添加
table_not_core_list = []

conn_conf_dict = {}
for db_name, table_name, conn_id, prefix_name, priority_weight_nm in table_list:
    if conn_id not in conn_conf_dict:
        conn_conf_dict[conn_id] = BaseHook.get_connection(conn_id)

    hive_table_name = HIVE_TABLE % (prefix_name, table_name)

    # sqoop import
    import_table = BashOperator(
        task_id='import_table_{}'.format(hive_table_name),
        priority_weight=priority_weight_nm,
        bash_command='''
            #!/usr/bin/env bash
            sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
            -D mapred.job.queue.name=root.collects \
            --connect "jdbc:mysql://{host}:{port}/{schema}?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8" \
            --username {username} \
            --password {password} \
            --query 'select * from {table} where (DATE_FORMAT(create_time,"%Y-%m-%d")="{{{{ ds }}}}" OR DATE_FORMAT(modify_time,"%Y-%m-%d")="{{{{ ds }}}}") AND $CONDITIONS' \
            --split-by id \
            --target-dir {ufile_path}/dt={{{{ ds }}}}/ \
            --fields-terminated-by "\\001" \
            --lines-terminated-by "\\n" \
            --hive-delims-replacement " " \
            --delete-target-dir \
            --compression-codec=snappy \
            -m 12
        '''.format(
            host=conn_conf_dict[conn_id].host,
            port=conn_conf_dict[conn_id].port,
            schema=db_name,
            username=conn_conf_dict[conn_id].login,
            password=conn_conf_dict[conn_id].password,
            table=table_name + '_' + '{{ execution_date.strftime("%Y") }}' + '_' + '{{ execution_date.strftime("%W") }}',
            ufile_path=UFILE_PATH % (db_name, table_name)
        ),
        dag=dag,
    )

    # add partitions
    add_partitions = HiveOperator(
        task_id='add_partitions_{}'.format(hive_table_name),
        priority_weight=priority_weight_nm,
        hql='''
                ALTER TABLE {table} ADD IF NOT EXISTS PARTITION (dt = '{{{{ ds }}}}')
            '''.format(table=hive_table_name),
        schema=HIVE_DB,
        dag=dag)

    validate_all_data = PythonOperator(
        task_id='validate_data_{}'.format(hive_table_name),
        priority_weight=priority_weight_nm,
        python_callable=validata_data,
        provide_context=True,
        op_kwargs={
            'db': HIVE_DB,
            'table_name': hive_table_name,
            'table_format': HIVE_TABLE,
            'table_core_list': table_core_list,
            'table_not_core_list': table_not_core_list
        },
        dag=dag
    )

    # 超时监控
    task_timeout_monitor = PythonOperator(
        task_id='task_timeout_monitor_{}'.format(hive_table_name),
        priority_weight=priority_weight_nm,
        python_callable=fun_task_timeout_monitor,
        provide_context=True,
        op_kwargs={
            'db_name': HIVE_DB,
            'table_name': hive_table_name,
        },
        dag=dag
    )

    import_table >> add_partitions >> validate_all_data
