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
from airflow.models import Variable

args = {
    'owner': 'linan',
    'start_date': datetime(2019, 12, 18),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'on_success_callback': on_success_callback,
}
schedule_interval = "30/* * * * *"

dag = airflow.DAG(
    'opay_binlog_hi',
    schedule_interval=schedule_interval,
    concurrency=40,
    max_active_runs=1,
    default_args=args)

dag_monitor = airflow.DAG(
    'opay_binlog_hi_monitor',
    schedule_interval=schedule_interval,
    default_args=args)


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, db_name, table_name, **op_kwargs):
    tb = [
        {"db": db_name, "table": table_name, "partition": "dt={pt}".format(pt=ds), "timeout": "7200"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


# 忽略数据量检查的table
IGNORED_TABLE_LIST = [
    'user_limit',
    'channel_router_rule',
]

'''
导入数据的列表
db_name,table_name,conn_id,prefix_name,priority_weight,server_name (采集配置，定位oss数据位置使用)
'''
#

table_list = [

    ("opay_user", "user_upgrade", "opay_user", "base", 3, "opay_user"),
    # ("opay_user","user", "opay_user", "base",1, "opay_user"),
    ("opay_user", "user_operator", "opay_user", "base", 1, "opay_user"),
    ("opay_user", "user_payment_instrument", "opay_user", "base", 1, "opay_user"),
    ("opay_user", "user_token", "opay_user", "base", 1, "opay_user"),
    ("opay_user", "user_telesale", "opay_user", "base", 1, "opay_user"),
    ("opay_user", "user_reseller", "opay_user", "base", 1, "opay_user"),
    ("opay_user", "user_push_token", "opay_user", "base", 1, "opay_user"),
    ("opay_user", "user_operator", "opay_user", "base", 1, "opay_user"),
    ("opay_user", "user_nearby_agent", "opay_user", "base", 1, "opay_user"),
    ("opay_user", "user_message", "opay_user", "base", 1, "opay_user"),

    ("opay_account", "account_user", "opay_account", "base", 2, "opay_account"),
    ("opay_account", "account_merchant", "opay_account", "base", 2, "opay_account"),
    ("opay_account", "accounting_merchant_record", "opay_account", "base", 1, "opay_account"),
    ("opay_account", "accounting_record", "opay_account", "base", 1, "opay_account"),

    ("opay_overlord", "overlord_user", "opay_overlord", "base", 1, "opay_merchant_overlord_recon"),

    ("opay_merchant", "merchant", "opay_merchant", "base", 1, "opay_merchant_overlord_recon"),

    ("opay_sms", "message_template", "opay_sms", "base", 1, "opay_idgen_xxljob_apollo"),

    ("opay_activity", "activity", "opay_activity", "base", 1, "opay_merchant_overlord_recon"),
    ("opay_activity", "activity_rules", "opay_activity", "base", 1, "opay_merchant_overlord_recon"),
    # ("opay_activity", "preferential_record", "opay_merchant_overlord_recon", "base", 1, "opay_merchant_overlord_recon"),

    ("opay_commission", "commission_account_balance", "opay_commission", "base", 1,
     "opay_merchant_overlord_recon"),
    ("opay_commission", "commission_order", "opay_commission", "base", 1, "opay_merchant_overlord_recon"),
    ("opay_commission", "commission_top_up_record", "opay_commission", "base", 1,
     "opay_merchant_overlord_recon"),
]

HIVE_DB = 'opay_dw_ods'
HIVE_TABLE = 'ods_binlog_%s_%s_hi'
OSS_PATH = 'oss://opay-datalake/opay_binlog/%s'
ODS_CREATE_TABLE_SQL = '''
    CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.`{table_name}`(
        `__db` string COMMENT 'from deserializer', 
        `__server_id` string COMMENT 'from deserializer', 
        `__file` string COMMENT 'from deserializer', 
        `__pos` string COMMENT 'from deserializer', 
        `__row` string COMMENT 'from deserializer', 
        `__table` string COMMENT 'from deserializer', 
        `__deleted` string COMMENT 'from deserializer', 
        `__version` string COMMENT 'from deserializer', 
        `__connector` string COMMENT 'from deserializer', 
        `__ts_ms` bigint COMMENT 'from deserializer', 
        {columns}
    )
    PARTITIONED BY (
      `dt` string,
      `hour` string
    )
    ROW FORMAT SERDE 
        'org.openx.data.jsonserde.JsonSerDe' 
    WITH SERDEPROPERTIES ( 
        'ignore.malformed.json'='true') 
    STORED AS INPUTFORMAT 
        'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 
        'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION
      '{oss_path}';
    MSCK REPAIR TABLE {db_name}.`{table_name}`;
    -- delete opay_dw table
    -- DROP TABLE IF EXISTS {db_name}.`{table_name}`;
'''

# 需要验证的核心业务表
table_core_list = [
    # ("oride_data", "data_order", "sqoop_db", "base", "create_time","priority_weight")
]

# 不需要验证的维度表，暂时为null
table_dim_list = []

# 需要验证的非核心业务表，根据需求陆续添加
table_not_core_list = []


def run_check_table(db_name, table_name, conn_id, hive_table_name, server_name, **kwargs):
    # SHOW TABLES in oride_db LIKE 'data_aa'
    check_sql = 'SHOW TABLES in %s LIKE \'%s\'' % (HIVE_DB, hive_table_name)
    hive2_conn = HiveServer2Hook().get_conn()
    cursor = hive2_conn.cursor()
    cursor.execute(check_sql)
    if len(cursor.fetchall()) == 0:
        logging.info('Create Hive Table: %s.%s', HIVE_DB, hive_table_name)
        # get table column
        column_sql = '''
                SELECT
                    COLUMN_NAME,
                    DATA_TYPE,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE,
                    COLUMN_COMMENT
                FROM
                    information_schema.columns
                WHERE
                    table_schema='{db_name}' and table_name='{table_name}'
            '''.format(db_name=db_name, table_name=table_name)
        mysql_hook = MySqlHook(conn_id)
        mysql_conn = mysql_hook.get_conn()
        mysql_cursor = mysql_conn.cursor()
        mysql_cursor.execute(column_sql)
        results = mysql_cursor.fetchall()
        rows = []
        for result in results:
            if result[0] == 'dt':
                col_name = '_dt'
            else:
                col_name = result[0]
            if result[1] == 'timestamp' or result[1] == 'varchar' or result[1] == 'char' or result[1] == 'text' or \
                    result[1] == 'longtext' or \
                    result[1] == 'datetime':
                data_type = 'string'
            elif result[1] == 'decimal':
                data_type = result[1] + "(" + str(result[2]) + "," + str(result[3]) + ")"
            else:
                data_type = result[1]
            rows.append("`%s` %s comment '%s'" % (col_name, data_type, result[4]))
        mysql_conn.close()

        # hive create table
        hive_hook = HiveCliHook()
        sql = ODS_CREATE_TABLE_SQL.format(
            db_name=HIVE_DB,
            table_name=hive_table_name,
            columns=",\n".join(rows),
            oss_path=OSS_PATH % ("{server_name}.{db_name}.{table_name}".format(
                server_name=server_name,
                db_name=db_name,
                table_name=table_name
            ))
        )
        logging.info('Executing: %s', sql)
        hive_hook.run_cli(sql)

    else:
        sqoopSchema = SqoopSchemaUpdate()
        response = sqoopSchema.append_hive_schema(
            hive_db=HIVE_DB,
            hive_table=hive_table_name,
            mysql_db=db_name,
            mysql_table=table_name,
            mysql_conn=conn_id
        )
        if response:
            return True
    return


conn_conf_dict = {}
for db_name, table_name, conn_id, prefix_name, priority_weight_nm, server_name in table_list:
    if conn_id not in conn_conf_dict:
        conn_conf_dict[conn_id] = BaseHook.get_connection(conn_id)

    hive_table_name = HIVE_TABLE % (prefix_name, table_name)

    # check table
    check_table = PythonOperator(
        task_id='check_table_{}'.format(hive_table_name),
        priority_weight=priority_weight_nm,
        python_callable=run_check_table,
        provide_context=True,
        op_kwargs={
            'db_name': db_name,
            'table_name': table_name,
            'conn_id': conn_id,
            'hive_table_name': hive_table_name,
            'server_name': server_name
        },
        dag=dag
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

    # if table_name in IGNORED_TABLE_LIST:
    #     add_partitions >> validate_all_data
    # else:
    #     # 数据量监控
    #     volume_monitoring = PythonOperator(
    #         task_id='volume_monitorin_{}'.format(hive_table_name),
    #         python_callable=data_volume_monitoring,
    #         provide_context=True,
    #         op_kwargs={
    #             'db_name': HIVE_DB,
    #             'table_name': hive_table_name,
    #             'is_valid_success': "true"
    #         },
    #         dag=dag
    #     )
    #     add_partitions >> volume_monitoring >> validate_all_data
    # 超时监控
    task_timeout_monitor = PythonOperator(
        task_id='task_timeout_monitor_{}'.format(hive_table_name),
        python_callable=fun_task_timeout_monitor,
        provide_context=True,
        op_kwargs={
            'db_name': HIVE_DB,
            'table_name': hive_table_name,
        },
        dag=dag_monitor
    )

    check_table >> add_partitions
