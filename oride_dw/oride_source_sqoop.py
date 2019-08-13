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

args = {
    'owner': 'root',
    'start_date': datetime(2019, 7, 17),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_source_sqoop',
    schedule_interval="00 01 * * *",
    concurrency=5,
    max_active_runs=1,
    default_args=args)

'''
导入数据的列表
db_name,table_name,conn_id,prefix_name
'''
#

table_list = [
    ("oride_data", "data_order", "sqoop_db", "base"),
    ("oride_data", "data_order_payment", "sqoop_db", "base"),
    ("oride_data", "data_user", "sqoop_db", "base"),
    ("oride_data", "data_user_extend", "sqoop_db", "base"),
    ("oride_data", "data_coupon", "sqoop_db", "base"),
    ("oride_data", "data_driver", "sqoop_db", "base"),
    ("oride_data", "data_driver_group", "sqoop_db", "base"),
    ("oride_data", "data_driver_extend", "sqoop_db", "base"),
    ("oride_data", "data_driver_comment", "sqoop_db", "base"),
    ("oride_data", "data_abnormal_order", "sqoop_db", "base"),
    ("oride_data", "data_anti_fraud_strategy", "sqoop_db", "base"),
    ("oride_data", "data_city_conf", "sqoop_db", "base"),
    ("oride_data", "", "sqoop_db", "base"),
    ("oride_data", "data_order_expired", "sqoop_db", "base"),
    ("oride_data", "data_device_extend", "sqoop_db", "base"),
    ("oride_data", "data_driver_recharge_records", "sqoop_db", "base"),
    ("oride_data", "data_driver_reward", "sqoop_db", "base"),

    ("bi", "weather_per_10min", "mysql_bi", "base"),
    # 协会数据
    # 数据库 opay_spread
    ("opay_spread", "driver_data", "opay_spread_mysql", "mass"),
    ("opay_spread", "driver_group", "opay_spread_mysql", "mass"),
    ("opay_spread", "driver_logs", "opay_spread_mysql", "mass"),
    ("opay_spread", "driver_team", "opay_spread_mysql", "mass"),
    ("opay_spread", "rider_signup", "opay_spread_mysql", "mass"),
    ("opay_spread", "rider_signups", "opay_spread_mysql", "mass"),
    ("opay_spread", "rider_signups_agents", "opay_spread_mysql", "mass"),
    ("opay_spread", "rider_signups_guarantors", "opay_spread_mysql", "mass"),
    ("opay_spread", "rider_signups_logs", "opay_spread_mysql", "mass"),
    # 数据库：oride_assets
    ("oride_assets", "oride_assets_transit", "opay_spread_mysql", "mass"),
    ("oride_assets", "oride_categories", "opay_spread_mysql", "mass"),
    ("oride_assets", "oride_my_storage", "opay_spread_mysql", "mass"),
    ("oride_assets", "oride_properties", "opay_spread_mysql", "mass"),
    ("oride_assets", "oride_property_customs", "opay_spread_mysql", "mass"),
    ("oride_assets", "oride_repair", "opay_spread_mysql", "mass"),
    ("oride_assets", "oride_retrieve", "opay_spread_mysql", "mass"),
    ("oride_assets", "oride_storage", "opay_spread_mysql", "mass"),
    ("oride_assets", "oride_storage_logs", "opay_spread_mysql", "mass"),
    ("oride_assets", "oride_user_ware", "opay_spread_mysql", "mass"),
    ("oride_assets", "oride_vehicles", "opay_spread_mysql", "mass"),
    ("oride_assets", "oride_vehicles_log", "opay_spread_mysql", "mass"),
    ("oride_assets", "oride_vehicles_transit", "opay_spread_mysql", "mass"),
    ("oride_assets", "oride_warehouses", "opay_spread_mysql", "mass"),
    # 地推数据源
    # 数据库：opay_spread
    ("opay_spread", "promoter_channel_day", "opay_spread_mysql", "promoter"),
    ("opay_spread", "promoter_data_day", "opay_spread_mysql", "promoter"),
    ("opay_spread", "promoter_data_hour", "opay_spread_mysql", "promoter"),
    ("opay_spread", "promoter_driver_day", "opay_spread_mysql", "promoter"),
    ("opay_spread", "promoter_logs", "opay_spread_mysql", "promoter"),
    ("opay_spread", "promoter_manager", "opay_spread_mysql", "promoter"),
    ("opay_spread", "promoter_order_day", "opay_spread_mysql", "promoter"),
    ("opay_spread", "promoter_team", "opay_spread_mysql", "promoter"),
    ("opay_spread", "promoter_user", "opay_spread_mysql", "promoter"),
    ("opay_spread", "promoter_user_relat_admin", "opay_spread_mysql", "promoter"),
    ("opay_spread", "promoter_users_device", "opay_spread_mysql", "promoter"),
]

HIVE_DB = 'oride_dw'
HIVE_TABLE = 'ods_sqoop_%s_%s_df'
UFILE_PATH = 'ufile://opay-datalake/oride_dw_sqoop/%s/%s'
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
    # ("oride_data", "data_order", "sqoop_db", "base", "create_time")
]

# 不需要验证的维度表，暂时为null
table_dim_list = []

# 需要验证的非核心业务表，根据需求陆续添加
table_not_core_list = []


def run_check_table(db_name, table_name, conn_id, hive_table_name, **kwargs):
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
                NUMERIC_SCALE,COLUMN_COMMENT
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
            ufile_path=UFILE_PATH % (db_name, table_name)
        )
        logging.info('Executing: %s', sql)
        hive_hook.run_cli(sql)
    return


conn_conf_dict = {}
for db_name, table_name, conn_id, prefix_name in table_list:
    if conn_id not in conn_conf_dict:
        conn_conf_dict[conn_id] = BaseHook.get_connection(conn_id)

    hive_table_name = HIVE_TABLE % (prefix_name, table_name)
    # sqoop import
    import_table = BashOperator(
        task_id='import_table_{}'.format(hive_table_name),
        bash_command='''
            #!/usr/bin/env bash
            sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
            --connect "jdbc:mysql://{host}:{port}/{schema}?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8" \
            --username {username} \
            --password {password} \
            --table {table} \
            --target-dir {ufile_path}/dt={{{{ ds }}}}/ \
            --fields-terminated-by "\\001" \
            --lines-terminated-by "\\n" \
            --hive-delims-replacement " " \
            --delete-target-dir \
            --compression-codec=snappy
        '''.format(
            host=conn_conf_dict[conn_id].host,
            port=conn_conf_dict[conn_id].port,
            schema=db_name,
            username=conn_conf_dict[conn_id].login,
            password=conn_conf_dict[conn_id].password,
            table=table_name,
            ufile_path=UFILE_PATH % (db_name, table_name)
        ),
        dag=dag,
    )

    # check table
    check_table = PythonOperator(
        task_id='check_table_{}'.format(hive_table_name),
        python_callable=run_check_table,
        provide_context=True,
        op_kwargs={
            'db_name': db_name,
            'table_name': table_name,
            'conn_id': conn_id,
            'hive_table_name': hive_table_name
        },
        dag=dag
    )
    # add partitions
    add_partitions = HiveOperator(
        task_id='add_partitions_{}'.format(hive_table_name),
        hql='''
                ALTER TABLE {table} ADD IF NOT EXISTS PARTITION (dt = '{{{{ ds }}}}')
            '''.format(table=hive_table_name),
        schema=HIVE_DB,
        dag=dag)

    validate_all_data = PythonOperator(
        task_id='validate_data_{}'.format(hive_table_name),
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

    import_table >> check_table >> add_partitions >> validate_all_data
