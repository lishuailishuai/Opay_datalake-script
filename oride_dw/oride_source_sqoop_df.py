import airflow
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.sql_sensor import SqlSensor
from datetime import datetime, timedelta
from utils.validate_metrics_utils import *
import logging
from plugins.SqoopSchemaUpdate import SqoopSchemaUpdate

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime(2019, 9, 28),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_source_sqoop_df',
    schedule_interval="00 01 * * *",
    concurrency=15,
    max_active_runs=1,
    default_args=args)


check_data_driver_records_finish = SqlSensor(
    task_id="check_data_driver_records_finish",
    conn_id='sqoop_db',
    sql='''
        select
            count(1)
        from
            oride_data.data_driver_records_finish
        where
            from_unixtime(day, "%Y-%m-%d") = '{{ ds }}'
    ''',
    dag=dag
)

'''
导入数据的列表
db_name,table_name,conn_id,prefix_name,priority_weight
'''
#

table_list = [
    # oride data
    ("oride_data", "data_order", "sqoop_db", "base",3),
    ("oride_data", "data_order_payment", "sqoop_db", "base",3),
    ("oride_data", "data_user", "sqoop_db", "base",3),
    ("oride_data", "data_user_extend", "sqoop_db", "base",3),
    ("oride_data", "data_coupon", "sqoop_db", "base",1),
    ("oride_data", "data_driver", "sqoop_db", "base",1),
    ("oride_data", "data_driver_group", "sqoop_db", "base",1),
    ("oride_data", "data_driver_extend", "sqoop_db", "base",3),
    ("oride_data", "data_driver_comment", "sqoop_db", "base",1),
    ("oride_data", "data_abnormal_order", "sqoop_db", "base",3),
    ("oride_data", "data_anti_fraud_strategy", "sqoop_db", "base",3),
    ("oride_data", "data_city_conf", "sqoop_db", "base",3),
    ("oride_data", "data_order_expired", "sqoop_db", "base",1),
    ("oride_data", "data_device_extend", "sqoop_db", "base",1),
    ("oride_data", "data_driver_recharge_records", "sqoop_db", "base",3),
    ("oride_data", "data_driver_reward", "sqoop_db", "base",3),
    ("oride_data", "data_activity", "sqoop_db", "base",1),
    ("oride_data", "data_agenter_motorbike", "sqoop_db", "base",1),
    ("oride_data", "data_billboard_config", "sqoop_db", "base",1),
    #("oride_data", "data_coupon_template", "sqoop_db", "base",1),
    ("oride_data", "data_device", "sqoop_db", "base",1),
    ("oride_data", "data_driver_balance_records", "sqoop_db", "base",1),
    ("oride_data", "data_driver_discount", "sqoop_db", "base",1),
    ("oride_data", "data_driver_fee_blacklist", "sqoop_db", "base",1),
    ("oride_data", "data_driver_operation_log", "sqoop_db", "base",1),
    ("oride_data", "data_driver_bind_logs", "sqoop_db", "base",1),
    ("oride_data", "data_driver_pay_records", "sqoop_db", "base",1),
    ("oride_data", "data_driver_reward_push", "sqoop_db", "base",1),
    ("oride_data", "data_fcm_template", "sqoop_db", "base",1),
    ("oride_data", "data_invite", "sqoop_db", "base",1),
    ("oride_data", "data_invite_conf", "sqoop_db", "base",1),
    ("oride_data", "data_motorbike", "sqoop_db", "base",1),
    ("oride_data", "data_motorbike_extend", "sqoop_db", "base",1),
    ("oride_data", "data_novice_coupons_conf", "sqoop_db", "base",1),
    ("oride_data", "data_opay_transaction", "sqoop_db", "base",1),
    ("oride_data", "data_payconf", "sqoop_db", "base",1),
    ("oride_data", "data_promo_code", "sqoop_db", "base",1),
    ("oride_data", "data_recharge_conf", "sqoop_db", "base",1),
    ("oride_data", "data_recharge_options", "sqoop_db", "base",1),
    ("oride_data", "data_reward_conf", "sqoop_db", "base",1),
    ("oride_data", "data_role_invite", "sqoop_db", "base",1),
    ("oride_data", "data_sms_template", "sqoop_db", "base",1),
    ("oride_data", "data_user_comment", "sqoop_db", "base",1),
    ("oride_data", "data_user_complaint", "sqoop_db", "base",1),
    ("oride_data", "data_user_recharge", "sqoop_db", "base",1),
    ("oride_data", "data_user_whitelist", "sqoop_db", "base",1),
    ("oride_data", "data_driver_whitelist", "sqoop_db", "base",1),
    ("oride_data", "data_user_blacklist", "sqoop_db", "base",1),
    ("oride_data", "data_driver_blacklist", "sqoop_db", "base",1),
    ("oride_data", "data_driver_repayment", "sqoop_db", "base", 1),
    ("oride_data", "data_trip", "sqoop_db", "base", 1),
    ("oride_data", "data_driver_records_day", "sqoop_db", "base",1),
    ("oride_data", "data_driver_balance_extend", "sqoop_db", "base",1),

    ("bi", "weather_per_10min", "mysql_bi", "base",3),
    # 协会数据
    # 数据库 opay_spread
    ("opay_spread", "driver_data", "opay_spread_mysql", "mass",1),
    ("opay_spread", "driver_group", "opay_spread_mysql", "mass",3),
    ("opay_spread", "driver_logs", "opay_spread_mysql", "mass",1),
    ("opay_spread", "driver_team", "opay_spread_mysql", "mass",3),
    ("opay_spread", "rider_signup", "opay_spread_mysql", "mass",1),
    ("opay_spread", "rider_signups", "opay_spread_mysql", "mass",3),
    ("opay_spread", "rider_signups_agents", "opay_spread_mysql", "mass",1),
    ("opay_spread", "rider_signups_guarantors", "opay_spread_mysql", "mass",1),
    ("opay_spread", "rider_signups_logs", "opay_spread_mysql", "mass",1),
    # 数据库：oride_assets
    ("oride_assets", "oride_assets_transit", "opay_spread_mysql", "mass",1),
    ("oride_assets", "oride_categories", "opay_spread_mysql", "mass",1),
    ("oride_assets", "oride_my_storage", "opay_spread_mysql", "mass",1),
    ("oride_assets", "oride_properties", "opay_spread_mysql", "mass",1),
    ("oride_assets", "oride_property_customs", "opay_spread_mysql", "mass",1),
    ("oride_assets", "oride_repair", "opay_spread_mysql", "mass",1),
    ("oride_assets", "oride_retrieve", "opay_spread_mysql", "mass",1),
    ("oride_assets", "oride_storage", "opay_spread_mysql", "mass",1),
    ("oride_assets", "oride_storage_logs", "opay_spread_mysql", "mass",1),
    ("oride_assets", "oride_user_ware", "opay_spread_mysql", "mass",1),
    ("oride_assets", "oride_vehicles", "opay_spread_mysql", "mass",1),
    ("oride_assets", "oride_vehicles_log", "opay_spread_mysql", "mass",1),
    ("oride_assets", "oride_vehicles_transit", "opay_spread_mysql", "mass",1),
    ("oride_assets", "oride_warehouses", "opay_spread_mysql", "mass",1),
    # 地推数据源
    # 数据库：opay_spread
    ("opay_spread", "promoter_channel_day", "opay_spread_mysql", "promoter",1),
    ("opay_spread", "promoter_data_day", "opay_spread_mysql", "promoter",1),
    ("opay_spread", "promoter_data_hour", "opay_spread_mysql", "promoter",1),
    ("opay_spread", "promoter_driver_day", "opay_spread_mysql", "promoter",1),
    ("opay_spread", "promoter_logs", "opay_spread_mysql", "promoter",1),
    ("opay_spread", "promoter_manager", "opay_spread_mysql", "promoter",1),
    ("opay_spread", "promoter_order_day", "opay_spread_mysql", "promoter",1),
    ("opay_spread", "promoter_team", "opay_spread_mysql", "promoter",1),
    ("opay_spread", "promoter_user", "opay_spread_mysql", "promoter",1),
    ("opay_spread", "promoter_user_relat_admin", "opay_spread_mysql", "promoter",1),
    ("opay_spread", "promoter_users_device", "opay_spread_mysql", "promoter",1),
    ("opay_spread", "spread_sign_up", "opay_spread_mysql", "promoter",1),
]

HIVE_DB = 'oride_dw_ods'
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
    MSCK REPAIR TABLE {db_name}.`{table_name}`;
    -- delete oride_dw table
    DROP TABLE IF EXISTS oride_dw.`{table_name}`;
'''

# 需要验证的核心业务表
table_core_list = [
    # ("oride_data", "data_order", "sqoop_db", "base", "create_time","priority_weight")
]

# 不需要验证的维度表，暂时为null
table_dim_list = []

# 需要验证的非核心业务表，根据需求陆续添加
table_not_core_list = []


def run_check_table(db_name, table_name, conn_id, hive_table_name, **kwargs):
    sqoopSchema = SqoopSchemaUpdate()
    response = sqoopSchema.update_hive_schema(
        hive_db=HIVE_DB,
        hive_table=hive_table_name,
        mysql_db=db_name,
        mysql_table=table_name,
        mysql_conn=conn_id
    )
    if response:
        return True

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
for db_name, table_name, conn_id, prefix_name,priority_weight_nm in table_list:
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
            --table {table} \
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
            table=table_name,
            ufile_path=UFILE_PATH % (db_name, table_name)
        ),
        dag=dag,
    )

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
            'hive_table_name': hive_table_name
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

    touchz_data_success = BashOperator(
        task_id='touchz_data_success_{}'.format(hive_table_name),
        priority_weight=priority_weight_nm,
        bash_command="""
                line_num=`$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk '{{print $1}}'`

                if [ $line_num -eq 0 ]
                then
                    echo "FATAL {hdfs_data_dir} is empty"
                    exit 1
                else
                    echo "DATA EXPORT Successed ......"
                    $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
                fi
            """.format(
            hdfs_data_dir=UFILE_PATH % (db_name, table_name)+"/dt={{ds}}"
        ),
        dag=dag)

    if table_name in ['data_driver_records_day', 'data_driver_balance_extend']:
        check_data_driver_records_finish >> import_table

    import_table >> check_table >> add_partitions >> validate_all_data >> touchz_data_success
