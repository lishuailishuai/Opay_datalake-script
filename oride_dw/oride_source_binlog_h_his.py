import airflow
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.sensors import OssSensor
from datetime import datetime, timedelta
from utils.validate_metrics_utils import *
import logging
from plugins.SqoopSchemaUpdate import SqoopSchemaUpdate
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from utils.util import on_success_callback
from airflow.models import Variable
import time
import os

args = {
    'owner': 'linan',
    'start_date': datetime(2020, 3, 3),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'on_success_callback': on_success_callback,
}
schedule_interval = "20 * * * *"

dag = airflow.DAG(
    'oride_source_binlog_h_his',
    schedule_interval=schedule_interval,
    concurrency=40,
    max_active_runs=1,
    default_args=args)

dag_monitor = airflow.DAG(
    'oride_source_binlog_h_his_monitor',
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
    # 'user_limit',
    # 'channel_router_rule',
]

'''
导入数据的列表
db_name,table_name,conn_id,prefix_name,priority_weight,server_name (采集配置，定位oss数据位置使用),是否验证数据存在
'''
#

table_list = [

    # oride
    ("oride_data", "data_driver", "sqoop_db", "base", 3, "oride_db", "false"),
    ("oride_data", "data_driver_extend", "sqoop_db", "base", 3, "oride_db", "false"),
    ("oride_data", "data_country_conf", "sqoop_db", "base", 3, "oride_db", "false"),
    ("oride_data", "data_city_conf", "sqoop_db", "base", 3, "oride_db", "false"),
    ("oride_data", "data_device", "sqoop_db", "base", 3, "oride_db", "false"),
    ("oride_data", "data_driver_whitelist", "sqoop_db", "base", 3, "oride_db", "false"),
    ("oride_data", "data_user", "sqoop_db", "base", 3, "oride_db", "false"),
    ("oride_data", "data_user_whitelist", "sqoop_db", "base", 3, "oride_db", "false"),
    ("oride_data", "data_user_extend", "sqoop_db", "base", 3, "oride_db", "false"),

    # opay_spread
    ("opay_spread", "rider_signups", "opay_spread_mysql", "mass", 3, "opay_spread_db", "false"),
    ("opay_spread", "driver_group", "opay_spread_mysql", "mass", 3, "opay_spread_db", "false"),
    ("opay_spread", "driver_team", "opay_spread_mysql", "mass", 3, "opay_spread_db", "false"),

]

HIVE_DB = 'oride_dw_ods'
HIVE_SQOOP_TEMP_DB = 'test_db'
HIVE_FULL_TABLE = 'ods_binlog_%s_%s_h_his'
HIVE_HI_TABLE = 'ods_binlog_%s_%s_hi'
HIVE_SQOOP_TEMP_TABLE = '%s_full'

UFILE_PATH = Variable.get("OBJECT_STORAGE_PROTOCOL") + 'opay-datalake/temp/%s/%s'
H_HIS_OSS_PATH = 'oss://opay-datalake/oride_h_his/%s'
HI_OSS_PATH = 'oss://opay-datalake/oride_binlog/%s'
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
        `uuid` string COMMENT 'from deserializer', 
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

ODS_SQOOP_CREATE_TABLE_SQL = '''
    CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.`{table_name}`(
        {columns}
    )
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION
      '{ufile_path}';
    MSCK REPAIR TABLE test_db.`{table_name}`;
'''

MERGE_HI_SQL = '''
    SET hive.exec.parallel=TRUE;
    SET hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table {db_name}.`{hive_h_his_table_name}` partition(dt,hour)
    select 
    {columns},
    '{pt}' as dt,
    '{now_hour}' as hour
    from 
    {db_name}.`{hive_hi_table_name}`
    where dt = '{pt}' and hour = '{now_hour}'
    union all 
    select 
    {columns},
    '{pt}' as dt,
    '{now_hour}' as hour
    from 
    {db_name}.`{hive_h_his_table_name}`
    where dt = '{pre_hour_day}' and hour = '{pre_hour}'
    ;
    

'''

MERGE_HI_WITH_FULL_SQL = '''
    SET hive.exec.parallel=TRUE;
    SET hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table {db_name}.`{hive_h_his_table_name}` partition(dt,hour)
    select 
    {columns},
    '{pt}' as dt,
    '{now_hour}' as hour
    from 
    {db_name}.`{hive_hi_table_name}`
    where dt = '{pt}' and hour = '{now_hour}'
    
    union all 
    
    select 
    '{mysql_db_name}' as `__db` , 
    '0' as `__server_id` , 
    '0' as `__file` , 
    '0' as `__pos` , 
    '0' as `__row` , 
    '{mysql_table_name}' as `__table` , 
    'false' as `__deleted` , 
    '0' as `__version` , 
    '0' as `__connector` , 
    {pre_day_ms} as `__ts_ms` , 
    'uuid' as `uuid` , 
    {mysql_columns},
    '{pt}' as dt,
    '{now_hour}' as hour
    from 
    {sqoop_temp_db_name}.`{sqoop_table_name}`
    ;
    

'''


def add_partition(v_execution_date, v_execution_day, v_execution_hour, db_name, table_name, conn_id, hive_table_name,
                  server_name, hive_db, is_must_have_data, **kwargs):
    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS

    """
    TaskTouchzSuccess().countries_touchz_success(
        v_execution_day,
        hive_db,
        hive_table_name,
        H_HIS_OSS_PATH % hive_table_name,
        "false", is_must_have_data, v_execution_hour)

    sql = '''
            ALTER TABLE {hive_db}.{table} ADD IF NOT EXISTS PARTITION (dt = '{ds}', hour = '{hour}')
        '''.format(hive_db=hive_db, table=hive_table_name, ds=v_execution_day, hour=v_execution_hour)

    hive2_conn = HiveServer2Hook().get_conn()
    cursor = hive2_conn.cursor()
    cursor.execute(sql)

    return


def add_hi_partition(v_execution_date, v_execution_day, v_execution_hour, db_name, table_name, conn_id, hive_table_name,
                     server_name, hive_db, is_must_have_data, **kwargs):
    sql = '''
            ALTER TABLE {hive_db}.{table} ADD IF NOT EXISTS PARTITION (dt = '{ds}', hour = '{hour}')
        '''.format(hive_db=hive_db, table=hive_table_name, ds=v_execution_day, hour=v_execution_hour)

    hive2_conn = HiveServer2Hook().get_conn()
    cursor = hive2_conn.cursor()
    cursor.execute(sql)

    return


def run_check_table(mysql_db_name, mysql_table_name, conn_id, hive_h_his_table_name, server_name, **kwargs):
    # SHOW TABLES in oride_db LIKE 'data_aa'
    check_sql = 'SHOW TABLES in %s LIKE \'%s\'' % (HIVE_DB, hive_h_his_table_name)
    hive2_conn = HiveServer2Hook().get_conn()
    cursor = hive2_conn.cursor()
    cursor.execute(check_sql)
    if len(cursor.fetchall()) == 0:
        logging.info('Create Hive Table: %s.%s', HIVE_DB, hive_h_his_table_name)
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
            '''.format(db_name=mysql_db_name, table_name=mysql_table_name)
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
            rows.append(
                "`%s` %s comment '%s'" % (col_name, data_type, str(result[4]).replace('\n', '').replace('\r', '')))
        mysql_conn.close()

        # hive create table
        hive_hook = HiveCliHook()
        sql = ODS_CREATE_TABLE_SQL.format(
            db_name=HIVE_DB,
            table_name=hive_h_his_table_name,
            columns=",\n".join(rows),
            oss_path=H_HIS_OSS_PATH % hive_h_his_table_name
        )
        logging.info('Executing: %s', sql)
        hive_hook.run_cli(sql)

    else:
        sqoopSchema = SqoopSchemaUpdate()
        response = sqoopSchema.append_hive_schema(
            hive_db=HIVE_DB,
            hive_table=hive_h_his_table_name,
            mysql_db=mysql_db_name,
            mysql_table=mysql_table_name,
            mysql_conn=conn_id,
            oss_path=H_HIS_OSS_PATH % hive_h_his_table_name
        )
        if response:
            return True
    return


def run_sqoop_check_table(mysql_db_name, mysql_table_name, conn_id, hive_table_name, **kwargs):
    sqoopSchema = SqoopSchemaUpdate()
    response = sqoopSchema.update_hive_schema(
        hive_db=HIVE_SQOOP_TEMP_DB,
        hive_table=hive_table_name,
        mysql_db=mysql_db_name,
        mysql_table=mysql_table_name,
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
        '''.format(db_name=mysql_db_name, table_name=mysql_table_name)
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
        sql = ODS_SQOOP_CREATE_TABLE_SQL.format(
            db_name=HIVE_SQOOP_TEMP_DB,
            table_name=hive_table_name,
            columns=",\n".join(rows),
            ufile_path=UFILE_PATH % (mysql_db_name, mysql_table_name)
        )
        logging.info('Executing: %s', sql)
        hive_hook.run_cli(sql)
    return


def validate_full_table_exist_task(hive_h_his_table_name, mysql_table_name, **kwargs):
    check_sql = 'show partitions %s.%s' % (HIVE_DB, hive_h_his_table_name)
    hive2_conn = HiveServer2Hook().get_conn()
    cursor = hive2_conn.cursor()
    cursor.execute(check_sql)
    if len(cursor.fetchall()) == 0:
        return 'import_table_{}'.format(mysql_table_name)
    else:
        return 'add_partitions_{}'.format(hive_h_his_table_name)


def merge_pre_hi_data_task(hive_db, hive_h_his_table_name, hive_hi_table_name, pt, now_hour, pre_hour_day, pre_hour,
                           **kwargs):
    sqoopSchema = SqoopSchemaUpdate()
    hive_columns = sqoopSchema.get_hive_column_name(hive_db, hive_h_his_table_name)

    hql = MERGE_HI_SQL.format(
        db_name=hive_db,
        hive_h_his_table_name=hive_h_his_table_name,
        hive_hi_table_name=hive_hi_table_name,
        pt=pt,
        now_hour=now_hour,
        pre_hour_day=pre_hour_day,
        pre_hour=pre_hour,
        columns=',\n'.join(hive_columns)
    )

    hive_hook = HiveCliHook()

    # 读取sql

    logging.info('Executing: %s', hql)

    # 执行Hive
    hive_hook.run_cli(hql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(pt, hive_db, hive_h_his_table_name,
                                                 H_HIS_OSS_PATH % hive_h_his_table_name,
                                                 "false",
                                                 "true",
                                                 now_hour)


def merge_pre_hi_with_full_data_task(hive_db, hive_h_his_table_name, hive_hi_table_name, mysql_db_name,
                                     mysql_table_name, mysql_conn,
                                     sqoop_temp_db_name, sqoop_table_name,
                                     pt, now_hour, pre_day, pre_hour_day, pre_hour, **kwargs):
    sqoopSchema = SqoopSchemaUpdate()

    hive_columns = sqoopSchema.get_hive_column_name(hive_db, hive_h_his_table_name)
    mysql_columns = sqoopSchema.get_mysql_column_name(mysql_db_name, mysql_table_name, mysql_conn)
    pre_day_ms = int(time.mktime(time.strptime(pre_day, "%Y-%m-%d"))) * 1000

    hql = MERGE_HI_WITH_FULL_SQL.format(
        columns=',\n'.join(hive_columns),
        pt=pt,
        now_hour=now_hour,
        db_name=hive_db,
        mysql_db_name=mysql_db_name,
        hive_h_his_table_name=hive_h_his_table_name,
        hive_hi_table_name=hive_hi_table_name,
        mysql_table_name=mysql_table_name,
        pre_day_ms=pre_day_ms,
        mysql_columns=',\n'.join(mysql_columns),
        sqoop_temp_db_name=sqoop_temp_db_name,
        sqoop_table_name=sqoop_table_name
    )

    hive_hook = HiveCliHook()

    # 读取sql
    logging.info('Executing: %s', hql)

    # 执行Hive
    hive_hook.run_cli(hql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 
    
    """
    TaskTouchzSuccess().countries_touchz_success(pt, hive_db, hive_h_his_table_name,
                                                 H_HIS_OSS_PATH % hive_h_his_table_name,
                                                 "false",
                                                 "true",
                                                 now_hour)


conn_conf_dict = {}
for mysql_db_name, mysql_table_name, conn_id, prefix_name, priority_weight_nm, server_name, is_must_have_data in table_list:
    if conn_id not in conn_conf_dict:
        conn_conf_dict[conn_id] = BaseHook.get_connection(conn_id)

    hive_h_his_table_name = HIVE_FULL_TABLE % (prefix_name, mysql_table_name)
    hive_hi_table_name = HIVE_HI_TABLE % (prefix_name, mysql_table_name)
    sqoop_table_name = HIVE_SQOOP_TEMP_TABLE % mysql_table_name

    # check h_his table 校验全量表是否存在
    check_h_his_table = PythonOperator(
        task_id='check_h_his_table_{}'.format(hive_h_his_table_name),
        priority_weight=priority_weight_nm,
        python_callable=run_check_table,
        provide_context=True,
        op_kwargs={
            'mysql_db_name': mysql_db_name,
            'mysql_table_name': mysql_table_name,
            'conn_id': conn_id,
            'hive_h_his_table_name': hive_h_his_table_name,
            'server_name': server_name
        },
        dag=dag
    )

    # 校验是否hive全量表是否存在数据
    validate_full_table_exist = BranchPythonOperator(
        task_id='validate_full_table_exist_{}'.format(hive_h_his_table_name),
        provide_context=True,
        python_callable=validate_full_table_exist_task,
        op_kwargs={
            'mysql_table_name': mysql_table_name,
            'hive_h_his_table_name': hive_h_his_table_name
        },
        dag=dag,
    )

    # sqoop import sqoop导入全量数据
    import_table = BashOperator(
        task_id='import_table_{}'.format(mysql_table_name),
        priority_weight=priority_weight_nm,
        bash_command='''
                #!/usr/bin/env bash
                sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
                -D mapred.job.queue.name=root.collects \
                --connect "jdbc:mysql://{host}:{port}/{schema}?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8" \
                --username {username} \
                --password {password} \
                --table {table} \
                --target-dir {ufile_path}/ \
                --fields-terminated-by "\\001" \
                --lines-terminated-by "\\n" \
                --hive-delims-replacement " " \
                --delete-target-dir \
                --compression-codec=snappy 
            '''.format(
            host=conn_conf_dict[conn_id].host,
            port=conn_conf_dict[conn_id].port,
            schema=mysql_db_name,
            username=conn_conf_dict[conn_id].login,
            password=conn_conf_dict[conn_id].password,
            table=mysql_table_name,
            ufile_path=UFILE_PATH % (mysql_db_name, mysql_table_name)
        ),
        dag=dag,
    )

    # check table
    check_sqoop_table = PythonOperator(
        task_id='check_sqoop_table_{}'.format(sqoop_table_name),
        priority_weight=priority_weight_nm,
        python_callable=run_sqoop_check_table,
        provide_context=True,
        op_kwargs={
            'mysql_db_name': mysql_db_name,
            'mysql_table_name': mysql_table_name,
            'conn_id': conn_id,
            'hive_table_name': sqoop_table_name
        },
        dag=dag
    )

    # add h_his partitions
    add_partitions = PythonOperator(
        task_id='add_partitions_{}'.format(hive_h_his_table_name),
        priority_weight=priority_weight_nm,
        python_callable=add_partition,
        provide_context=True,
        op_kwargs={
            'db_name': mysql_db_name,
            'table_name': mysql_table_name,
            'conn_id': conn_id,
            'hive_table_name': hive_h_his_table_name,
            'server_name': server_name,
            'hive_db': HIVE_DB,
            'is_must_have_data': is_must_have_data,
            'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
            'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
            'v_execution_hour': '{{execution_date.strftime("%H")}}',
        },
        dag=dag
    )

    ## 脱离小时级增量绑定过程
    # add_hi_partitions
    add_hi_partitions = PythonOperator(
        task_id='add_hi_partitions_{}'.format(hive_h_his_table_name),
        priority_weight=priority_weight_nm,
        python_callable=add_hi_partition,
        provide_context=True,
        op_kwargs={
            'db_name': mysql_db_name,
            'table_name': mysql_table_name,
            'conn_id': conn_id,
            'hive_table_name': hive_hi_table_name,
            'server_name': server_name,
            'hive_db': HIVE_DB,
            'is_must_have_data': 'false',
            'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
            'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
            'v_execution_hour': '{{execution_date.strftime("%H")}}',
        },
        dag=dag
    )

    # merge_pre_hi_data_task
    merge_pre_hi_data = PythonOperator(
        task_id='merge_pre_hi_data_task_{}'.format(hive_h_his_table_name),
        priority_weight=priority_weight_nm,
        python_callable=merge_pre_hi_data_task,
        provide_context=True,
        op_kwargs={
            'hive_db': HIVE_DB,
            'hive_h_his_table_name': hive_h_his_table_name,
            'hive_hi_table_name': hive_hi_table_name,
            'pt': '{{execution_date.strftime("%Y-%m-%d")}}',
            'now_hour': '{{execution_date.strftime("%H")}}',
            'pre_hour_day': '{{(execution_date+macros.timedelta(hours=-1)).strftime("%Y-%m-%d")}}',
            'pre_hour': '{{(execution_date+macros.timedelta(hours=-1)).strftime("%H")}}'

        },
        dag=dag
    )

    # merge_pre_hi_with_full_data_task
    merge_pre_hi_with_full_data = PythonOperator(
        task_id='merge_pre_hi_with_full_data_task_{}'.format(hive_h_his_table_name),
        priority_weight=priority_weight_nm,
        python_callable=merge_pre_hi_with_full_data_task,
        provide_context=True,
        op_kwargs={
            'hive_db': HIVE_DB,
            'hive_h_his_table_name': hive_h_his_table_name,
            'hive_hi_table_name': hive_hi_table_name,
            'pt': '{{execution_date.strftime("%Y-%m-%d")}}',
            'pre_day': '{{macros.ds_add(ds, -1)}}',
            'now_hour': '{{execution_date.strftime("%H")}}',
            'pre_hour_day': '{{(execution_date+macros.timedelta(hours=-1)).strftime("%Y-%m-%d")}}',
            'pre_hour': '{{(execution_date+macros.timedelta(hours=-1)).strftime("%H")}}',
            'mysql_db_name': mysql_db_name,
            'mysql_table_name': mysql_table_name,
            'mysql_conn': conn_id,
            'sqoop_temp_db_name': HIVE_SQOOP_TEMP_DB,
            'sqoop_table_name': sqoop_table_name
        },
        dag=dag
    )

    # 超时监控
    task_timeout_monitor = PythonOperator(
        task_id='task_timeout_monitor_{}'.format(hive_h_his_table_name),
        python_callable=fun_task_timeout_monitor,
        provide_context=True,
        op_kwargs={
            'db_name': HIVE_DB,
            'table_name': hive_h_his_table_name,
        },
        dag=dag_monitor
    )

    check_h_his_table >> add_hi_partitions >> validate_full_table_exist >> [add_partitions, import_table]
    add_partitions >> merge_pre_hi_data
    import_table >> check_sqoop_table >> merge_pre_hi_with_full_data
