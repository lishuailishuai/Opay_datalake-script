# coding=utf-8

import airflow
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator
from utils.connection_helper import get_db_conf
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from utils.connection_helper import get_hive_cursor

from datetime import datetime, timedelta
from airflow.utils.email import send_email
from airflow.models import Variable
import math

args = {
    'owner': 'root',
    'start_date': datetime(2019, 6, 22),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_import_mysql_to_hive',
    schedule_interval="00 01 * * *",
    concurrency=5,
    max_active_runs=1,
    default_args=args)

# 导入数据的列表

table_list = [
    # 以下表已经在oride_dw库每日导入，后续开发请使用oride_dw库
    ("data_order", "sqoop_db"),
    ("data_order_payment", "sqoop_db"),
    ("data_user", "sqoop_db"),
    ("data_user_extend", "sqoop_db"),
    ("data_coupon", "sqoop_db"),
    ("data_driver", "sqoop_db"),
    ("data_driver_group", "sqoop_db"),
    ("data_driver_extend", "sqoop_db"),
    ("data_driver_comment", "sqoop_db"),
    ("data_abnormal_order", "sqoop_db"),
    ("data_anti_fraud_strategy", "sqoop_db"),
    ("data_city_conf", "sqoop_db"),
    ("data_device_extend", "sqoop_db"),
]

# 需要验证的核心业务表
table_core_list = ["data_order"]

# 不需要验证的维度表，暂时为null
table_dim_list = []

# 需要验证的非核心业务表，根据需求陆续添加
table_not_core_list = []

now = datetime.today()



# 预警阀值
ods_data_alert_limit = 0.1

# 停止阀值
ods_data_stop_limit = 0.45

"""
    校验全局数据量函数
"""


def validata_data(table_name, ds, **kwargs):
    cursor = get_hive_cursor()
    day = ds
    day_before_1 = airflow.macros.ds_add(ds, -1)
    # 暂时修改为对比前3天
    day_before_7 = airflow.macros.ds_add(ds, -3)

    if table_name not in table_core_list and table_name not in table_not_core_list:
        write_meta_data(table_name, day, 1, '此表不在校验列表中，导入成功')
        return

    # 第一次验证
    sql = '''
        select 
        from_unixtime(create_time,'yyyy-MM-dd') order_day,
        count(1)
        from 
        oride_db.{table_name}
        where dt = '{day}'
        and (from_unixtime(create_time,'yyyy-MM-dd') = '{day}' or from_unixtime(create_time,'yyyy-MM-dd') = '{day_before_7}')
        group by from_unixtime(create_time,'yyyy-MM-dd')
        order by order_day desc
    '''.format(
        table_name=table_name,
        day=day,
        day_before_1=day_before_1,
        day_before_7=day_before_7
    )

    t = validate(cursor, table_name, sql, day, day_before_1, day_before_7)

    if not t[0]:
        write_meta_data(table_name, day, t[0], t[1])
        # send error mail
        email_subject = '数据导入预警_{table_name}_{day}'.format(
            table_name=table_name,
            day=day)
        send_email(
            'bigdata@opay-inc.com'
            , email_subject, t[1], mime_charset='utf-8')
        return

    sql = '''
        select 
        t.order_day,
        t.counts
        from 
        (
        select 
                from_unixtime(create_time,'yyyy-MM-dd') order_day,
                count(1) counts
                from 
                oride_db.{table_name}
                where dt = '{day}' and from_unixtime(create_time,'yyyy-MM-dd') = '{day}'
                group by from_unixtime(create_time,'yyyy-MM-dd')
                union all 
                select 
                from_unixtime(create_time,'yyyy-MM-dd') order_day,
                count(1) counts
                from 
                oride_db.{table_name}
                where dt = '{day_before_1}' and from_unixtime(create_time,'yyyy-MM-dd') = '{day_before_7}'
                group by from_unixtime(create_time,'yyyy-MM-dd')
        ) t
        order by t.order_day desc
    '''.format(
        table_name=table_name,
        day=day,
        day_before_1=day_before_1,
        day_before_7=day_before_7
    )

    t = validate(cursor, table_name, sql, day, day_before_1, day_before_7)

    if not t[0]:
        write_meta_data(table_name, day, t[0], t[1])
        # send error mail
        email_subject = '数据导入预警_{table_name}_{day}'.format(
            table_name=table_name,
            day=day)
        send_email(
            'bigdata@opay-inc.com'
            , email_subject, t[1], mime_charset='utf-8')
        return

    write_meta_data(table_name, day, t[0], '验证完成，成功导入')


def validate(cursor, table_name, sql, day, day_before_1, day_before_7):
    error_message = ''
    is_import = 1

    cursor.execute(sql)
    res = cursor.fetchall()
    if res is None or len(res) < 2:
        is_import = 0
        error_message = '''
            {table_name}  未找到 {day} 日期数据
        '''.format(
            table_name=table_name,
            day=day)
    else:
        day_num = float(res[0][1])
        day_before_7_num = float(res[1][1])

        print('day_num : {} , day_before_7_num : {} '.format(day_num, day_before_7_num))

        if day_num == 0:
            is_import = 0
            error_message = '''
                {table_name}  {day}  日期数据数据量为 0 ,请关注相关数据指标
            '''.format(table_name=table_name, day=day)
        elif (day_num - day_before_7_num) < 0 and abs((day_num - day_before_7_num) / day_num) > ods_data_stop_limit:
            is_import = 0
            error_message = '''
            
                监控规则：数据量下降幅度超过10%进行预警，数据量下降幅度超过45%发送BI，进行审核。
                异常说明：表： {table_name} ，{day} 数据量为 {day_num}，{day_before_7}  数据量为 {day_before_7_num}，已达到预警标准。
                请BI 核查指标数据是否异常，如存在策略调整，请通知相关使用方。
                
               
            '''.format(
                table_name=table_name,
                day=day,
                day_num=day_num,
                day_before_7=day_before_7,
                day_before_7_num=day_before_7_num)
        elif (day_num - day_before_7_num) < 0 and abs((day_num - day_before_7_num) / day_num) > ods_data_alert_limit:
            error_message = '''
            
                监控规则：数据量下降幅度超过10%进行预警，数据量下降幅度超过45%发送BI，进行审核。
                异常说明：表： {table_name} ，{day} 数据量为 {day_num}，{day_before_7}  数据量为 {day_before_7_num}，需要BI审核。
                请BI 核查指标数据是否异常，如存在策略调整，请通知相关使用方。
                
            '''.format(
                table_name=table_name,
                day=day,
                day_num=day_num,
                day_before_7=day_before_7,
                day_before_7_num=day_before_7_num)

    return (is_import, error_message)


def write_meta_data(table_name, day, result, msg):
    cursor = get_hive_cursor()
    # if not result:
    #     sql = '''
    #         ALTER TABLE oride_db.{table_name} DROP IF EXISTS PARTITION(dt='{day}')
    #     '''.format(
    #         table_name=table_name,
    #         day=day)
    #
    #     cursor.execute(sql)

    sql = '''
        insert into table oride_bi.oride_meta_import_data 
        partition (dt='{day}',table_name='{table_name}')
        select {result},'{msg}','{timestamp}' from default.dual
    '''.format(
        table_name=table_name,
        day=day,
        result=result,
        msg=msg,
        timestamp=now.strftime('%Y-%m-%d %H:%M:%S')
    )

    cursor.execute(sql)


conn_conf_dict = {}
for table_name, conn_id in table_list:
    if conn_id not in conn_conf_dict:
        conn_conf_dict[conn_id] = BaseHook.get_connection(conn_id)
    import_table = BashOperator(
        task_id='import_table_{}'.format(table_name),
        bash_command='''
            #!/usr/bin/env bash
            sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
            -D mapred.job.queue.name=root.collects \
            --connect "jdbc:mysql://{host}:{port}/{schema}?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8" \
            --username {username} \
            --password {password} \
            --table {table} \
            --target-dir ufile://opay-datalake/oride/db/{table}/dt={{{{ ds }}}}/ \
            --fields-terminated-by "\\001" \
            --lines-terminated-by "\\n" \
            --hive-delims-replacement " " \
            --delete-target-dir \
            --compression-codec=snappy
        '''.format(
            host=conn_conf_dict[conn_id].host,
            port=conn_conf_dict[conn_id].port,
            schema=conn_conf_dict[conn_id].schema,
            username=conn_conf_dict[conn_id].login,
            password=conn_conf_dict[conn_id].password,
            table=table_name
        ),
        dag=dag,
    )

    if conn_id == 'sqoop_db':
        hive_db = 'oride_db'
        hive_table = table_name
    else:
        hive_db = 'oride_dw'
        hive_table = 'ods_sqoop_%s_df' % table_name

    add_partitions = HiveOperator(
        task_id='add_partitions_{}'.format(table_name),
        hql='''
                ALTER TABLE {table} ADD IF NOT EXISTS PARTITION (dt = '{{{{ ds }}}}')
            '''.format(table=hive_table),
        schema=hive_db,
        dag=dag)

    validate_all_data = PythonOperator(
        task_id='validate_data_{}'.format(table_name),
        python_callable=validata_data,
        provide_context=True,
        op_kwargs={'table_name': table_name},
        dag=dag
    )

    import_table >> add_partitions >> validate_all_data
