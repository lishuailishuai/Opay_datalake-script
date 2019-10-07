# -*- coding: utf-8 -*-
"""
# 每周日计算之前所有的用户数据
# 1. 有账号从未下过单的用户
# 2. 下过单从未完单的用户
# 3. 有完单,计算时前7日没下单的用户
# 4. 有完单,计算时前14日没下单的用户
"""

import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from utils.connection_helper import get_hive_cursor, get_db_conn
from datetime import datetime, timedelta
from plugins.comwx import ComwxApi
import time
import logging

args = {
    'owner': 'wuduo',
    'start_date': datetime(2019, 9, 25),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'app_oride_users_tags_w',
    schedule_interval="30 06 * * 0",
    default_args=args
)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 20',
    dag=dag
)

# 依赖
dependence_table_data_user_extend_task = HivePartitionSensor(
    task_id="dependence_table_data_user_extend_task",
    table="ods_sqoop_base_data_user_extend_df",
    partition="dt='{{ macros.ds_add(ds, 6) }}'",
    schema="oride_dw_ods",
    poke_interval=60,
    dag=dag
)

dependence_table_data_user_task = HivePartitionSensor(
    task_id="dependence_table_data_user_task",
    table="ods_sqoop_base_data_user_df",
    partition="dt='{{ macros.ds_add(ds, 6) }}'",
    schema="oride_dw_ods",
    poke_interval=60,
    dag=dag
)

dependence_table_data_order_task = HivePartitionSensor(
    task_id="dependence_table_data_order_task",
    table="ods_sqoop_base_data_order_df",
    partition="dt='{{ macros.ds_add(ds, 6) }}'",
    schema="oride_dw_ods",
    poke_interval=60,
    dag=dag
)
# end

hive_table = 'oride_dw.app_oride_users_tags_w'
create_result_table_task = HiveOperator(
    task_id='create_result_table_task',
    hql='''
        CREATE TABLE IF NOT EXISTS {table} (
            city_id      bigint comment '城市ID',
            user_id      bigint comment '用户ID',
            user_type    int    comment '用户类型0有账号未下单,1下单未完单,2完单近7日未下单,3完单近14日未下单',
            phone_number string comment '用户手机号',
            create_at    int    comment '计算时间'
        ) 
        PARTITIONED BY (  
            `dt` string comment '日期'
        )
        STORED AS ORC 
        LOCATION 's3a://opay-bi/oride_dw/app_oride_users_tags_w' 
        TBLPROPERTIES ("orc.compress"="SNAPPY")
    '''.format(table=hive_table),
    schema='oride_dw',
    dag=dag
)
# ufile://opay-datalake/oride/oride_dw/

count_sql = '''
--有oride账号，但未下过单的用户
user_unorders AS (
    SELECT 
        ui.city_id AS city_id,
        ui.id AS user_id,
        0 AS user_type,
        ui.phone_number AS phone_number,
        unix_timestamp() AS create_at
    FROM (SELECT 
            u.id,
            u.city_id,
            v.phone_number 
        FROM (SELECT 
                id,
                city_id 
            FROM oride_dw_ods.ods_sqoop_base_data_user_extend_df 
            WHERE dt = '{dt}' 
            ) AS u 
        JOIN (SELECT 
                id,
                phone_number 
            FROM oride_dw_ods.ods_sqoop_base_data_user_df 
            WHERE dt = '{dt}' 
            ) AS v 
        ON u.id = v.id
        ) AS ui  
    LEFT JOIN (SELECT 
            user_id
        FROM oride_dw_ods.ods_sqoop_base_data_order_df 
        WHERE dt = '{dt}' 
        GROUP BY user_id 
        ) AS o
    ON ui.id = o.user_id 
    WHERE o.user_id IS NULL 
),
--在oride下过单，但未完过单的用户
user_unfinished AS (
    SELECT 
        ui.city_id AS city_id,
        ui.id AS user_id,
        1 AS user_type,
        ui.phone_number AS phone_number, 
        unix_timestamp() AS create_at 
    FROM (SELECT 
            user_id 
        FROM oride_dw_ods.ods_sqoop_base_data_order_df 
        WHERE dt = '{dt}' 
        GROUP BY user_id 
        ) AS a 
    LEFT JOIN (SELECT 
            user_id
        FROM oride_dw_ods.ods_sqoop_base_data_order_df 
        WHERE dt = '{dt}' AND 
            status IN (4,5) 
        GROUP BY user_id 
        ) AS b 
    ON a.user_id = b.user_id 
    JOIN (SELECT 
            u.id,
            u.city_id,
            v.phone_number 
        FROM (SELECT 
                id,
                city_id 
            FROM oride_dw_ods.ods_sqoop_base_data_user_extend_df 
            WHERE dt = '{dt}' 
            ) AS u 
        JOIN (SELECT 
                id,
                phone_number 
            FROM oride_dw_ods.ods_sqoop_base_data_user_df 
            WHERE dt = '{dt}' 
            ) AS v 
        ON u.id = v.id
        ) AS ui  
    ON a.user_id = ui.id 
    WHERE b.user_id IS NULL
),
--有过完单，计算时前7日没下单的用户
user_unorders7 AS (
    SELECT 
        ui.city_id AS city_id,
        ui.id AS user_id,
        2 AS user_type,
        ui.phone_number AS phone_number,
        unix_timestamp() AS create_at 
    FROM (SELECT 
            user_id 
        FROM oride_dw_ods.ods_sqoop_base_data_order_df 
        WHERE dt = '{dt}' AND 
            status IN (4,5) 
        GROUP BY user_id
        ) AS uf 
    LEFT JOIN (SELECT 
            user_id 
        FROM oride_dw_ods.ods_sqoop_base_data_order_df 
        WHERE dt = '{dt}' AND 
            create_time > unix_timestamp(date_sub('{dt}', 7), 'yyyy-MM-dd')
        GROUP BY user_id 
        ) AS uo 
    ON uf.user_id = uo.user_id 
    JOIN (SELECT 
            u.id,
            u.city_id,
            v.phone_number 
        FROM (SELECT 
                id,
                city_id 
            FROM oride_dw_ods.ods_sqoop_base_data_user_extend_df 
            WHERE dt = '{dt}' 
            ) AS u 
        JOIN (SELECT 
                id,
                phone_number 
            FROM oride_dw_ods.ods_sqoop_base_data_user_df 
            WHERE dt = '{dt}' 
            ) AS v 
        ON u.id = v.id
        ) AS ui  
    ON uf.user_id = ui.id 
    WHERE uo.user_id IS NULL
),
--有过完单，计算时前14日没下单的用户
user_unorders14 AS (
    SELECT 
        ui.city_id AS city_id,
        ui.id AS user_id,
        3 AS user_type,
        ui.phone_number AS phone_number,
        unix_timestamp() AS create_at 
    FROM (SELECT 
            user_id 
        FROM oride_dw_ods.ods_sqoop_base_data_order_df 
        WHERE dt = '{dt}' AND 
            status IN (4,5) 
        GROUP BY user_id
        ) AS uf 
    LEFT JOIN (SELECT 
            user_id 
        FROM oride_dw_ods.ods_sqoop_base_data_order_df 
        WHERE dt = '{dt}' AND 
            create_time > unix_timestamp(date_sub('{dt}', 14), 'yyyy-MM-dd')
        GROUP BY user_id 
        ) AS uo 
    ON uf.user_id = uo.user_id 
    JOIN (SELECT 
            u.id,
            u.city_id,
            v.phone_number 
        FROM (SELECT 
                id,
                city_id 
            FROM oride_dw_ods.ods_sqoop_base_data_user_extend_df 
            WHERE dt = '{dt}' 
            ) AS u 
        JOIN (SELECT 
                id,
                phone_number 
            FROM oride_dw_ods.ods_sqoop_base_data_user_df 
            WHERE dt = '{dt}' 
            ) AS v 
        ON u.id = v.id
        ) AS ui  
    ON uf.user_id = ui.id 
    WHERE uo.user_id IS NULL
)
'''.format(dt='{{ macros.ds_add(ds, 6) }}')

insert_result_to_hive = HiveOperator(
    task_id='insert_result_to_hive',
    hql='''
        SET mapred.job.queue.name=root.users.airflow;
        SET hive.exec.parallel=true;
        SET hive.exec.dynamic.partition=true;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        WITH {sql} 
        INSERT OVERWRITE TABLE {table} PARTITION (dt) 
        SELECT 
            *,
            '{pt}' AS dt 
        FROM user_unorders 
        UNION ALL 
        SELECT 
            *, 
            '{pt}' AS dt 
        FROM user_unfinished 
        UNION ALL 
        SELECT 
            *,
            '{pt}' AS dt 
        FROM user_unorders7 
        UNION ALL 
        SELECT 
            *,
            '{pt}' AS dt 
        FROM user_unorders14 
    '''.format(sql=count_sql, table=hive_table, pt='{{ macros.ds_add(ds, 6) }}'),
    schema='oride_dw',
    dag=dag
)


# 结果写入mysql
def sync_result_to_mysql(**op_kwargs):
    wxapi = ComwxApi('wwd26d45f97ea74ad2', 'BLE_v25zCmnZaFUgum93j3zVBDK-DjtRkLisI_Wns4g', '1000011')

    dt = op_kwargs.get('ds', time.strftime('%Y-%m-%d', time.localtime(time.time() - 86400)))
    sql = '''
        SELECT 
            city_id, user_id, user_type, phone_number, create_at 
        FROM {table} 
        WHERE dt = '{dt}' 
    '''.format(table=hive_table, dt=dt)
    hive_cursor = get_hive_cursor()
    logging.info(sql)
    hive_cursor.execute(sql)
    # res = hive_cursor.fetchall()
    mysql_cursor = None

    try:
        mconn = get_db_conn('mysql_bi_utf8mb4')
        mysql_cursor = mconn.cursor()
        msql = '''
            TRUNCATE TABLE oride_dw.app_oride_users_tag_w
        '''
        logging.info(msql)
        mysql_cursor.execute(msql)

        msql = '''
            INSERT INTO oride_dw.app_oride_users_tag_w VALUES 
        '''
        rows = []
        cnt = 0
        while True:
            try:
                record = hive_cursor.next()
            except:
                record = None
            # logging.info(record)
            if not record:
                break
            rows.append("('{}')".format("','".join([str(x) for x in record])))
            # logging.info(rows)
            cnt += 1
            if cnt >= 2000:
                logging.info(cnt)
                mysql_cursor.execute("{h} {v}".format(
                    h=msql,
                    v=",".join(rows)
                ))
                cnt = 0
                rows = []

        # logging.info(rows)
        if cnt > 0:
            logging.info("last: {}".format(cnt))
            mysql_cursor.execute("{h} {v}".format(
                h=msql,
                v=",".join(rows)
            ))
        mysql_cursor.close()
    except BaseException as e:
        logging.info(e)
        if mysql_cursor:
            mysql_cursor.close()
        wxapi.postAppMessage(
            '重要重要重要：用户标签数据写入mysql异常【{}】'.format(dt),
            '271'
        )


sync_task = PythonOperator(
    task_id='sync_task',
    python_callable=sync_result_to_mysql,
    provide_context=True,
    op_kwargs={
        "ds": '{{ macros.ds_add(ds, 6) }}'
    },
    dag=dag
)


dependence_table_data_user_extend_task >> sleep_time
dependence_table_data_user_task >> sleep_time
dependence_table_data_order_task >> sleep_time

sleep_time >> create_result_table_task >> insert_result_to_hive >> sync_task
