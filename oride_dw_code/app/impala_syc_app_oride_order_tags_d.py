# -*- coding: utf-8 -*-
"""
调度算法效果监控指标新版2019-08-02
"""
import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.impala_plugin import ImpalaOperator
from airflow.sensors import UFileSensor
from airflow.sensors import WebHdfsSensor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from utils.connection_helper import get_hive_cursor
from plugins.comwx import ComwxApi
from datetime import datetime, timedelta
from plugins.DingdingAlert import DingdingAlert
import re
import logging

#comwx = ComwxApi('wwd26d45f97ea74ad2', 'BLE_v25zCmnZaFUgum93j3zVBDK-DjtRkLisI_Wns4g', '1000011')
dingding_alert = DingdingAlert('https://oapi.dingtalk.com/robot/send?access_token=928e66bef8d88edc89fe0f0ddd52bfa4dd28bd4b1d24ab4626c804df8878bb48')

args = {
    'owner': 'yangmingze',
    'start_date': datetime(2019, 8, 25),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'impala_syc_app_oride_order_tags_d',
    schedule_interval="30 03 * * *",
    default_args=args
)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 120',
    dag=dag
)

sleep_time2 = BashOperator(
    task_id='sleep_i2',
    depends_on_past=False,
    bash_command='sleep 20',
    dag=dag
)

"""
##----依赖数据源---##
"""
dependence_ods_log_oride_order_skyeye_di = HivePartitionSensor(
    task_id="dependence_ods_log_oride_order_skyeye_di",
    table="ods_log_oride_order_skyeye_di",
    partition="dt='{{ ds }}'",
    schema="oride_dw_ods",
    poke_interval=60,
    dag=dag
)

dependence_ods_oride_data_order = HivePartitionSensor(
    task_id="dependence_ods_oride_data_order",
    table="ods_sqoop_base_data_order_df",
    partition="dt='{{ ds }}'",
    schema="oride_dw_ods",
    poke_interval=60,
    dag=dag
)

dependence_data_city_conf = HivePartitionSensor(
    task_id="dependence_data_city_conf",
    table="ods_sqoop_base_data_city_conf_df",
    partition="dt='{{ ds }}'",
    schema="oride_dw_ods",
    poke_interval=60,
    dag=dag
)

"""
##-----end-------##
"""

hive_table = 'oride_dw.app_oride_order_tags_d'

create_result_impala_table = HiveOperator(
    task_id='create_result_impala_table',
    hql='''
        CREATE TABLE IF NOT EXISTS {table_name} (
            city_id BIGINT COMMENT '城市ID',   
            city_name STRING COMMENT '城市名称',   
            product_id BIGINT COMMENT '业务线',   
            tag_name STRING COMMENT 'tag名称',   
            hit_orders bigint COMMENT '命中订单数'   
        ) 
        PARTITIONED BY (
            country_code STRING COMMENT '二位国家码',
            dt STRING COMMENT '日期' 
        ) 
        STORED AS PARQUET
       '''.format(table_name=hive_table),
    schema='oride_dw',
    priority_weight=50,
    dag=dag
)


def drop_partions(*op_args, **op_kwargs):
    dt = op_kwargs['ds']
    cursor = get_hive_cursor()
    sql = '''
        show partitions {table_name}
    '''.format(table_name=hive_table)
    cursor.execute(sql)
    res = cursor.fetchall()
    logging.info(res)
    for partition in res:
        prt, = partition
        matched = re.search(r'country_code=(?P<cc>\w+)/dt=(?P<dy>.*)$', prt)
        cc = matched.groupdict().get('cc', 'nal')
        dy = matched.groupdict().get('dy', '')
        if dy == dt:
            hql = '''
                ALTER TABLE {table_name} DROP IF EXISTS PARTITION (country_code='{cc}', dt='{dt}')
            '''.format(cc=cc, dt=dt, table_name=hive_table)
            logging.info(hql)
            cursor.execute(hql)


drop_partitons_from_table = PythonOperator(
    task_id='drop_partitons_from_table',
    python_callable=drop_partions,
    provide_context=True,
    dag=dag
)


insert_result_to_impala = HiveOperator(
    task_id='insert_result_to_impala',
    hql="""
        set mapred.job.queue.name=root.users.airflow;
        set hive.execution.engine=tez;
        set hive.mapjoin.hybridgrace.hashtable=false;
        set hive.vectorized.execution.enabled=false;
        --set hive.vectorized.execution.enabled = true;
        --set hive.vectorized.execution.reduce.enabled = true;
        set hive.prewarm.enabled=true;
        set hive.prewarm.numcontainers=16;
        --set hive.exec.parallel=true;
        with
        --分城市、分类型 
        tag_part_data as (
            select 
                do.city_id as city_id,
                'null' as city_name,
                do.serv_type as serv_type,
                t.tag,
                count(distinct t.order_id) as orders 
            from 
                (select 
                    tags.tag as tag,
                    order_id
                from ods_log_oride_order_skyeye_di 
                lateral view posexplode(tag_ids) tags as pos, tag 
                where dt='{pt}' 
                ) as t
            inner join (select * from oride_dw_ods.ods_sqoop_base_data_order_df where dt='{pt}') as do  
            where t.order_id = do.id and 
                from_unixtime(do.create_time, 'yyyy-MM-dd') = '{pt}'
            group by 
                do.city_id, do.serv_type, t.tag
        ),
        --城市全部类型
        tag_city_data as (
            select 
                do.city_id as city_id,
                'null' as city_name,
                -1 as serv_type,
                t.tag,
                count(distinct t.order_id) as orders 
            from 
                (select 
                    tags.tag as tag,
                    order_id
                from ods_log_oride_order_skyeye_di 
                lateral view posexplode(tag_ids) tags as pos, tag 
                where dt='{pt}' 
                ) as t
            inner join (select * from oride_dw_ods.ods_sqoop_base_data_order_df where dt='{pt}') as do  
            where t.order_id = do.id and 
                from_unixtime(do.create_time, 'yyyy-MM-dd') = '{pt}'
            group by 
                do.city_id, t.tag
        ),
        --类型全部城市
        tag_type_data as (
            select 
                0 as city_id,
                'null' as city_name,
                do.serv_type as serv_type,
                t.tag,
                count(distinct t.order_id) as orders 
            from 
                (select 
                    tags.tag as tag,
                    order_id
                from ods_log_oride_order_skyeye_di 
                lateral view posexplode(tag_ids) tags as pos, tag 
                where dt='{pt}' 
                ) as t
            inner join (select * from oride_dw_ods.ods_sqoop_base_data_order_df where dt='{pt}') as do  
            where t.order_id = do.id and 
                from_unixtime(do.create_time, 'yyyy-MM-dd') = '{pt}'
            group by 
                do.serv_type, t.tag
        ),
        --全部城市，全部类型
        tag_all_data as (
            select 
                0 as city_id,
                'null' as city_name,
                -1 as serv_type,
                tags.tag as tag,
                count(distinct order_id) as orders
            from ods_log_oride_order_skyeye_di 
            lateral view posexplode(tag_ids) tags as pos, tag 
            where dt='{pt}' 
            group by tags.tag  
        )
        insert overwrite table {table_name} PARTITION (country_code='nal', dt='{pt}')
        select 
            city_id,
            city_name,
            serv_type,
            tag,
            orders
        from tag_all_data union 
        select 
            city_id,
            city_name,
            serv_type,
            tag,
            orders
        from tag_city_data union 
        select 
            city_id,
            city_name,
            serv_type,
            tag,
            orders
        from tag_part_data union 
        select 
            city_id,
            city_name,
            serv_type,
            tag,
            orders
        from tag_type_data
    """.format(pt='{{ ds }}', table_name=hive_table),
    schema='oride_dw_ods',
    priority_weight=50,
    dag=dag
)

refresh_impala_table_other = ImpalaOperator(
    task_id='refresh_impala_table_other',
    hql="""
        REFRESH oride_bi.oride_global_daily_report;
        REFRESH oride_dw_ods.ods_sqoop_base_data_city_conf_df;
        REFRESH oride_bi.oride_global_city_serv_daily_report; 
    """,
    schema='oride_bi',
    dag=dag
)

refresh_impala_table_self = ImpalaOperator(
    task_id='refresh_impala_table',
    hql="""
        REFRESH {table_name}; 
    """.format(table_name=hive_table),
    schema='oride_dw',
    dag=dag
)


dependence_data_city_conf >> sleep_time2
dependence_ods_log_oride_order_skyeye_di >> sleep_time
dependence_ods_oride_data_order >> sleep_time
sleep_time2 >> refresh_impala_table_other
sleep_time2 >> sleep_time >> create_result_impala_table >> drop_partitons_from_table >> insert_result_to_impala >> refresh_impala_table_self
