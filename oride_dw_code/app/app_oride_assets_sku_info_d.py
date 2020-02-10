# -*- coding: utf-8 -*-
"""资产sku统计"""

import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.sensors import UFileSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from airflow.sensors.s3_key_sensor import S3KeySensor
import json
import logging
from airflow.models import Variable
import requests
import os
from airflow.sensors import OssSensor

args = {
    'owner': 'lijialong',
    'start_date': datetime(2020, 2, 3),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_assets_sku_info_d',
                  schedule_interval="30 2 * * *",
                  default_args=args)
##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "app_oride_assets_sku_info_d"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 依赖 ---------------------------------------##

dependence_dwm_oride_assets_sku_df_task = OssSensor(
    task_id='dwm_oride_assets_sku_df_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwm_oride_assets_sku_df",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)
dependence_dim_oride_city_task = OssSensor(
    task_id="dim_oride_city_task",
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dim_oride_city",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dependence_dwm_oride_driver_finance_di_task = OssSensor(
    task_id='dwm_oride_driver_finance_di',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwm_oride_driver_finance_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dependence_dim_oride_driver_base_task = OssSensor(
    task_id='dim_oride_driver_base_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dim_oride_driver_base",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)




dependence_dwd_oride_driver_records_day_df_task = OssSensor(
    task_id='dwd_oride_driver_records_day_df_task',
    bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_driver_records_day_df",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dependence_dwm_oride_driver_base_df_task = OssSensor(
    task_id='dwm_oride_driver_base_df_task',
    bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwm_oride_driver_base_df",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)





dependence_dwd_oride_finance_driver_repayment_extend_df_task = OssSensor(
    task_id='dwd_oride_finance_driver_repayment_extend_df',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_finance_driver_repayment_extend_df/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"dag": dag, "db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "2400"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def app_oride_assets_sku_info_d_sql_task(ds):
    HQL = '''

        SET hive.exec.parallel=TRUE;
        set hive.exec.dynamic.partition.mode=nonstrict;    
      
        INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
        select 
          sku.business_name,--'业务线名称'
          case 
            when sku.status in (1,2,50) then sku.city_id
            when sku.status in (3,4,5) then admin.city_id
            when sku.status = 6 then dri.city_id
            else null
          end as city_id,
        
          --case 
            --when sku.status in (1,2,50) then sku.city_name
            --when sku.status in (3,4,5) then admin.city_name
            --when sku.status = 6 then dri.city_name
            --else null
          --end as city_name,
        
          sku.ware_id,--'仓库ID'
          sku.ware_name,--'仓库名称'
          sku.cate_ids,--'资产分类,多个逗号分隔'
          sku.sn,--'资产SN'
          sku.property_name,--'资产名称'
          sku.status,--资产状态
          sku.is_retrieved, --'资产新旧,是否由回收员操作过回收。操作过为"1 二手"，未操作过为" 0 一手"'
        
          FROM_UNIXTIME(sku.claimed_time ,'yyyy-MM-dd HH:mm:ss') as claimed_time,--'发放时间,资产发放给该司机的时间'
          dri.driver_id,
          dri.driver_name,
          dri.phone_number,
          dri.fault,--司机状态
          dri.product_id,
          FROM_UNIXTIME(driver_base.first_finish_order_create_time ,'yyyy-MM-dd HH:mm:ss') as first_finish_order_create_time, --首次完单时间
          FROM_UNIXTIME(driver_base.recent_finish_create_time ,'yyyy-MM-dd HH:mm:ss') as recent_finish_create_time, --最近一次完单时间
        
          case 
            when driver_base.first_finish_order_create_time is not null
              then (unix_timestamp('{pt}','yyyy-MM-dd') - driver_base.recent_finish_create_time)/3600  
            else (unix_timestamp('{pt}','yyyy-MM-dd') - sku.claimed_time)/3600 
          end as silence_duration, --沉默时长
        
          driver_base.driver_finish_order_cnt,--完单量
          driver_base.driver_finish_price,--完单gmv
          finance.amount_true,--'当日骑手-实际到手收入'
          finance.balance, --骑手余额
          finance.overdue_days, --逾期天数
          repay.already_amount,--累计实际还款金额

          'NG' as country_code,
          '{pt}' as dt
        from
        ( 
            select 
              s.business_name,--'业务线名称'
              s.city_id,--'城市id'
              s.ware_id,--'仓库ID'
              s.ware_name,--'仓库名称'
              s.cate_ids,--'资产分类,多个逗号分隔'
              s.sn,--'资产id(资产SN)'
              s.property_name,--'资产名称'
              s.status,--资产状态
              s.is_retrieved, --'资产新旧,是否由回收员操作过回收。操作过为"1 二手"，未操作过为" 0 一手"'
              s.claimed_driver_id,--'司机id'
              s.claim_user_id,--认领人ID，即运营ID
              s.claimed_time,--'发放时间,资产发放给该司机的时间'
              city.city_name
          from 
          (
            select
              business_name,--'业务线名称'
              city_id,--'城市id'
              ware_id,--'仓库ID'
              ware_name,--'仓库名称'
              cate_ids,--'资产分类,多个逗号分隔'
              sn,--'资产id(资产SN)'
              property_name,--'资产名称'
              status,--资产状态
              is_retrieved, --'资产新旧,是否由回收员操作过回收。操作过为"1 二手"，未操作过为" 0 一手"'
              claimed_driver_id,--'司机id'
              claim_user_id,--认领人ID，即运营ID
              claimed_time--'发放时间,资产发放给该司机的时间'
            from oride_dw.dwm_oride_assets_sku_df
            where dt = '{pt}'
          )s
          left join
          (
            SELECT city_id,city_name
            from oride_dw.dim_oride_city 
            WHERE dt='{pt}'
          )city on s.city_id = city.city_id
        )sku
        left join
        (
          select
            driver_id,
            driver_name,
            phone_number,
            fault,--司机状态
            product_id,
            city_id,
            city_name
          from oride_dw.dim_oride_driver_base
          where dt = '{pt}'
        )dri  on dri.driver_id = sku.claimed_driver_id
        left join
        ( 
          select 
            u.id,
            u.city_id,
            city.city_name
          from
          (  select 
              id, --运营id
              city_id
            from oride_dw_ods.ods_sqoop_base_admin_users_df 
            where dt = '{pt}'
          )u
          left join
          (
            SELECT city_id,city_name
            from oride_dw.dim_oride_city 
            WHERE dt='{pt}'
          )city on u.city_id = city.city_id
        )admin on sku.claim_user_id = admin.id
        left join
        (
          select
            driver_id,
            first_finish_order_create_time,--首次完单时间
            recent_finish_create_time,--最近一次完单时间
            driver_finish_order_cnt,--完单量
            driver_finish_price--完单gmv
          from oride_dw.dwm_oride_driver_base_df
          where dt = '{pt}' 
        )driver_base on driver_base.driver_id = sku.claimed_driver_id
        left join
        (
          select 
            driver_id,
            amount_true,--'当日骑手-实际到手收入'
            balance,--'骑手余额'
            if(balance<0, round(abs(balance)/amount),0) as overdue_days --逾期天数
          from oride_dw.dwm_oride_driver_finance_di
          where dt= '{pt}'
        )finance on finance.driver_id = sku.claimed_driver_id
        left join
        (
            select
              driver_id,
              already_amount--累计实际还款金额
            from oride_dw.dwd_oride_finance_driver_repayment_extend_df
            where dt = '{pt}'
        )repay on repay.driver_id = sku.claimed_driver_id
'''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_oride_assets_sku_info_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    # check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


app_oride_assets_sku_info_d_task = PythonOperator(
    task_id='app_oride_assets_sku_info_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

# dependence_dwm_oride_order_base_di_task >> dependence_dim_oride_city_task >> dependence_dim_oride_passenger_base_task >> \
# dependence_dim_oride_driver_base_task >> dependence_dwd_oride_order_finance_df_task >> dependence_dwd_oride_driver_records_day_df_task >> \
# dependence_dwd_oride_driver_recharge_records_df_task >> dependence_dm_oride_driver_base_task>>dependence_dm_oride_passenger_base_cube_task>>\
# dependence_dwd_oride_order_base_include_test_di_task>>app_oride_order_global_operate_to_mysql_d_task


dependence_dwd_oride_driver_records_day_df_task >> app_oride_assets_sku_info_d_task
dependence_dim_oride_city_task >> app_oride_assets_sku_info_d_task
dependence_dwm_oride_driver_finance_di_task >> app_oride_assets_sku_info_d_task
dependence_dwd_oride_driver_records_day_df_task >> app_oride_assets_sku_info_d_task
dependence_dim_oride_driver_base_task >> app_oride_assets_sku_info_d_task
dependence_dwd_oride_finance_driver_repayment_extend_df_task >> app_oride_assets_sku_info_d_task
dependence_dwm_oride_assets_sku_df_task>> app_oride_assets_sku_info_d_task
dependence_dwm_oride_driver_base_df_task>> app_oride_assets_sku_info_d_task