# -*- coding: utf-8 -*-
import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.hive_hooks import HiveCliHook
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.sensors import UFileSensor
from airflow.sensors import OssSensor
from airflow.sensors.s3_key_sensor import S3KeySensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'lijialong',
    'start_date': datetime(2020, 2, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwm_oride_assets_sku_df',
                  schedule_interval="30 01 * * *",
                  default_args=args)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dwm_oride_assets_sku_df"
# 路径
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 依赖 ---------------------------------------##

dwd_oride_assets_sku_df_prev_day_task = OssSensor(
    task_id='dwd_oride_assets_sku_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_assets_sku_df/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
dwd_oride_properties_df_prev_day_task = OssSensor(
    task_id='dwd_oride_properties_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_properties_df/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_oride_admin_business_df_prev_day_task = OssSensor(
    task_id='dwd_oride_admin_business_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_admin_business_df/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_oride_warehouses_df_prev_day_task = OssSensor(
    task_id='dwd_oride_warehouses_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_push_driver_detail_di/country_code=NG",
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
         "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def dwm_oride_assets_sku_df_sql_task(ds):
    HQL = '''
   set hive.exec.parallel=true;
   set hive.exec.dynamic.partition.mode=nonstrict;
    
       
    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
    select 
      sku.ware_id      ,--仓库ID
      sku.property_id      ,--资产ID
      sn     ,--资产SN
      claim_user_id      ,--认领人ID，即运营ID
      claim_time      ,--认领时间
      claimed_rider_id      ,--骑手招募ID
      claimed_driver_id      ,--骑手OrideID
      claimed_time      ,--领取时间
      sku.status    ,--状态: 1:在仓库,2:正在转库中,3:发放给运营中,4:运营已接收领用,5:运营归还给仓库中,6:已发放给骑手,50-丢失未赔付,51-丢失已赔付,99-被删除
      update_time      ,--更新时间
      create_time      ,--添加时间
      retrieved_num      ,--回收次数：默认0，表示没有被回收过，每次回收时+1，该数字即表示总共回收过多少次
      retrieved_time      ,--最后一次回收时间，每次回收时进行更新
      run_status     ,--运营中资产状态
      run_update_time,  --运营中资产状态最后一次更改时间
      property_name,--'资产名称',
      cate_ids,--'资产分类,多个逗号分隔',
      supplier,--'供应商',
      producer,--'生产商',
      consumable,--'是否是消费品',
      consumable_special,--'消费品特殊字段',
      common_attribute,--'公用属性',
      custom_attribute,--'定制属性',
      ip,--'操作IP',
      prop.user_id,--'操作人ID',
      prop.business_id,--'业务线Id',
      tag_id,--'资产所属标签',
      name ,--'业务线名称',
      business.status as business_status, --'业务线状态 是否启用 1-启用 2-禁用',
      ware_name  ,-- '仓库名称',  
      ware_address  ,-- '仓库地址',   
      ware_code  ,-- '仓库编号',  
      country_id  , -- '仓库所属国家',  
      city_id  , -- '仓库所属city',   
      is_inbound, -- '是否允许该仓库进行inbound操作？0-不允许，1-允许',   
      staff_city_ids, -- '仓库对应资产发放覆盖城市：多个城市ID之间使用英文逗号分
      if(retrieved_num >0,1,0) as is_retrieved, --'资产新旧,是否由回收员操作过回收。操作过为"1 二手"，未操作过为" 0 一手"'
      'NG' as country_code,
      dt
    from
    (
    
        select
          ware_id      ,--仓库ID
          property_id      ,--资产ID
          sn     ,--资产SN
          claim_user_id      ,--认领人ID，即运营ID
          claim_time      ,--认领时间
          claimed_rider_id      ,--骑手招募ID
          claimed_driver_id      ,--骑手OrideID
          claimed_time      ,--领取时间
          status    ,--状态: 1:在仓库,2:正在转库中,3:发放给运营中,4:运营已接收领用,5:运营归还给仓库中,6:已发放给骑手,50-丢失未赔付,51-丢失已赔付,99-被删除
          update_time      ,--更新时间
          create_time      ,--添加时间
          business_id     ,--业务线Id
          retrieved_num      ,--回收次数：默认0，表示没有被回收过，每次回收时+1，该数字即表示总共回收过多少次
          retrieved_time      ,--最后一次回收时间，每次回收时进行更新
          run_status     ,--运营中资产状态
          run_update_time  --运营中资产状态最后一次更改时间     
        from  oride_dw.dwd_oride_assets_sku_df
        where dt ='2020-01-14' and status  != '99' 
    )sku
    left join
    (
        select
            property_id,--'资产ID',
            property_name,--'资产名称',
            cate_ids,--'资产分类,多个逗号分隔',
            supplier,--'供应商',
            producer,--'生产商',
            consumable,--'是否是消费品',
            consumable_special,--'消费品特殊字段',
            common_attribute,--'公用属性',
            custom_attribute,--'定制属性',
            ip,--'操作IP',
            user_id,--'操作人ID',
            business_id,--'业务线Id',
            tag_id--'资产所属标签',
        from oride_dw.dwd_oride_properties_df
        where dt ='2020-01-14'
    )prop  on sku.property_id = prop.property_id   --and sku.business_id = prop.business_id   property_id和 business_id 一一对应
    left join
    (
        SELECT
            id ,--'主键ID--业务线id',
            name ,--'业务线名称',
            status --'是否启用 1-启用 2-禁用',
        from oride_dw.dwd_oride_admin_business_df  
        WHERE dt ='2020-01-14'
    )business on  prop.business_id = business.id
    left join
    (
        select       
            ware_id  , -- '仓库ID',   
            ware_name  ,-- '仓库名称',  
            ware_address  ,-- '仓库地址',   
            user_id, -- '操作人ID',  
            ware_code  ,-- '仓库编号',  
            country_id  , -- '仓库所属国家',  
            city_id  , -- '仓库所属city',   
            is_inbound, -- '是否允许该仓库进行inbound操作？0-不允许，1-允许',   
            staff_city_ids -- '仓库对应资产发放覆盖城市：多个城市ID之间使用英文逗号分
        from oride_dw.dwd_oride_warehouses_df 
        where dt = '2020-01-14'
)ware on sku.ware_id = ware.ware_id;

    '''.format(
        pt=ds,
        bef_yes_day=airflow.macros.ds_add(ds, -1),
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )
    return HQL



# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwm_oride_assets_sku_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    #check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwm_oride_assets_sku_df = PythonOperator(
    task_id='dwm_oride_assets_sku_df',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_oride_assets_sku_df_prev_day_task >> dwm_oride_assets_sku_df
dwd_oride_properties_df_prev_day_task >> dwm_oride_assets_sku_df
dwd_oride_admin_business_df_prev_day_task >> dwm_oride_assets_sku_df
dwd_oride_warehouses_df_prev_day_task >> dwm_oride_assets_sku_df
