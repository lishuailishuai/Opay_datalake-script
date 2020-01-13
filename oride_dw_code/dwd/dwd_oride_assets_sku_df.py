# -*- coding: utf-8 -*-
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
from plugins.CountriesPublicFrame import CountriesPublicFrame
import json
import logging
from airflow.models import Variable
import requests
import os
from airflow.sensors import OssSensor

args = {
    'owner': 'lijialong',
    'start_date': datetime(2020, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_assets_sku_df',
                  schedule_interval="00 01 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

ods_sqoop_base_oride_assets_sku_df_task = OssSensor(
    task_id='ods_sqoop_base_oride_properties_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="oride_dw_sqoop/opay_assets/oride_assets_sku",

        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dwd_oride_assets_sku_df"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name


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

def dwd_oride_assets_sku_df_sql_task(ds):
    HQL = '''


    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)

    select

          id   ,--自增主键
          ware_id      ,--仓库ID
          property_id      ,--资产ID
          sn     ,--资产SN
          custom_attribute_value   ,--自定义字段值
          claim_user_id      ,--认领人ID，即运营ID
          to_claim_time      ,--给运营发放时间
          to_claim_desc     ,--给运营发放时的描述
          to_claim_pics   ,--给运营发放时的照片
          claim_time      ,--认领时间
          claim_desc     ,--认领时描述
          claim_pics   ,--认领时照片，非必填
          claimed_rider_id      ,--骑手招募ID
          claimed_driver_id      ,--骑手OrideID
          claimed_time      ,--领取时间
          claimed_pics   ,--骑手领取时拍照
          status    ,--状态: 1:在仓库,2:正在转库中,3:发放给运营中,4:运营已接收领用,5:运营归还给仓库中,6:已发放给骑手,50-丢失未赔付,51-丢失已赔付,99-被删除
          update_time      ,--更新时间
          create_time      ,--添加时间
          business_id     ,--业务线Id
          retrieved_num      ,--回收次数：默认0，表示没有被回收过，每次回收时+1，该数字即表示总共回收过多少次
          retrieved_time      ,--最后一次回收时间，每次回收时进行更新
          claimed_desc     ,--发送给骑手时的描述
          run_status     ,--运营中资产状态
          run_update_time      ,--运营中资产状态最后一次更改时间
          'NG' as country_code,
           dt

    from oride_dw_ods.ods_sqoop_base_oride_assets_sku_df
    where dt='{pt}'

    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kwargs):
    v_date = kwargs.get('v_execution_date')
    v_day = kwargs.get('v_execution_day')
    v_hour = kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

    """
        #功能函数
        alter语句: alter_partition
        删除分区: delete_partition
        生产success: touchz_success

        #参数
        第一个参数true: 数据目录是有country_code分区。false 没有
        第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

        #读取sql
        %_sql_task(ds,v_hour)

        第一个参数ds: 天级任务
        第二个参数v_hour: 小时级任务，需要使用

    """

    cf = CountriesPublicFrame("true", ds, db_name, table_name, hdfs_path, "true", "true")

    # 删除分区
    # cf.delete_partition()

    # 拼接SQL

    _sql = "\n" + cf.alter_partition() + "\n" + dwd_oride_assets_sku_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 熔断数据
    # check_key_data_task(ds)

    # 生产success
    cf.touchz_success()


dwd_oride_assets_sku_df_task = PythonOperator(
    task_id='dwd_oride_assets_sku_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

ods_sqoop_base_oride_assets_sku_df_task >> dwd_oride_assets_sku_df_task