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
from airflow.sensors import OssSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from plugins.CountriesPublicFrame import CountriesPublicFrame
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2019, 11, 15),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dm_oride_driver_order_base_cube',
                  schedule_interval="50 01 * * *",
                  default_args=args)
##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dm_oride_driver_order_base_cube"

##----------------------------------------- 依赖 ---------------------------------------##
#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":
    # 依赖前一天分区
    dependence_dwm_oride_driver_order_base_di_prev_day_task = UFileSensor(
        task_id='dwm_oride_driver_order_base_di_prev_day_task',
        filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_driver_order_base_di/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    #路径
    hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name
else:
    print("成功")
    dependence_dwm_oride_driver_order_base_di_prev_day_task = OssSensor(
        task_id='dwm_oride_driver_order_base_di_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_driver_order_base_di/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    # 路径
    hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
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

def dm_oride_driver_order_base_cube_sql_task(ds):
    HQL = '''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)

    select nvl(city_id,-10000) as city_id,

       nvl(product_id,-10000) as product_id,
       
       td_request_driver_num,
       --当日接单司机数
       
       td_finish_order_driver_num,
       --当日完单司机数
       
       driver_request_order_cnt,
       --司机接单量（理论和应答量一样）
       
       driver_finish_order_cnt,
       --司机完单量
       
       driver_finished_pay_order_cnt,
       --司机支付完单量
       
       driver_finish_price,
       --司机完单gmv
       
       driver_billing_dur,
       --司机计费时长
       
       driver_service_dur,
       --司机服务时长
       
       driver_finished_dur,
       --司机支付完单做单时长（支付跨天可能偏大）
       
       driver_cannel_pick_dur,
       -- 司机当天订单被取消时长,不可用于计算司机在线时长 
       
       nvl(country_code,'total') as country_code,
       
       '{pt}' as dt              
from (select city_id,

           product_id,
           --下单业务类型
           
           count(distinct if(driver_request_order_cnt>=1,driver_id,null)) as td_request_driver_num,
           --当日接单司机数
           
           count(distinct if(driver_finish_order_cnt>=1,driver_id,null)) as td_finish_order_driver_num,
           --当日完单司机数
           
           sum(driver_request_order_cnt) as driver_request_order_cnt,
           --司机接单量（理论和应答量一样）
           
           sum(driver_finish_order_cnt) as driver_finish_order_cnt,
           --司机完单量
           
           sum(driver_finished_pay_order_cnt) as driver_finished_pay_order_cnt,
           --司机支付完单量
           
           sum(driver_finish_price) as driver_finish_price,
           --司机完单gmv
           
           sum(driver_billing_dur) as driver_billing_dur,
           --司机计费时长
           
           sum(driver_service_dur) as driver_service_dur,
           --司机服务时长
           
           sum(driver_finished_dur) as driver_finished_dur,
           --司机支付完单做单时长（支付跨天可能偏大）
           
           sum(driver_cannel_pick_dur) as driver_cannel_pick_dur,
           -- 司机当天订单被取消时长,不可用于计算司机在线时长
           
           country_code
           --二位国家码  --(去除with cube为空的BUG)
              
        from oride_dw.dwm_oride_driver_order_base_di
        where dt='{pt}'   
        group by city_id,
        
               product_id,
               --下单业务类型 
                 
               country_code
               --二位国家码
               
               with cube) t;   

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
            第一个参数true: 所有国家是否上线。false 没有
            第二个参数true: 数据目录是有country_code分区。false 没有
            第三个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

            #读取sql
            %_sql(ds,v_hour)

            第一个参数ds: 天级任务
            第二个参数v_hour: 小时级任务，需要使用

        """
    cf = CountriesPublicFrame("true", ds, db_name, table_name, hdfs_path, "true", "true")

    # 删除分区
    cf.delete_partition()

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dm_oride_driver_order_base_cube_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 生产success
    cf.touchz_success()

dm_oride_driver_order_base_cube_task = PythonOperator(
    task_id='dm_oride_driver_order_base_cube_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

dependence_dwm_oride_driver_order_base_di_prev_day_task >> dm_oride_driver_order_base_cube_task