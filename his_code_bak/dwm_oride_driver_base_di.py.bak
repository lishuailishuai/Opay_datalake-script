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
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
        'owner': 'lijialong',
        'start_date': datetime(2019,8,31),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=2),
        'email': ['bigdata_dw@opay-inc.com'],
        'email_on_failure': True,
        'email_on_retry': False,
} 

dag = airflow.DAG( 'dwm_oride_driver_base_di', 
    schedule_interval="30 01 * * *", 
    default_args=args) 


##----------------------------------------- 依赖 ---------------------------------------## 


# 依赖前一天分区
dim_oride_driver_base_prev_day_tesk = UFileSensor(
    task_id='dim_oride_driver_base_prev_day_tesk',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dim_oride_driver_base/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_oride_order_base_include_test_di_prev_day_tesk = UFileSensor(
    task_id='dwd_oride_order_base_include_test_di_prev_day_tesk',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_oride_order_push_driver_detail_di_prev_day_tesk = UFileSensor(
    task_id='dwd_oride_order_push_driver_detail_di_prev_day_tesk',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_push_driver_detail_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
oride_driver_timerange_prev_day_tesk = HivePartitionSensor(
    task_id="oride_driver_timerange_prev_day_tesk",
    table="ods_log_oride_driver_timerange",
    partition="dt='{{ds}}'",
    schema="oride_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------## 


db_name = "oride_dw"
table_name="dwm_oride_driver_base_di"
hdfs_path="ufile://opay-datalake/oride/oride_dw/"+table_name

##----------------------------------------- 任务超时监控 ---------------------------------------## 

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "3600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)



task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)
##----------------------------------------- 脚本 ---------------------------------------## 

def dwm_oride_driver_base_di_sql_task(ds):

    HQL='''
        set hive.exec.parallel=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
    
        INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
        SELECT product_id,
           city_id,
           driver_id,
           driver_finish_order_dur,
           --完单做单时长(分钟）
    
           driver_cannel_pick_dur,
           --取消订单时长（分钟）
    
           driver_free_dur,
           --司机空闲时长（分钟）
    
           succ_push_order_cnt,
           --成功推送司机的订单数
    
           finish_driver_online_dur,
            --完单司机在线时长（分钟）
            
            driver_click_order_cnt,
            --司机点击接受订单总数（accpet_click阶段，算法要求此指标为订单总应答）
            
            driver_pushed_order_cnt,
            --司机被推送订单总数（accpet_show阶段，算法要求此指标为订单总推送）
            
            driver_billing_dur,
            --司机订单计费时长

            driver_finish_ord_num,
            --司机完成订单数
    
            driver_finish_pay_order_num,
            --司机支付完单数

            driver_finish_gmv,
            --司机完单gmv
            
            is_finish_driver,
            --是否完单司机标志

           country_code,
           --国家码字段
    
           '{pt}' AS dt
    FROM
    (
            SELECT dri.product_id,
            dri.city_id,
            dri.country_code,
            dri.driver_id,
            sum(nvl(td_finish_order_dur,0)) AS driver_finish_order_dur,
            --完单做单时长(秒）
    
            sum(nvl(td_cannel_pick_dur,0)) AS driver_cannel_pick_dur,
            --取消订单时长（秒）
    
            sum(nvl(dtr.driver_freerange,0)) AS driver_free_dur,
            --司机空闲时长（秒）
    
            sum(DISTINCT (CASE WHEN ord.driver_id=p1.driver_id THEN ord.succ_push_order_cnt ELSE 0 END)) AS succ_push_order_cnt,--成功推送司机的订单数
    
            
            sum(if(ord.is_td_finish>=1,nvl(dtr.driver_freerange,0) + nvl(ord.td_billing_dur,0) + nvl(ord.td_cannel_pick_dur,0),0)) AS finish_driver_online_dur,
            --完单司机在线时长（秒）
            
            sum(nvl(c1.driver_click_order_cnt,0)) as driver_click_order_cnt,
            --司机点击接受订单总数（accpet_click阶段，算法要求此指标为订单总应答）
            
            sum(nvl(s1.driver_pushed_order_cnt,0)) as driver_pushed_order_cnt,
            --司机被推送订单总数（accpet_show阶段，算法要求此指标为订单总推送）
            
            sum(nvl(ord.td_billing_dur,0)) as driver_billing_dur,
            --司机订单计费时长
            
            sum(nvl(ord.driver_finish_ord_num,0)) as driver_finish_ord_num,
            --司机当日完成订单数(完单量)
           
            sum(nvl(ord.driver_finish_pay_order_num,0)) as driver_finish_pay_order_num,
            --司机支付完单量

            sum(nvl(finish_gmv,0)) as driver_finish_gmv,
            --司机完单gmv
            if(ord.is_td_finish>=1,1,0) as is_finish_driver
            --是否完单司机标志

    
       FROM
            (
                SELECT 
                *
                FROM oride_dw.dim_oride_driver_base
                WHERE dt='{pt}'
            ) dri
            LEFT OUTER JOIN
            (
                SELECT 
                driver_id,
                count(order_id) as succ_push_order_cnt,
                sum(if(is_td_finish = 1,td_finish_order_dur,0)) as td_finish_order_dur,
                sum(td_billing_dur) as td_billing_dur,
                sum(td_cannel_pick_dur) as td_cannel_pick_dur,
                sum(is_td_finish) as is_td_finish,  --用于判断该订单是否是完单                
                sum(if(is_td_finish = 1,1,0)) as driver_finish_ord_num,
                sum(if(is_td_finish_pay = 1, 1, 0)) as driver_finish_pay_order_num, --支付完单量
                sum(if(is_td_finish = 1, price, 0)) as finish_gmv --当日完单gmv
                FROM oride_dw.dwd_oride_order_base_include_test_di
                WHERE dt='{pt}'
                AND city_id<>'999001' --去除测试数据
                group by driver_id
            ) ord ON dri.driver_id=ord.driver_id
            LEFT OUTER JOIN
            (
                SELECT *
                FROM oride_dw_ods.ods_log_oride_driver_timerange
                WHERE dt='{pt}'
            ) dtr ON dri.driver_id=dtr.driver_id
            AND dri.dt=dtr.dt
            LEFT OUTER JOIN
            (
                SELECT driver_id --成功播单司机
                FROM oride_dw.dwd_oride_order_push_driver_detail_di
                WHERE dt='{pt}'
                AND success=1
                GROUP BY driver_id
            ) p1 ON ord.driver_id=p1.driver_id
            LEFT OUTER JOIN
            (
                SELECT 
                driver_id,
                count(distinct(order_id)) driver_click_order_cnt
                FROM 
                oride_dw.dwd_oride_driver_accept_order_click_detail_di
                WHERE dt='{pt}'
                GROUP BY driver_id
            ) c1 on dri.driver_id=c1.driver_id
            LEFT OUTER JOIN
            (
                SELECT 
                driver_id,
                count(distinct(order_id)) driver_pushed_order_cnt
                FROM 
                oride_dw.dwd_oride_driver_accept_order_show_detail_di
                WHERE dt='{pt}'
                GROUP BY driver_id
            ) s1 on dri.driver_id=s1.driver_id
            
       GROUP BY dri.product_id,
                dri.city_id,
                dri.country_code,
                dri.driver_id,
                if(ord.is_td_finish>=1,1,0)
    ) x
    WHERE x.country_code IN ('nal');

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
    _sql = dwm_oride_driver_base_di_sql_task(ds)

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


dwm_oride_driver_base_di_task = PythonOperator(
    task_id='dwm_oride_driver_base_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dim_oride_driver_base_prev_day_tesk >>dwm_oride_driver_base_di_task 
dwd_oride_order_base_include_test_di_prev_day_tesk >> dwm_oride_driver_base_di_task
dwd_oride_order_push_driver_detail_di_prev_day_tesk >>dwm_oride_driver_base_di_task 
oride_driver_timerange_prev_day_tesk  >> dwm_oride_driver_base_di_task