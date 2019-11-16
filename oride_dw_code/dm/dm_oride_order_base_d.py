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
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'yangmingze',
    'start_date': datetime(2019, 5, 20),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dm_oride_order_base_d',
                  schedule_interval="00 02 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------## 

# 依赖前一天分区
dwm_oride_order_base_di_prev_day_task = UFileSensor(
    task_id='dwm_oride_order_base_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwm_oride_order_base_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name="oride_dw"
table_name = "dm_oride_order_base_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 任务超时监控 ---------------------------------------## 

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------## 


def dm_oride_order_base_d_sql_task(ds):

    HQL='''

    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    
    insert overwrite table  oride_dw.{table} partition(country_code,dt)
    select city_id,
       product_id,
       count(1) as ride_order_cnt, --下单量
       sum(is_request) as request_order_cnt, --接单量
       sum(is_finish) as finish_order_cnt, --完单量
       sum(pick_up_order_dur) as pick_up_order_dur, --当天接驾订单时长
       sum(take_order_dur) as take_order_dur, --当天应答订单时长
       sum(cannel_pick_order_dur) as cannel_pick_order_dur,--当天取消接驾订单时长
       sum(wait_order_dur) as wait_order_dur, -- 当天等待上车订单时长
       sum(billing_order_dur) as billing_order_dur, --废弃字段
       sum(pay_order_dur) as pay_order_dur, --当天支付订单时长
       sum(is_sys_cancel) as sys_cancel_order_cnt, --当天系统取消订单数
       sum(is_passanger_before_cancel) as passanger_before_cancel_order_cnt, --当天乘客应答前取消订单数
       sum(is_passanger_after_cancel) as passanger_after_cancel_order_cnt, --当天乘客应答后取消订单数
       sum(is_finished_pay) as finish_pay, --当天完成支付
       sum(succ_broadcast_distance) as succ_broadcast_dis, --当天成功播单距离(米)
       sum(is_driver_after_cancel) as driver_after_cancel_order_cnt, --当天司机应答后取消订单数
       sum(if(is_finish=1,billing_order_dur,0)) as finish_billing_dur, --当天完单计费时长,该字段和当天计费订单时长不一样
       sum(finished_order_dur) as finish_order_dur, --当天支付完单做单时长
       sum(if(is_finish=1,pick_up_distance,0)) as finish_order_pick_up_dis, --完单接驾距离(米)
       sum(is_succ_broadcast) as succ_broadcast_cnt, --成功播单数
       sum(is_broadcast) as broadcast_cnt, --播单数，之前在chose节点统计的有问题？？？
       sum(is_accpet_click) as driver_accpet_order_cnt, --司机应答订单数(accpet_click阶段)
       null as dispatch_push_driver_order_cnt, --推送给骑手的订单数(推送给骑手的订单目前只在骑手端show)，之前在push节点统计的有问题？？？？废弃,跟push节点broadcast_cnt一样
       sum(push_all_times_cnt) as push_driver_times_cnt, -- 推送给司机的总次数 
       sum(succ_push_all_times) as succ_push_all_times_cnt, --推送成功的总次数
       sum(nvl(finished_order_dur,0)+nvl(cannel_pick_order_dur,0)) as accept_order_dur, --当天做单时长(当天完成做单时长+当天取消接驾时长）
       sum(is_accpet_show) as push_driver_order_accpet_show_cnt, --司机被推送订单数,即推送给骑手的订单数(accpet_show阶段)
       sum(if(is_finish=1,order_service_dur,0)) as service_dur, -- 当天完单服务时长，服务时长不一定都是完单的，有status=6的也有arrive_time
       sum(driver_click_times_cnt) as driver_click_times_cnt, --司机应答的总次数(accpet_click阶段)
       sum(if(is_finish=1,take_order_dur,0)) as finish_take_order_dur, -- 当天完成订单应答时长
       sum(if(is_finish=1,order_onride_distance,0)) as finish_order_onride_dis, --完单送驾距离(米)
       sum(if(is_finish=1,order_assigned_cnt,0)) as finish_order_pick_up_assigned_cnt, --完单订单被分配次数（计算平均接驾距离使用）
       sum(if(is_finish=1,price,0)) as price, --当日完单gmv
       sum(if(is_finished_pay=1,price,0)) as pay_price, --当日应付金额
       sum(if(is_finished_pay=1,pay_amount,0)) as pay_amount, -- 当日实付金额
       sum(is_valid) as valid_ord_cnt, --当日有效订单数
       sum(if(is_finish=1,pick_up_order_dur,0)) as finish_pick_up_dur, --当日完单接驾时长 
       sum(if(is_finish=1,pax_num,0)) as pax_num, --乘客数
       sum(is_opay_pay) as opay_pay_cnt, --当日opay支付订单数,pay_mode=2
       sum(if(is_opay_pay=1 and is_succ_pay=0,1,0)) as opay_pay_failed_cnt, --当日opay支付失败订单数,pay_mode=2 and pay_status in(0,2)
       sum(is_wet_order) as wet_ord_cnt, --湿单订单量
       sum(if(score in(1,2) and is_finish=1,1,0)) as bad_feedback_finish_ord_cnt, -- 差评完单量
       sum(is_after_cancel) as after_cancel_order_cnt, --应答后取消订单数
       sum(if(is_passanger_after_cancel=1,cannel_pick_order_dur,0)) as passanger_after_cancel_time_dur, -- 乘客应答后取消时长（秒）
       sum(if(is_driver_after_cancel=1,cannel_pick_order_dur,0)) as driver_after_cancel_time_dur, --司机应答后取消时长（秒）
       sum(if(is_passanger_before_cancel=1 or is_driver_after_cancel=1,order_onride_distance,0)) as passanger_cancel_order_dis, --乘客取消订单送驾距离????
       sum(if(is_request=1,pick_up_distance,0)) as accept_order_pick_up_dis, --应答单接驾距离(米)（计算平均接驾距离（应答单使用））先使用接单标志？？？？
       sum(if(is_request=1,order_assigned_cnt,0)) as accept_order_pick_up_assigned_cnt, 
       -- 应答单分配次数（应答单接驾距离(米)（计算平均接驾距离（应答单使用））先使用接单标志？？？？之前逻辑有问题
       null as column1,
       driver_serv_type, --订单表与司机绑定的业务类型
       sum(is_carpool) as carpool_num, --拼车订单数
       sum(is_chartered_bus) as chartered_bus_num, --包车订单数
       sum(is_carpool_success) as carpool_success_num, --拼成订单数
       sum(if(is_request=1 and is_carpool=1,1,0)) as carpool_accept_num, -- 拼车应答订单数
       sum(if(is_finish=1 and is_carpool_success=1,1,0)) as carpool_success_and_finish_num, --拼车成功且完单数
       country_code,
       dt as dt
from oride_dw.dwm_oride_order_base_di
where dt='{pt}'
group by city_id,
       product_id,
       driver_serv_type,
       country_code,
       dt;
'''.format(
        pt=ds,
        table=table_name,
        now_day='{{macros.ds_add(ds, +1)}}',
        db=db_name
        )
    return HQL

#主流程
def execution_data_task_id(ds,**kargs):

    hive_hook = HiveCliHook()

    #读取sql
    _sql=dm_oride_order_base_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    #执行Hive
    hive_hook.run_cli(_sql)

    #熔断数据
    #check_key_data_task(ds)

    #生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds,db_name,table_name,hdfs_path,"true","true")
    
dm_oride_order_base_d_task= PythonOperator(
    task_id='dm_oride_order_base_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwm_oride_order_base_di_prev_day_task >>dm_oride_order_base_d_task
