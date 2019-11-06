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
    'owner': 'lili.chen',
    'start_date': datetime(2019, 11, 3),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwm_oride_driver_base_d',
                  schedule_interval="30 01 * * *",
                  default_args=args)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
dim_oride_driver_base_prev_day_task = UFileSensor(
    task_id='dim_oride_driver_base_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dim_oride_driver_base/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
oride_driver_timerange_prev_day_task = UFileSensor(
    task_id='oride_driver_timerange_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw_ods/ods_log_oride_driver_timerange",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_oride_order_base_include_test_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_order_base_include_test_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_oride_order_push_driver_detail_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_order_push_driver_detail_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_push_driver_detail_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_oride_driver_accept_order_show_detail_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_driver_accept_order_show_detail_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_driver_accept_order_show_detail_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_oride_driver_accept_order_click_detail_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_driver_accept_order_click_detail_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_driver_accept_order_click_detail_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

table_name = "dwm_oride_driver_base_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "3600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

dwm_oride_driver_base_d_task = HiveOperator(

    task_id='dwm_oride_driver_base_d_task',
    hql='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
select dri.driver_id, 
            --司机ID
            
            dri.city_id,
            --城市ID
            
            dri.product_id,
            --司机绑定的业务类型
            
            dri.register_time,
            --司机注册时间
            
            if(substr(dri.register_time,1,10)='{pt}',1,0) as is_td_sign,
            --是否当天签约or注册
            
            dri.is_bind,
            --是否绑定车辆
            
            null as is_survival,
            --是否存活司机,逻辑待定
            
            if(dtr.driver_id is not null,1,0) as is_td_online,
            --当天是否在线
            
            if(push.driver_id is not null,1,0) as is_td_broadcast,
            --当天是否被播单（push节点）
            
            if(push.success>=1,1,0) as is_td_succ_broadcast,
            --当天是否被成功播单（push节点）
            
            if(show.driver_id is not null,1,0) as is_td_accpet_show,
            --当天是否被推送订单（骑手端show节点）
            
            if(click.driver_id is not null,1,0) as is_td_accpet_click,
            --当天是否应答（骑手端click节点）
            
            if(ord.is_td_request>=1,1,0) as is_td_request,
            --当天是否接单（应答）或者直接关联订单表不为空也可以判断司机是否接单
            
            if(ord.is_td_finish>=1,1,0) as is_td_finish,
            -- 当天是否完单
            
            sum(push.push_times) as push_times,
            --司机被播单总次数（push节点）
            
            sum(push.succ_push_times) as succ_push_times,
            --成功被播单总次数（push节点）
            
            sum(show.driver_show_order_times) as driver_show_order_times,
            --成功被推送总次数（骑手端show节点）
            
            sum(click.driver_click_order_times) as driver_click_order_times,
            -- 应答总次数（骑手端click节点）
            
            sum(push.push_order_cnt) as push_order_cnt, 
            --司机被播单订单量（push节点）
            
            sum(push.succ_push_order_cnt) as succ_push_order_cnt, 
            --成功被播单订单量（push节点）
            
            sum(show.driver_show_order_cnt) as driver_show_order_cnt, 
            --成功被推送订单量（骑手端show节点）
            
            sum(click.driver_click_order_cnt) as driver_click_order_cnt, 
            --成功应答订单量（骑手端show节点）
            
            sum(ord.is_td_request) as driver_request_order_cnt,
            --司机接单量（理论和应答量一样）
            
            sum(ord.is_td_finish) as driver_finish_order_cnt,
            --司机当天完单量
            
            sum(ord.is_td_finish_pay) as driver_finished_pay_order_cnt,
            --司机支付完单量
            
            sum(ord.price) as driver_finish_price, 
            --司机完单gmv
            
            sum(ord.td_finish_billing_dur) as driver_billing_dur,
            --司机计费时长
            
            sum(ord.td_service_dur) as driver_service_dur,
            --司机服务时长
            
            sum(ord.td_finish_order_dur) as driver_finished_dur,
            --司机支付完单做单时长（支付跨天可能偏大）
            
            sum(ord.td_cannel_pick_dur) as cannel_pick_dur,
            --司机当天订单被取消时长,该取消时长包含应答后司机、乘客等各方取消，用于计算司机在线时长
            
            sum(dtr.driver_freerange) as driver_free_dur,
            --司机空闲时长
            
            sum(if(ord.is_td_finish>=1,(nvl(ord.td_service_dur,0)+nvl(ord.td_cannel_pick_dur,0)+nvl(dtr.driver_freerange,0)),0)) as driver_finish_online_dur,
            --完单司机在线时长
            
            sum(if(ord.is_td_finish>=1 and is_strong_dispatch>=1,(nvl(ord.td_service_dur,0)+nvl(ord.td_cannel_pick_dur,0)+nvl(dtr.driver_freerange,0)),0)) as strong_driver_finish_online_dur,
            --强派单完单司机在线时长
            dri.country_code as country_code,
            dri.dt as dt
            
       FROM
            (
                SELECT 
                *
                FROM oride_dw.dim_oride_driver_base
                WHERE dt='{pt}'
            ) dri
            LEFT OUTER JOIN
            (
                SELECT *
                FROM oride_dw_ods.ods_log_oride_driver_timerange
                WHERE dt='{pt}'
            ) dtr ON dri.driver_id=dtr.driver_id
            AND dri.dt=dtr.dt
            LEFT OUTER JOIN
            (
                SELECT driver_id, --成功播单司机
                count(distinct order_id) as push_order_cnt, --司机被播单订单量（push节点）
                count(1) as push_times,  --司机被播单总次数（push节点）
                count(distinct (if(success=1,order_id,null))) as succ_push_order_cnt, --成功被播单订单量（push节点）
                sum(if(success=1,1,0)) as succ_push_times,  --成功被播单总次数（push节点）
                sum(success) as success  --用于判断该司机是否被成功播单
                FROM oride_dw.dwd_oride_order_push_driver_detail_di
                WHERE dt='{pt}'
                GROUP BY driver_id
            ) push ON dri.driver_id=push.driver_id
            LEFT OUTER JOIN
            (
                SELECT 
                driver_id,
                count(distinct order_id) driver_show_order_cnt, --成功被推送订单量（骑手端show节点）
                count(1) as driver_show_order_times   --成功被推送总次数（骑手端show节点）
                FROM 
                oride_dw.dwd_oride_driver_accept_order_show_detail_di
                WHERE dt='{pt}'
                GROUP BY driver_id
            ) show on dri.driver_id=show.driver_id            
            LEFT OUTER JOIN
            (
                SELECT 
                driver_id,
                count(distinct order_id) driver_click_order_cnt, --成功应答订单量（骑手端show节点）
                count(1) as driver_click_order_times  -- 应答总次数（骑手端click节点）
                FROM 
                oride_dw.dwd_oride_driver_accept_order_click_detail_di
                WHERE dt='{pt}'
                GROUP BY driver_id
            ) click on dri.driver_id=click.driver_id           
            LEFT OUTER JOIN
            (
                SELECT 
                driver_id,
                sum(is_td_request) as is_td_request, --用于判断司机当天是否接单,该字段为司机当天接单量
                sum(is_td_finish) as is_td_finish,  --用于判断司机是否有完单，该字段为司机当天完单量  
                sum(is_td_finish_pay) as is_td_finish_pay,  --用于判断司机是否有支付完单，该字段为司机当天支付完单量
                sum(if(is_td_finish = 1, price, 0)) as price, --司机完单gmv
                sum(td_finish_billing_dur) as td_finish_billing_dur, --司机计费时长，和司机完单计费时长不一样
                sum(td_service_dur) as td_service_dur, --司机服务时长
                sum(td_finish_order_dur) as td_finish_order_dur, --司机支付完单做单时长（支付跨天可能偏大）
                sum(td_cannel_pick_dur) as td_cannel_pick_dur, --司机当天订单被取消时长
                sum(is_strong_dispatch) as is_strong_dispatch, --用于判断司机是否有强派单
                          
                count(order_id) as succ_push_order_cnt  --该字段可以用于对比数据
                
                FROM oride_dw.dwd_oride_order_base_include_test_di
                WHERE dt='{pt}'
                AND city_id<>'999001' --去除测试数据
                and driver_id<>1
                group by driver_id
            ) ord ON dri.driver_id=ord.driver_id     
           group by dri.driver_id, 
            --司机ID
            
            dri.city_id,
            --城市ID
            
            dri.product_id,
            --司机绑定的业务类型
            
            dri.register_time,
            
            if(substr(dri.register_time,1,10)='{pt}',1,0),
            --是否当天签约or注册
            
            dri.is_bind,
            --是否绑定车辆
            
           -- dri.block,
            --是否存活司机0:存活
            
            if(dtr.driver_id is not null,1,0),
            --当天是否在线
            
            if(push.driver_id is not null,1,0),
            --当天是否被播单（push节点）
            
            if(push.success>=1,1,0),
            --当天是否被成功播单（push节点）
            
            if(show.driver_id is not null,1,0),
            --当天是否被推送订单（骑手端show节点）
            
            if(click.driver_id is not null,1,0),
            --当天是否应答（骑手端click节点）
            
            if(ord.is_td_request>=1,1,0),
            --当天是否接单（应答）或者直接关联订单表不为空也可以判断司机是否接单
            
            if(ord.is_td_finish>=1,1,0),
            dri.country_code,
            dri.dt;  
'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
    ),
    dag=dag)


# 生成_SUCCESS
def check_success(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"table": "{dag_name}".format(dag_name=dag_ids),
         "hdfs_path": "{hdfsPath}/country_code=nal/dt={pt}".format(pt=ds, hdfsPath=hdfs_path)}
    ]

    TaskTouchzSuccess().set_touchz_success(msg)


touchz_data_success = PythonOperator(
    task_id='touchz_data_success',
    python_callable=check_success,
    provide_context=True,
    dag=dag
)

dim_oride_driver_base_prev_day_task >> dwd_oride_order_base_include_test_di_prev_day_task >> \
dwd_oride_order_push_driver_detail_di_prev_day_task >> oride_driver_timerange_prev_day_task >> \
dwd_oride_driver_accept_order_show_detail_di_prev_day_task >>\
dwd_oride_driver_accept_order_click_detail_di_prev_day_task >> \
sleep_time >> dwm_oride_driver_base_d_task >> touchz_data_success