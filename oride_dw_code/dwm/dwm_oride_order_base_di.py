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

dag = airflow.DAG('dwm_oride_order_base_di',
                  schedule_interval="30 01 * * *",
                  default_args=args,
                  catchup=False)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##


# 依赖前一天分区
dependence_dwd_oride_order_base_include_test_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_order_base_include_test_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dependence_dwd_oride_order_dispatch_funnel_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_order_dispatch_funnel_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_dispatch_funnel_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
dependence_dwd_oride_driver_accept_order_show_detail_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_driver_accept_order_show_detail_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_driver_accept_order_show_detail_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dependence_dwd_oride_driver_accept_order_click_detail_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_driver_accept_order_click_detail_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_driver_accept_order_click_detail_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
dependence_dwd_oride_order_mark_df_prev_day_task = UFileSensor(
    task_id='dwd_oride_order_mark_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_mark_df/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
##----------------------------------------- 变量 ---------------------------------------##

table_name = "dwm_oride_order_base_di"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##


dwm_oride_order_base_di_task = HiveOperator(
    task_id='dwm_oride_order_base_di_task',
    hql='''

    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    insert overwrite table  oride_dw.{table} partition(country_code,dt)
    select  ord.order_id,
          ord.city_id,
           --所属城市
    
           ord.product_id as product_id,
           --订单下单业务类型(0: 专快混合 1:driect[专车] 2: street[快车] 99:招手停)
           
           ord.driver_serv_type,
           --接单后业务类型，和司机绑定
           
           from_unixtime(ord.create_time,'yyyy-MM-dd HH:mm:ss') as create_time,
           --下单时间
           
           null as is_peak,
           --是否高峰
           
           ord.driver_id,
           --司机ID
           
           ord.passenger_id,
           --乘客id
           
           ord.is_td_sys_cancel as is_sys_cancel,
           --是否系统取消
           
           ord.is_td_after_cancel as is_after_cancel,
           --是否应答后取消
           
           ord.is_td_passanger_before_cancel as is_passanger_before_cancel,
           --是否应答前乘客取消
           
           ord.is_td_passanger_after_cancel as is_passanger_after_cancel,
           --是否应答后乘客取消
           
           ord.is_td_driver_after_cancel as is_driver_after_cancel,
           --是否应答后司机取消
           if(push.success>=1,1,0) as is_succ_broadcast,    
           --是否成功播单（push节点）dm层成功播单量没有限定success=1
           if(show.order_id is not null,1,0) as is_accpet_show,
           --是否推送给骑手（骑手端打点show节点）
           if(click.order_id is not null,1,0) as is_accpet_click,
           --是否应答（骑手端打点click节点）
           ord.is_td_request as is_request,
           --是否接单（应答）
           ord.is_td_finish as is_finish,
           --是否完单
           ord.is_td_finish_pay as is_finished_pay,
           --是否完成支付
           ord.td_take_dur as take_order_dur,
           --应单订单时长
           ord.td_pick_up_dur as pick_up_order_dur,
           -- 当天接驾订单时长
           ord.td_cannel_pick_dur as cannel_pick_order_dur,
           --当天取消接驾订单时长
           ord.td_wait_dur as wait_order_dur,
           --当天等待上车订单时长
           ord.td_billing_dur as billing_order_dur,
           --当天计费订单时长
           ord.td_pay_dur as pay_order_dur,
           --当天支付订单时长（该字段有可能跨天支付导致时长偏大）
           ord.td_finish_order_dur as finished_order_dur,
           --当天支付完单做单时长（该字段有可能跨天支付导致时长偏大）
           assign.pick_up_distance as pick_up_distance,
           --接驾总距离（assign节点）
           assign.order_assigned_cnt as order_assigned_cnt,
           --订单被分配次数（assign节点）
           push.succ_broadcast_distance as succ_broadcast_distance,
           --成功播单总距离（push节点）
           push.succ_push_all_times_cnt as succ_push_all_times,
           --成功播单总次数（push节点）
           show.driver_show_times as driver_show_times_cnt,
           --骑手端推送给司机总次数（骑手端show节点）
           click.driver_click_times as driver_click_times_cnt,
           --司机应答总次数（骑手端click节点）
           ord.distance as order_onride_distance,
           --送驾距离
           ord.price as price,
           --gmv
           ord.pay_amount as pay_amount,
           --实际支付金额
           mark_ord.is_valid as is_valid,
           --是否有效订单
           if(ord.pay_mode=2,1,0) as is_opay_pay,
           --是否opay支付
           if(ord.pay_status=1,1,0) as is_succ_pay,
           --是否成功支付。全局运营中支付失败是限定pay_status in(0,2)
           mark_ord.is_wet_order as is_wet_order,
           --是否湿单
           mark_ord.score as score,
           --订单评分
           ord.pax_num as pax_num,
           --乘客数
           ord.is_carpool,
           --是否拼车
           ord.is_chartered_bus,
           --是否包车
           ord.is_carpool_success,
           --是否拼车成功
           ord.is_strong_dispatch,
           --是否强派1：是，0:否
           if(ord.arrive_time>0,(ord.arrive_time-ord.create_time),0) as user_order_total_dur,
           --乘客下单到行程结束总时长
           
           if(push.order_id is not null,1,0) as is_broadcast,
           --是否播单，这个播单包含播了但是没有成功的
           
           push.broadcast_distance, 
           --播单总距离
           
           push.push_all_times_cnt, 
           --播单总次数
 		   ord.country_code as country_code,
           
           ord.dt as dt
    FROM
      (
         SELECT *
         FROM oride_dw.dwd_oride_order_base_include_test_di
         WHERE dt = '{pt}'
         AND city_id<>'999001' --去除测试数据
         and driver_id<>1
       ) ord
    LEFT OUTER JOIN
      (
        SELECT  
        order_id,
        count(1) as order_assigned_cnt, --订单被分配次数（计算平均接驾距离使用）
        sum(distance) AS pick_up_distance --接驾总距离
        FROM oride_dw.dwd_oride_order_dispatch_funnel_di
        WHERE dt='{pt}' and event_name='dispatch_assign_driver'
        GROUP BY order_id
       ) assign ON ord.order_id=assign.order_id
    LEFT OUTER JOIN
      (
        SELECT 
        order_id,  --成功播单的订单
        sum(if(success=1,distance,0)) AS succ_broadcast_distance, --成功播单距离
        sum(if(success=1,1,0)) AS succ_push_all_times_cnt, --成功播单总次数
        sum(distance) as broadcast_distance, --播单总距离
        count(1) as push_all_times_cnt, --播单总次数
        sum(success) as success  --用于判断是否成功播单
        FROM oride_dw.dwd_oride_order_dispatch_funnel_di
        WHERE dt='{pt}' and event_name='dispatch_push_driver' 
        GROUP BY order_id
        ) push ON ord.order_id=push.order_id
    LEFT OUTER JOIN 
    (
        SELECT 
        order_id,
        count(1) as driver_show_times   --骑手段推送司机总次数
        FROM 
        oride_dw.dwd_oride_driver_accept_order_show_detail_di  --骑手show埋点
        WHERE dt='{pt}'
        GROUP BY order_id
    )  show on ord.order_id = show.order_id
    LEFT OUTER JOIN 
    (
        SELECT 
        order_id,
        count(1) driver_click_times  --司机应答次数,司机点接单次数
        FROM 
        oride_dw.dwd_oride_driver_accept_order_click_detail_di
        WHERE dt='{pt}'
        GROUP BY order_id
    ) click on ord.order_id = click.order_id
    left outer join 
    (
        select * from oride_dw.dwd_oride_order_mark_df 
        where dt='{pt}' and substr(create_time,1,10)='{pt}'
    )  mark_ord on ord.order_id=mark_ord.order_id
    ;
'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
    ),
    schema='oride_dw',
    dag=dag)

# 生成_SUCCESS
touchz_data_success = BashOperator(

    task_id='touchz_data_success',

    bash_command="""
    line_num=`$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk '{{print $1}}'`

    if [ $line_num -eq 0 ]
    then
        echo "FATAL {hdfs_data_dir} is empty"
        exit 1
    else
        echo "DATA EXPORT Successed ......"
        $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
    fi
    """.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        hdfs_data_dir=hdfs_path + '/country_code=nal/dt={{ds}}'
    ),
    dag=dag)

dependence_dwd_oride_order_base_include_test_di_prev_day_task >> \
dependence_dwd_oride_order_dispatch_funnel_di_prev_day_task >> \
dependence_dwd_oride_driver_accept_order_show_detail_di_prev_day_task >> \
dependence_dwd_oride_driver_accept_order_click_detail_di_prev_day_task >> \
dependence_dwd_oride_order_mark_df_prev_day_task >> \
sleep_time >> dwm_oride_order_base_di_task >> touchz_data_success
