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

# 依赖前一天分区
dependence_dwd_oride_order_mark_df_prev_day_task = HivePartitionSensor(
    task_id="dwd_oride_order_mark_df_prev_day_task",
    table="dwd_oride_order_mark_df",
    partition="dt='{{ds}}'",
    schema="oride_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


##----------------------------------------- 变量 ---------------------------------------##

table_name = "dm_oride_order_base_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------## 


dm_oride_order_base_d_task = HiveOperator(
    task_id='dm_oride_order_base_d_task',
    hql='''

    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    
    insert overwrite table  oride_dw.{table} partition(country_code,dt)
    select 
          ord.city_id,
           --所属城市
    
           ord.product_id as product_id,
           --订单车辆类型(0: 专快混合 1:driect[专车] 2: street[快车] 99:招手停)
    
           count(ord.order_id) AS ride_order_cnt,
           --当天下单数
    
           sum(is_td_request) AS request_order_cnt,
           --当天接单数
    
           sum(is_td_finish) AS finish_order_cnt,
           --当天完单数
    
           sum(td_pick_up_dur) AS pick_up_order_dur,
           --当天接驾订单时长（分钟）
    
           sum(td_take_dur) AS take_order_dur,
           --当天应答订单时长（分钟）
    
           sum(td_cannel_pick_dur) AS cannel_pick_order_dur,
           --当天取消接驾订单时长（分钟）
    
           sum(td_wait_dur) AS wait_order_dur,
           --当天等待上车订单时长（分钟）
    
           sum(td_billing_dur) AS billing_order_dur,
           --当天计费订单时长（分钟）
    
           sum(td_pay_dur) AS pay_order_dur,
           --当天支付订单时长(分钟)
    
           sum(is_td_sys_cancel) AS sys_cancel_order_cnt,
           --当天系统取消订单数
    
           sum(is_td_passanger_before_cancel) AS passanger_before_cancel_order_cnt,
           --当天乘客应答前取消订单数
    
           sum(is_td_passanger_after_cancel) AS passanger_after_cancel_order_cnt, --当天乘客应答后取消订单数
    
           sum(is_td_finish_pay) as finish_pay, --当天完成支付
    
           sum(nvl(succ_broadcast_distance,0)) as succ_broadcast_dis, --当天成功播单距离(米)
    
           sum(is_td_driver_after_cancel) as driver_after_cancel_order_cnt,--当天司机应答后取消订单数
    
           sum(td_finish_billing_dur) as finish_billing_dur,--当天完单计费时长（分钟）
          
           sum(td_finish_order_dur) as finish_order_dur,--当天完单做单时长(分钟）
    
           sum(case when ord.order_id=a1.order_id and is_td_finish=1 then pick_up_distance else 0 end) as finish_order_pick_up_dis, --完单接驾距离(米)
    
           count(distinct p1.order_id) as succ_broadcast_cnt,--成功播单数
    
           sum(case when ord.order_id = d1.order_id then 1 else 0 end) as broadcast_cnt, --播单数
           
           count(distinct r1.order_id) as driver_accpet_order_cnt,  --司机应答订单数
    
           sum (case when ord.order_id=p1.order_id then 1 else 0 end) as dispatch_push_driver_order_cnt, --推送给骑手的订单数(push阶段)
           
           sum (case when ord.order_id=p1.order_id then p1.push_driver_times_cnt else 0 end) as push_driver_times_cnt, --推送成功给司机的总次数(push阶段)
            
           sum (case when ord.order_id=p1.order_id then p1.succ_push_all_times_cnt else 0 end) as succ_push_all_times_cnt, --推送成功的总次数(push阶段)
            
           sum(td_finish_order_dur)+sum(td_cannel_pick_dur) as accept_order_dur,
           --当天做单时长(当天完成做单时长+当天取消接驾时长（分钟）)
           
           count(distinct r2.order_id) as push_driver_order_accpet_show_cnt,  --司机被推送订单数(accpet_show阶段)
           
           sum(if(ord.is_td_finish = 1, td_service_dur,0)) service_dur, --当天完单服务时长
           
           sum (case when ord.order_id = r1.order_id then r1.driver_click_times else 0 end) as driver_click_times_cnt, --司机应答的总次数(accpet_click阶段)
           
           sum(if(ord.is_td_finish = 1, ord.td_take_dur,0)) as finish_take_order_dur, --当天完成订单应答时长
           
           sum(if(ord.is_td_finish = 1, ord.distance,0)) as finish_order_onride_dis, --完单送驾距离(米)
           
           sum(case when ord.order_id=a1.order_id and is_td_finish=1 then order_assigned_cnt else 0 end) as finish_order_pick_up_assigned_cnt, --订单被分配次数（计算平均接驾距离使用）
           
           sum(if(ord.status in(4,5),ord.price,0.0)) as price,  --当日完单gmv
           sum(if(ord.status=5,ord.price,0.0)) as pay_price, --当日应付金额
           sum(if(ord.status=5,ord.pay_amount,0.0)) as pay_amount, --当日实付金额
           count(if(mark_ord.is_valid=1,ord.order_id,null)) as valid_ord_cnt, --有效订单数
           sum(if(ord.is_td_finish = 1,ord.td_pick_up_dur,0)) as finish_pick_up_dur, --当日完单接驾时长
           sum(if(ord.is_td_finish = 1,ord.pax_num,0)) as pax_num,  --当日完单乘客数
           count(if(ord.pay_mode=2,ord.order_id,null)) as opay_pay_cnt, --opay支付订单数
           count(if(ord.pay_mode=2 and ord.pay_status in(0,2),ord.order_id,null)) as opay_pay_failed_cnt, --opay支付失败订单数
           count(if(mark_ord.is_wet_order=1,ord.order_id,null)) as wet_ord_cnt, --湿单订单量
           count(if(mark_ord.score in(1,2) and ord.is_td_finish=1,ord.order_id,null)) as bad_feedback_finish_ord_cnt, --差评完单量
           
           count(if(is_td_after_cancel = 1 ,ord.order_id,null)) as after_cancel_order_cnt,--应答后取消订单数
           sum(td_passanger_after_cancel_time_dur) AS passanger_after_cancel_time_dur,--乘客应答后取消时长（秒）
           sum(td_driver_after_cancel_time_dur) AS driver_after_cancel_time_dur,--司机应答后取消时长（秒）
           sum(if((ord.status = 6 and cancel_role =2),ord.distance,0.0)) as  passanger_cancel_order_dis,   --乘客取消订单接驾距离
           sum(a1.pick_up_distance) as accept_order_pick_up_dis, --应答单接驾距离(米)（计算平均接驾距离（应答单使用））
           sum(r1.accept_order_assigned_cnt) as  accept_order_pick_up_assigned_cnt, --应答单分配次数（应答单接驾距离(米)（计算平均接驾距离（应答单使用）））
           ord.driver_serv_type,  --业务类型，订单表中司机类型

           ord.country_code,
           
           ord.dt
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
        sum(if (success=1, distance,0)) AS succ_broadcast_distance, --成功播单距离
        count(concat(order_id,'_',order_round)) AS push_driver_times_cnt, -- 推送成功给司机的总次数
        count(if (success=1,1,null)) AS succ_push_all_times_cnt --推送成功的总次数
        FROM oride_dw.dwd_oride_order_dispatch_funnel_di
        WHERE dt='{pt}' and event_name='dispatch_push_driver' 
        GROUP BY order_id
        ) p1 ON ord.order_id=p1.order_id
    LEFT OUTER JOIN
      (
        SELECT  
        order_id,
        count(1) as order_assigned_cnt, --订单被分配次数（计算平均接驾距离使用）
        sum(distance) AS pick_up_distance --接驾总距离
        FROM oride_dw.dwd_oride_order_dispatch_funnel_di
        WHERE dt='{pt}' and event_name='dispatch_assign_driver'
        GROUP BY order_id
       ) a1 ON ord.order_id=a1.order_id
    LEFT OUTER JOIN
      (
        SELECT order_id
        FROM oride_dw.dwd_oride_order_dispatch_funnel_di --播单
        WHERE dt='{pt}' and event_name='dispatch_chose_driver'
        GROUP BY order_id
      ) d1 ON ord.order_id=d1.order_id
    
    LEFT OUTER JOIN 
    (
        SELECT 
        order_id,
        count(distinct order_id) as accept_order_assigned_cnt, --应答订单被分配次数（计算平均接驾距离（应答单）使用）
        count(1) driver_click_times
        FROM 
        oride_dw.dwd_oride_driver_accept_order_click_detail_di
        WHERE dt='{pt}'
        GROUP BY order_id
    )  r1 on ord.order_id = r1.order_id
    LEFT OUTER JOIN 
    (
        SELECT 
        order_id
        FROM 
        oride_dw.dwd_oride_driver_accept_order_show_detail_di
        WHERE dt='{pt}'
        GROUP BY order_id
    )  r2 on ord.order_id = r2.order_id
    left outer join 
    (
        select * from oride_dw.dwd_oride_order_mark_df 
        where dt='{pt}' and substr(create_time,1,10)='{pt}'
    )  mark_ord on ord.order_id=mark_ord.order_id
    
    GROUP BY ord.city_id,
             ord.product_id,
             ord.country_code,
             ord.dt,
             ord.driver_serv_type  --业务类型，司机业务类型
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
dependence_dwd_oride_driver_accept_order_click_detail_di_prev_day_task >> \
dependence_dwd_oride_driver_accept_order_show_detail_di_prev_day_task >> \
dependence_dwd_oride_order_mark_df_prev_day_task >>\
sleep_time >> dm_oride_order_base_d_task >> touchz_data_success
