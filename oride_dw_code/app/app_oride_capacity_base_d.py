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
    'owner': 'nan.li',
    'start_date': datetime(2019, 9, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_capacity_base_d',
                  schedule_interval="00 01 * * *",
                  default_args=args,
                  catchup=False)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##


# 依赖前一天分区
dependence_dm_oride_order_base_d_prev_day_task = UFileSensor(
    task_id='dm_oride_order_base_d_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dm_oride_order_base_d/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dependence_dm_oride_driver_base_cube_d_prev_day_task = UFileSensor(
    task_id='dm_oride_driver_base_cube_d_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dm_oride_driver_base_cube_d/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dependence_dm_oride_driver_base_d_prev_day_task = UFileSensor(
    task_id='dm_oride_driver_base_d_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dm_oride_driver_base_d/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


##----------------------------------------- 变量 ---------------------------------------##

table_name = "app_oride_capacity_base_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##


app_oride_capacity_base_d_task = HiveOperator(
    task_id='app_oride_capacity_base_d_task',
    hql='''

    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    
    insert overwrite TABLE oride_dw.{table} partition(country_code,dt)
    select 
    
    nvl(ord.city_id,-10000) as city_id,
    nvl(ord.product_id,-10000) as product_id,
    
    nvl(round((succ_broadcast_dis)/(succ_push_all_times_cnt),1),0) as broadcast_dis_avg,
    --平均播单距离
    
    nvl(round((finish_order_pick_up_dis)/(finish_order_pick_up_assigned_cnt),1),0) as finish_order_pick_up_dis_avg,
    --平均接驾距离
    
    0 as request_order_pick_up_dis_avg,
    --缺少 应答单接驾距离
    
    
    --nvl(round((finish_order_pick_up_dis)/(ride_order_cnt),1),0) as request_order_pick_up_dis_avg,
    --平均接驾距离（应答单）
    
    0 as obey_rate,
    --调度服从率 司机平均应答次数/人均推单次数
    
    (request_order_cnt) as request_order_cnt,
    --应答订单数
    
    nvl(round((request_order_cnt) * 100 / (ride_order_cnt),1),0) as request_order_rate,
    --应答率
    
    round((finish_take_order_dur/60)/(finish_order_cnt),1) as finish_order_request_time_avg,
    --平均应答时长（分）
    
    round((take_order_dur/60)/(request_order_cnt),1) as  request_order_request_time_avg,
    --平均应答时长（应答单）（分）
    
    nvl(round((passanger_after_cancel_order_cnt) * 100 /(ride_order_cnt),1),0) as passanger_after_cancel_order_rate,
    --乘客应答后取消率
    
    nvl(round((driver_after_cancel_order_cnt) * 100 /(ride_order_cnt),1),0) as driver_after_cancel_order_rate,
    --司机应答后取消率
    
    nvl(round((finish_order_cnt) / (finish_driver_online_dur),1),0) as TPH,
    --TPH
    
    nvl(round((finish_driver_online_dur/60) / (finish_order_driver_num),1),0) as online_time_avg,
    --人均在线时长（分）
    
    nvl(round((driver_billing_dur) * 100/(finish_driver_online_dur),1),0) as billing_time_rate,
    --计费时长占比
    
    (ride_order_cnt) as ride_order_cnt,
    --下单数
    
    0 as legal_ride_order_cnt, 
    --有效下单数
    
    (succ_broadcast_cnt) as succ_broadcast_cnt,
    --成功播单数
    
    round((succ_broadcast_cnt) * 100/ (ride_order_cnt),1) as succ_broadcast_rate,
    --播单率
    
    round((request_order_cnt) * 100/ (succ_broadcast_cnt),1) as capacity_request_order_rate,
    --调度接单率
    
    (finish_order_cnt) as finish_order_cnt,
    --完单数
    
    round((finish_order_cnt) * 100 / (ride_order_cnt),1) as finish_order_rate,
    --完单率
    
    0 as legal_finish_order_rate,
    --有效完单率
    
    round((finish_order_cnt) * 100 / (broadcast_cnt),1) as capacity_finish_order_rate,
    --调度完单率
    
    0  as take_time_avg,
    --平均接驾时长（分）
    
    round((finish_order_onride_dis)/(finish_order_cnt),1) as finish_order_onride_dis_avg,
    --平均送驾距离
    
    round((finish_billing_dur/60)/(finish_order_cnt),1) as finish_order_billing_time_avg,
    --平均计费时长（分）
    
    round((pay_order_dur/60) / (finish_pay),1) as pay_order_paytime_avg,
    --平均支付时长（分）
    
    (online_driver_num) as online_driver_num,
    --在线司机数
    
    (request_driver_num) as request_driver_num,
    --接单司机数
    
    (finish_order_driver_num) as finish_order_driver_num,
    --完单司机数
    
    round((finish_order_cnt)/(finish_order_driver_num),1) as finish_order_driver_order_avg,
    --人均完单数
    
    round((odb.driver_pushed_order_cnt) / (odc.push_accpet_show_driver_num),1) as  push_driver_order_avg,
    --人均推送订单数
    
    round((odb.driver_click_order_cnt) / (driver_accept_take_num),1) as driver_click_order_avg,
    --人均应答订单数
    
    round(((odb.driver_click_order_cnt) / (odc.push_accpet_show_driver_num)) * 100 / ((odb.driver_pushed_order_cnt) / (driver_accept_take_num)),1) as driver_obey_rate,
    --司机服从率 司机服从率=人均应答订单数/人均推送订单数
    
    round((service_dur/60) / (finish_order_cnt),1) as service_dur_avg,
    --平均服务时长（分）
    
    0 as finish_order_driver_IPH,
    --完单司机IPH
    
    0 as finish_order_driver_day_salary,
    --完单司机日薪
    
    0 as chose_broadcast_cnt,
    --播单数
    
    
    round((succ_push_all_times_cnt) * 100 /(push_driver_times_cnt),1) as broadcast_success_rrate,
    --播报到达率
    
    round((succ_push_all_times_cnt)/(driver_take_num),1) as succ_push_driver_avg,
    --人均推单次数
    
    0 as push_driver_time_avg,
    --平均推送司机数(chose阶段数据，无法介入漏斗模型)
    
    (driver_accpet_order_cnt) as driver_accpet_order_cnt,
    --司机总应答次数
    
    round((driver_accpet_order_cnt)/ (driver_accept_take_num),1) as driver_accpet_order_avg,
    -- 司机平均应答次数
    
    (sys_cancel_order_cnt + passanger_before_cancel_order_cnt + passanger_after_cancel_order_cnt + driver_after_cancel_order_cnt) as cancel_order_cnt,
    --取消订单数
    
    (sys_cancel_order_cnt) as sys_cancel_order_cnt,
    --系统取消订单数
    
    round((sys_cancel_order_cnt) * 100/(ride_order_cnt),1) as sys_cancel_order_rate,
    --系统取消率
    
    (passanger_before_cancel_order_cnt) as passanger_before_cancel_order_cnt,
    --乘客应答前取消数
    
    round((passanger_before_cancel_order_cnt) * 100/(ride_order_cnt),1) as passanger_before_cancel_order_rate,
    --乘客应答前取消率
    
    0 as before_cancel_order_cnt,
    --应答后取消数
    
    (passanger_after_cancel_order_cnt) as passanger_after_cancel_order_cnt,
    --乘客应答后取消数
    
    0 as passanger_after_cancel_order_time_avg,
    --乘客应答后取消平均时长（分）
    
    0 as passanger_after_cancel_order_dis_avg,
    --乘客取消订单平均接驾距离
    
    (driver_after_cancel_order_cnt) as driver_after_cancel_order_cnt,
    --司机应答后取消数
    
    0 as driver_after_cancel_order_time_avg,
    --司机应答后取消平均时长（分）
    
    nvl(ord.country_code,-10000) as country_code,
    
    '{pt}'  as dt
    
    from 
    (
        select 
        nvl(country_code,-10000) as country_code,
        nvl(city_id,-10000) as city_id,
        nvl(product_id,-10000) as product_id,
        sum(succ_broadcast_dis) as succ_broadcast_dis,
        sum(succ_push_all_times_cnt) as succ_push_all_times_cnt,
        sum(finish_order_pick_up_dis) as finish_order_pick_up_dis,
        sum(finish_order_cnt) as finish_order_cnt,
        sum(request_order_cnt) as request_order_cnt,
        sum(ride_order_cnt) as ride_order_cnt,
        sum(finish_take_order_dur) as finish_take_order_dur,
        sum(take_order_dur) as take_order_dur,
        sum(passanger_after_cancel_order_cnt) as passanger_after_cancel_order_cnt,
        sum(driver_after_cancel_order_cnt) as driver_after_cancel_order_cnt,
        sum(succ_broadcast_cnt) as succ_broadcast_cnt,
        sum(broadcast_cnt) as broadcast_cnt,
        sum(finish_billing_dur) as finish_billing_dur,
        sum(pay_order_dur) as pay_order_dur,
        sum(finish_pay) as finish_pay,
        sum(push_driver_times_cnt) as push_driver_times_cnt,
        sum(driver_accpet_order_cnt) as driver_accpet_order_cnt,
        sum(sys_cancel_order_cnt) as sys_cancel_order_cnt,
        sum(service_dur) as service_dur,
        sum(passanger_before_cancel_order_cnt) as passanger_before_cancel_order_cnt,
        sum(finish_order_onride_dis) as finish_order_onride_dis,
        sum(finish_order_pick_up_assigned_cnt) as finish_order_pick_up_assigned_cnt
        
        from 
        oride_dw.dm_oride_order_base_d
        where 
        dt = '{pt}'
        group by country_code,city_id,product_id
        with cube
    ) ord 
    left join 
    (
        select 
        country_code,
        city_id,
        product_id,
        online_driver_num,
        driver_accept_take_num,
        driver_take_num,
        request_driver_num,
        finish_order_driver_num,
        push_accpet_show_driver_num
    
        from oride_dw.dm_oride_driver_base_cube_d
        where dt = '{pt}'
    )  odc on ord.country_code = odc.country_code  and ord.city_id = odc.city_id and ord.product_id = odc.product_id
    left join 
    (
        select 
        nvl(country_code,-10000) as country_code,
        nvl(city_id,-10000) as city_id,
        nvl(product_id,-10000) as product_id,
        sum(driver_finish_order_dur) as driver_finish_order_dur,
        sum(driver_cannel_pick_dur) as driver_cannel_pick_dur,
        sum(driver_free_dur) as  driver_free_dur,
        sum(succ_push_order_cnt) as succ_push_order_cnt,
        sum(finish_driver_online_dur) as finish_driver_online_dur,
        sum(driver_click_order_cnt) as driver_click_order_cnt,
        sum(driver_pushed_order_cnt) as driver_pushed_order_cnt,
        sum(driver_billing_dur) as driver_billing_dur
        from oride_dw.dm_oride_driver_base_d
        where dt = '{pt}'
        group by country_code,city_id,product_id
        with cube
    )  odb on ord.country_code = odb.country_code  and ord.city_id = odb.city_id and ord.product_id = odb.product_id
    ;

'''.format(
        pt='{{ds}}',
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

dependence_dm_oride_order_base_d_prev_day_task >> \
dependence_dm_oride_driver_base_cube_d_prev_day_task >>\
dependence_dm_oride_driver_base_d_prev_day_task >> \
sleep_time >> app_oride_capacity_base_d_task >> touchz_data_success
