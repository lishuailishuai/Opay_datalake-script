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
    'owner': 'chenlili',
    'start_date': datetime(2019, 9, 9),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_global_operate_report_d',
                  schedule_interval="00 03 * * *",
                  default_args=args)

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

# 依赖前一天分区
dependence_dm_oride_passenger_base_cube_d_prev_day_task = UFileSensor(
    task_id='dm_oride_passenger_base_cube_d_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dm_oride_passenger_base_cube_d/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
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

# 依赖前一天分区
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

# 依赖当天分区00点
dependence_server_magic_now_day_task = HivePartitionSensor(
    task_id="server_magic_now_day_task",
    table="server_magic",
    partition="dt='{{macros.ds_add(ds, +1)}}' and hour='00'",
    schema="oride_source",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
dependence_dwd_oride_order_finance_df_prev_day_task = UFileSensor(
    task_id='dwd_oride_order_finance_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_finance_df/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
dependence_dwd_oride_passenger_recharge_df_prev_day_task = UFileSensor(
    task_id='dwd_oride_passenger_recharge_df',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_passenger_recharge_df/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
##----------------------------------------- 变量 ---------------------------------------##

table_name = "app_oride_global_operate_report_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##
order_data_null="""
       null as ride_order_cnt, --当日下单量
       null as finish_order_cnt, --当日完单量
       null as finish_pay, --当日完成支付量
       null as valid_ord_cnt,  --当日有效订单量
       null as finish_take_order_dur,  --当日完单应答时长
       null as finish_pick_up_dur,  --当日完单接驾时长
       null as finish_billing_dur,  --当日完单计费时长【跟计费时长有点差异】
       null as finish_order_onride_dis,  --当日完单送驾距离
       null as pax_num,  --乘客数
       null as price, --当日完单gmv,订单状态4，5
       null as pay_price,  --当日应付金额，订单状态5
       null as pay_amount,  --当日实付金额，订单状态5
       null as order_cnt_lfw, --近四周同期下单数据
       null as finish_order_cnt_lfw  --近四周同期完单数据
       """

passenger_data_null="""
       null as new_users,  --当天注册乘客数
       null as act_users,  --当天活跃乘客数
       null as ord_users,  --当日下单乘客数
       null as finished_users,  --当日完单乘客数
       null as first_finished_users,  --当日首次完单乘客数
       null as old_finished_users,  --当日完单老客数
       null as new_user_ord_cnt,  --当日新注册乘客下单量
       null as new_user_finished_cnt,  --当日新注册乘客完单量
       null as paid_users,  --当日总支付乘客数
       null as online_paid_users,  --当日线上支付乘客数
       null as new_user_gmv  --当日新注册乘客完单gmv
"""

driver_cube_data_null="""
       null as td_audit_finish_driver_num,  --当日审核通过司机数
       null as online_driver_num,  --当日在线司机数
       null as request_driver_num, --当日接单司机数
       null as finish_order_driver_num,  --当日完单司机数
       null as push_accpet_show_driver_num --被推送骑手数
"""

driver_data_null="""
       null as finish_driver_online_dur,  --当日完单司机在线时长
       null as driver_billing_dur, --当日司机计费时长
       null as driver_pushed_order_cnt  --司机被推送订单数
"""

finance_data_null="""
       null AS recharge_amount, --充值金额
       null AS reward_amount, --奖励金额
       null AS amount_pay_online, --当日总收入-线上支付金额
       null AS amount_pay_offline --当日总收入-线下支付金额 
"""
passenger_recharge_data_null="""
       null as recharge_users, --每天充值用户数
       null as user_recharge_succ_balance  --每天用户充值真实金额
"""

app_oride_global_operate_report_d_task = HiveOperator(

    task_id='app_oride_global_operate_report_d_task',
    hql='''
    SET hive.exec.parallel=true;
    SET hive.exec.dynamic.partition=true;
    SET hive.exec.dynamic.partition.mode=nonstrict;
    
    with order_data as
    (
--订单相关,只过滤当日的
select nvl(t.country_code,'-10000') as country_code,
       nvl(t.city_id,-10000) as city_id,
       nvl(t.product_id,-10000) as product_id,
       sum(ride_order_cnt) as ride_order_cnt, --当日下单量
       sum(finish_order_cnt) as finish_order_cnt, --当日完单量
       sum(finish_pay) as finish_pay, --当日完成支付量
       sum(valid_ord_cnt) as valid_ord_cnt,  --当日有效订单量
       sum(finish_take_order_dur) as finish_take_order_dur,  --当日完单应答时长
       sum(finish_pick_up_dur) as finish_pick_up_dur,  --当日完单接驾时长
       sum(finish_billing_dur) as finish_billing_dur,  --当日完单计费时长【跟计费时长有点差异】
       sum(finish_order_onride_dis) as finish_order_onride_dis,  --当日完单送驾距离
       sum(pax_num) as pax_num,  --乘客数
       sum(price) as price, --当日完单gmv,订单状态4，5
       sum(pay_price) as pay_price,  --当日应付金额，订单状态5
       sum(pay_amount) as pay_amount,  --当日实付金额，订单状态5
       sum(order_cnt_lfw) as order_cnt_lfw,  --近四周同期下单数据 
       sum(finish_order_cnt_lfw) as finish_order_cnt_lfw,  --近四周同期完单数据
       {passenger_data_null},
       {driver_cube_data_null},
       {driver_data_null},
       null as map_request_num,
       {finance_data_null},
       {passenger_recharge_data_null}  
from (SELECT dt,country_code,
			 city_id,
			 product_id,
       sum(ride_order_cnt) as ride_order_cnt, --当日下单量
       sum(finish_order_cnt) as finish_order_cnt, --当日完单量
       sum(finish_pay) as finish_pay, --当日完成支付量
       sum(valid_ord_cnt) as valid_ord_cnt,  --当日有效订单量
       sum(finish_take_order_dur) as finish_take_order_dur,  --当日完单应答时长
       sum(finish_pick_up_dur) as finish_pick_up_dur,  --当日完单接驾时长
       sum(finish_billing_dur) as finish_billing_dur,  --当日完单计费时长【跟计费时长有点差异】
       sum(finish_order_onride_dis) as finish_order_onride_dis,  --当日完单送驾距离
       sum(pax_num) as pax_num,  --乘客数
       sum(price) as price, --当日完单gmv,订单状态4，5
       sum(pay_price) as pay_price,  --当日应付金额，订单状态5
       sum(pay_amount) as pay_amount,  --当日实付金额，订单状态5
       sum(if(dt>=date_add('{pt}',-28) and dt<'{pt}' and
       from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')=from_unixtime(unix_timestamp('{pt}', 'yyyy-MM-dd'),'u'),ride_order_cnt,0))/4 as order_cnt_lfw, --近四周同期下单数据
       sum(if(dt>=date_add('{pt}',-28) and dt<'{pt}' and
       from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')=from_unixtime(unix_timestamp('{pt}', 'yyyy-MM-dd'),'u'),finish_order_cnt,0))/4 as finish_order_cnt_lfw  --近四周同期完单数据
       
FROM oride_dw.dm_oride_order_base_d
WHERE dt>=date_add('{pt}',-28) and dt<='{pt}'
group by dt,country_code,
			 city_id,
			 product_id) t
where t.dt='{pt}'
group by nvl(t.country_code,'-10000'),
       nvl(t.city_id,-10000),  --with cube时，默认值无效
       nvl(t.product_id,-10000)
with cube),

--乘客相关
passenger_data as 
(
select country_code,
       city_id,
       product_id,  
       {order_data_null},
       new_users,  --当天注册乘客数
       act_users,  --当天活跃乘客数
       ord_users,  --当日下单乘客数
       finished_users,  --当日完单乘客数
       first_finished_users,  --当日首次完单乘客数
       old_finished_users,  --当日完单老客数
       new_user_ord_cnt,  --当日新注册乘客下单量
       new_user_finished_cnt,  --当日新注册乘客完单量
       paid_users,  --当日总支付乘客数
       online_paid_users,  --当日线上支付乘客数
       new_user_gmv,  --当日新注册乘客完单gmv  
       {driver_cube_data_null},
       {driver_data_null},
       null as map_request_num,
       {finance_data_null},
       {passenger_recharge_data_null} 
from oride_dw.dm_oride_passenger_base_cube_d 
where dt='{pt}' and nvl(country_code,'-10000')<>'-10000'),     
        
--司机相关cube
driver_cube_data as
(
select country_code,
       cast(city_id as bigint) as city_id,
       product_id,
       {order_data_null},
       {passenger_data_null},
       td_audit_finish_driver_num,  --当日审核通过司机数
       online_driver_num,  --当日在线司机数
       request_driver_num, --当日接单司机数
       finish_order_driver_num,  --当日完单司机数
       push_accpet_show_driver_num, --被推送骑手数
       {driver_data_null},
       null as map_request_num,
       {finance_data_null},
       {passenger_recharge_data_null} 
from oride_dw.dm_oride_driver_base_cube_d   --已经去除了with cube产生的country_code为空的数据
where dt='{pt}'),

--司机相关
driver_data as
(
select nvl(country_code,'-10000') as country_code,
       nvl(cast(city_id as bigint),-10000) as city_id,
       nvl(product_id,-10000) as product_id,
       {order_data_null},
       {passenger_data_null},
       {driver_cube_data_null},
       sum(finish_driver_online_dur) as finish_driver_online_dur,  --当日完单司机在线时长
       sum(driver_billing_dur) as driver_billing_dur, --当日司机计费时长[！！！不准确]
       sum(driver_pushed_order_cnt) as driver_pushed_order_cnt,  --司机被推送订单数
       null as map_request_num,
       {finance_data_null},
       {passenger_recharge_data_null} 
from oride_dw.dm_oride_driver_base_d
where dt='{pt}'
group by nvl(country_code,'-10000'),
       nvl(cast(city_id as bigint),-10000),
       nvl(product_id,-10000)
with cube),
        
--地图调用相关
map_data as
(
SELECT 'nal' as country_code,
       -10000 as city_id,
       -10000 as product_id,
       {order_data_null},
       {passenger_data_null},
       {driver_cube_data_null},
       {driver_data_null},
       count(1) as map_request_num,  --地图调用次数
       {finance_data_null},
       {passenger_recharge_data_null} 
       FROM oride_source.server_magic
       WHERE dt='{pt}'
       and event_name in ('googlemap_directions', 'googlemap_nearbysearch', 'googlemap_autocomplete', 'googlemap_details', 'googlemap_geocode')),

--gmv相关  
finance_data as
(
select country_code,
       -10000 as city_id,
       -10000 as product_id,
       {order_data_null},
       {passenger_data_null},
       {driver_cube_data_null},
       {driver_data_null},
       null as map_request_num,  --地图调用次数
       sum(recharge_amount) AS recharge_amount, --充值金额
       sum(reward_amount) AS reward_amount, --奖励金额
       sum(amount_pay_online) AS amount_pay_online, --当日总收入-线上支付金额
       sum(amount_pay_offline) AS amount_pay_offline, --当日总收入-线下支付金额 
       {passenger_recharge_data_null} 
from oride_dw.dwd_oride_order_finance_df 
where dt='{pt}'
and create_date='{pt}'
group by country_code
),

--用户充值相关
passenger_recharge_data as
(
select country_code,
       -10000 as city_id,
       -10000 as product_id,
       {order_data_null},
       {passenger_data_null},
       {driver_cube_data_null},
       {driver_data_null},
       null as map_request_num,  --地图调用次数
       {finance_data_null},
       count(distinct user_id) as recharge_users, --每天充值用户数
       sum(user_recharge_succ_balance) as user_recharge_succ_balance  --每天用户充值真实金额
from oride_dw.dwd_oride_passenger_recharge_df
where dt='{pt}'
and create_date='{pt}'
group by country_code
)
       
INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)       
SELECT nvl(city_id,-10000) as city_id,
       nvl(product_id,-10000) as product_id,
       sum(ride_order_cnt) as ride_order_cnt, --当日下单量
       sum(finish_order_cnt) as finish_order_cnt, --当日完单量
       sum(finish_pay) as finish_pay, --当日完成支付量
       sum(valid_ord_cnt) as valid_ord_cnt,  --当日有效订单量
       sum(finish_take_order_dur) as finish_take_order_dur,  --当日完单应答时长
       sum(finish_pick_up_dur) as finish_pick_up_dur,  --当日完单接驾时长
       sum(finish_billing_dur) as finish_billing_dur,  --当日完单计费时长【跟计费时长有差异】
       sum(finish_order_onride_dis) as finish_order_onride_dis,  --当日完单送驾距离
       sum(pax_num) as pax_num,  --乘客数
       sum(price) as price, --当日完单gmv,订单状态4，5
       sum(pay_price) as pay_price,  --当日应付金额，订单状态5
       sum(pay_amount) as pay_amount,  --当日实付金额，订单状态5
       sum(order_cnt_lfw) as order_cnt_lfw,  --近四周同期下单数据 
       sum(finish_order_cnt_lfw) as finish_order_cnt_lfw,  --近四周同期完单数据
       sum(new_users) as new_users,  --当天注册乘客数
       sum(act_users) as act_users,  --当天活跃乘客数
       sum(ord_users) as ord_users,  --当日下单乘客数
       sum(finished_users) as finished_users,  --当日完单乘客数
       sum(first_finished_users) as first_finished_users,  --当日首次完单乘客数
       sum(old_finished_users) as old_finished_users,  --当日完单老客数
       sum(new_user_ord_cnt) as new_user_ord_cnt,  --当日新注册乘客下单量
       sum(new_user_finished_cnt) as new_user_finished_cnt,  --当日新注册乘客完单量
       sum(paid_users) as paid_users,  --当日总支付乘客数
       sum(online_paid_users) as online_paid_users,  --当日线上支付乘客数
       sum(new_user_gmv) as new_user_gmv,  --当日新注册乘客完单gmv 
       sum(td_audit_finish_driver_num) as td_audit_finish_driver_num,  --当日审核通过司机数
       sum(online_driver_num) as online_driver_num,  --当日在线司机数
       sum(request_driver_num) as request_driver_num, --当日接单司机数
       sum(finish_order_driver_num) as finish_order_driver_num,  --当日完单司机数
       sum(push_accpet_show_driver_num) as push_accpet_show_driver_num, --被推送骑手数 
       sum(finish_driver_online_dur) as finish_driver_online_dur,  --当日完单司机在线时长
       sum(driver_billing_dur) as driver_billing_dur, --当日司机计费时长
       sum(driver_pushed_order_cnt) as driver_pushed_order_cnt,  --司机被推送订单数
       sum(map_request_num) as map_request_num,  --地图调用次数
       sum(reward_amount) AS reward_amount, --奖励金额
       sum(amount_pay_online) AS amount_pay_online, --当日总收入-线上支付金额
       sum(amount_pay_offline) AS amount_pay_offline, --当日总收入-线下支付金额 
       sum(recharge_users) as recharge_users, --每天充值用户数
       sum(user_recharge_succ_balance) as user_recharge_succ_balance,  --每天用户充值真实金额
       nvl(country_code,'nal') as country_code,
       '{pt}' as dt
FROM (select * from order_data where nvl(country_code,'-10000')<>'-10000'
UNION ALL 
select * from passenger_data
UNION ALL 
select * from driver_cube_data 
UNION ALL 
select * from driver_data where nvl(country_code,'-10000')<>'-10000'
UNION ALL 
select * from map_data
union all 
select * from finance_data
union all 
select * from passenger_recharge_data) t
GROUP BY nvl(country_code,'nal'),
       nvl(city_id,-10000),
       nvl(product_id,-10000);
'''.format(
        order_data_null=order_data_null,
        passenger_data_null=passenger_data_null,
        driver_cube_data_null=driver_cube_data_null,
        driver_data_null=driver_data_null,
        finance_data_null=finance_data_null,
        passenger_recharge_data_null=passenger_recharge_data_null,
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
    ),
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
dependence_dm_oride_passenger_base_cube_d_prev_day_task >> \
dependence_dm_oride_driver_base_cube_d_prev_day_task >> \
dependence_dm_oride_driver_base_d_prev_day_task >>\
dependence_server_magic_now_day_task >>\
dependence_dwd_oride_order_finance_df_prev_day_task >>\
dependence_dwd_oride_passenger_recharge_df_prev_day_task >>\
sleep_time >> \
app_oride_global_operate_report_d_task >> \
touchz_data_success
