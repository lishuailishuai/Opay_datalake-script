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
from plugins.CountriesPublicFrame import CountriesPublicFrame
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2019, 9, 9),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_global_operate_report_d',
                  schedule_interval="50 01 * * *",
                  default_args=args)

##----------------------------------------- 依赖 ---------------------------------------##


# 依赖前一天分区
dependence_dm_oride_order_base_d_prev_day_task = UFileSensor(
    task_id='dm_oride_order_base_d_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dm_oride_order_base_d/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
dependence_dm_oride_passenger_base_cube_prev_day_task = UFileSensor(
    task_id='dm_oride_passenger_base_cube_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dm_oride_passenger_base_cube/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwm_oride_passenger_base_df_prev_day_task = UFileSensor(
    task_id='dwm_oride_passenger_base_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwm_oride_passenger_base_df/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
dependence_dm_oride_driver_base_prev_day_task = UFileSensor(
    task_id='dm_oride_driver_base_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dm_oride_driver_base/country_code=NG",
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
dependence_dwm_oride_driver_finance_di_prev_day_task = UFileSensor(
    task_id='dwm_oride_driver_finance_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwm_oride_driver_finance_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
dependence_dm_oride_driver_order_base_cube_prev_day_task = UFileSensor(
    task_id='dm_oride_driver_order_base_cube',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dm_oride_driver_order_base_cube/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "4200"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "app_oride_global_operate_report_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本变量 ---------------------------------------##

order_data_null = """
           null as ride_order_cnt, --当日下单量
           null as finish_order_cnt, --当日完单量
           null as finish_pay, --当日成功完成支付量，自12.18开始多限定支付成功
           null as valid_ord_cnt,  --当日有效订单量
           null as wet_ord_cnt, --当日湿单订单量
           null as bad_feedback_finish_ord_cnt, --当日差评完单量
           null as beckoning_num, --当日招手停完单数
           null as finish_take_order_dur,  --当日完单应答时长
           null as finish_pick_up_dur,  --当日完单接驾时长
           null as finish_billing_dur,  --当日完单计费时长【跟计费时长有点差异】
           null as finish_order_onride_dis,  --当日完单送驾距离
           null as pax_num,  --乘客数
           null as price, --当日完单gmv,订单状态4，5
           null as pay_price,  --当日应付金额，订单状态5
           null as pay_amount,  --当日实付金额，订单状态5
           null as opay_pay_cnt, --opay支付订单数,pay_mode=2
           null as opay_pay_failed_cnt, --opay支付失败订单数,pay_mode=2 and pay_status in(0,2)
           null as order_cnt_lfw, --近四周同期下单数据
           null as finish_order_cnt_lfw,  --近四周同期完单数据
           null as online_pay_price,  --当日用opay的订单金额12.18号开始，自1226号升级为当日线上支付成功、订单状态为5且不包含招手停
           null as online_pay_amount,  --当日用opay实付金额12.18号开始，自1226号升级为当日线上支付成功、订单状态为5且不包含招手停
           null as falsify, --用户罚款，自12.25号开始该表接入
           null as falsify_driver_cancel --司机罚款，自12.25号开始该表接入
           """

passenger_data_null = """
           null as new_users,  --当天注册乘客数
           null as act_users  --当天活跃乘客数
    """

passenger_order_data_null = """
           null as ord_users,  --当日下单乘客数
           null as finished_users,  --当日完单乘客数
           null as first_finished_users,  --当日首次完单乘客数
           null as old_finished_users,  --当日完单老客数
           null as new_user_ord_cnt,  --当日新注册乘客下单量
           null as new_user_finished_cnt,  --当日新注册乘客完单量
           null as nobeckon_paid_users,  --当日总支付乘客数，自12.18号升级逻辑去掉招手停，发邮件需要改字段名称
           null as nobeckon_opay_paid_users,  --当日opay支付成功乘客数，自12.18号升级逻辑去掉招手停，发邮件需要改字段名称
           null as nobeckon_online_paid_users,  --当日线上支付乘客数，自12.18号开始新增
           null as new_user_gmv  --当日新注册乘客完单gmv 
    """

driver_data_null = """
           null as td_audit_finish_driver_num,  --当日审核通过司机数（包含同时呼叫）
           null as td_online_driver_num,  --当日在线司机数（包含同时呼叫）
           null as td_request_driver_num_inSimulRing, --当日接单司机数（包含同时呼叫）
           null as td_finish_order_driver_num_inSimulRing,  --当日完单司机数（包含同时呼叫）
           null as td_succ_broadcast_driver_num, --被推送骑手数
           null as finish_driver_online_dur,  --当日完单司机在线时长
           null as driver_billing_dur, --当日司机计费时长
           null as driver_pushed_order_cnt  --司机被推送订单数
    """

finance_data_null = """
           null AS recharge_amount, --充值金额
           null AS reward_amount --奖励金额
         --  null AS amount_pay_online, --当日总收入-线上支付金额
         --  null AS amount_pay_offline --当日总收入-线下支付金额 
    """

union_product_data_null = """
           null as finish_order_cnt_inSimulRing, --当日完单量(包含同时呼叫)
           null as td_request_driver_num, --当日接单司机数（不包含同时呼叫）
           null as td_finish_order_driver_num  --当日完单司机数（不包含同时呼叫）
          -- null as iph_fenzi_inSimulRing --iph分子（包含同时呼叫）
    """
##----------------------------------------- 脚本 ---------------------------------------##
def app_oride_global_operate_report_d_sql_task(ds):
    HQL ='''
    SET hive.exec.parallel=true;
    SET hive.exec.dynamic.partition=true;
    SET hive.exec.dynamic.partition.mode=nonstrict;
    
    with order_data as
    (
--订单相关,只过滤当日的
select nvl(t.country_code,'total') as country_code,
       t.city_id,
       t.product_id,
       sum(ride_order_cnt) as ride_order_cnt, --当日下单量
       sum(finish_order_cnt) as finish_order_cnt, --当日完单量
       sum(finish_pay) as finish_pay, --当日成功支付完单量
       sum(valid_ord_cnt) as valid_ord_cnt,  --当日有效订单量
       sum(wet_ord_cnt) as wet_ord_cnt, --当日湿单订单量
       sum(bad_feedback_finish_ord_cnt) as bad_feedback_finish_ord_cnt,  --当日差评完单量
       sum(beckoning_num) as beckoning_num,  --当日招手停完单数
       sum(finish_take_order_dur) as finish_take_order_dur,  --当日完单应答时长
       sum(finish_pick_up_dur) as finish_pick_up_dur,  --当日完单接驾时长
       sum(finish_billing_dur) as finish_billing_dur,  --当日完单计费时长【跟计费时长有点差异】
       sum(finish_order_onride_dis) as finish_order_onride_dis,  --当日完单送驾距离
       sum(pax_num) as pax_num,  --乘客数
       sum(price) as price, --当日完单gmv,订单状态4，5
       sum(pay_price) as pay_price,  --当日应付金额，订单状态5且支付成功，用于统计单均应付
       sum(pay_amount) as pay_amount,  --当日实付金额，订单状态5且支付成功，用于统计单均实付
       sum(opay_pay_cnt) as opay_pay_cnt, --opay支付订单数,pay_mode=2
       sum(opay_pay_failed_cnt) as opay_pay_failed_cnt, --opay支付失败订单数,pay_mode=2 and pay_status in(0,2)
       sum(order_cnt_lfw) as order_cnt_lfw,  --近四周同期下单数据 
       sum(finish_order_cnt_lfw) as finish_order_cnt_lfw,  --近四周同期完单数据
       sum(online_pay_price) as online_pay_price,  --当日线上支付成功订单金额12.18号开始,用于统计c补和gmv
       sum(online_pay_amount) as online_pay_amount, --当日线上支付成功金额12.18号开始,用于统计c补
       sum(falsify) as falsify, --用户罚款，自12.25号开始该表接入
       sum(falsify_driver_cancel) as falsify_driver_cancel, --司机罚款，自12.25号开始该表接入
       {passenger_data_null},
       {passenger_order_data_null},
       {driver_data_null},
       null as map_request_num,
       {finance_data_null},
       {union_product_data_null}  
from (SELECT dt,country_code,
       city_id,
       product_id,
       sum(ride_order_cnt) as ride_order_cnt, --当日下单量
       sum(finish_order_cnt) as finish_order_cnt, --当日完单量（不包含同时呼叫）
       sum(if(product_id<>99,finish_pay,0)) as finish_pay, --当日成功完成支付量，不含招手停
       sum(valid_ord_cnt) as valid_ord_cnt,  --当日有效订单量
       sum(wet_ord_cnt) as wet_ord_cnt, --当日湿单订单量
       sum(bad_feedback_finish_ord_cnt) as bad_feedback_finish_ord_cnt,--当日差评完单量
       sum(if(product_id=99,finish_order_cnt,0)) as beckoning_num, --招手停完单数
       sum(finish_take_order_dur) as finish_take_order_dur,  --当日完单应答时长
       sum(finish_pick_up_dur) as finish_pick_up_dur,  --当日完单接驾时长
       sum(finish_billing_dur) as finish_billing_dur,  --当日完单计费时长【跟计费时长有点差异】
       sum(finish_order_onride_dis) as finish_order_onride_dis,  --当日完单送驾距离
       sum(pax_num) as pax_num,  --乘客数
       sum(price) as price, --当日完单gmv,订单状态4，5
       sum(if(product_id<>99,pay_price,0)) as pay_price,  --当日应付金额，订单状态5且支付成功，12.18号开始升级，用于统计单均应付
       sum(if(product_id<>99,pay_amount,0)) as pay_amount,  --当日实付金额，订单状态5且支付成功，12.18号开始升级，用于统计单均实付
       sum(opay_pay_cnt) as opay_pay_cnt, --opay支付订单数,pay_mode=2
       sum(opay_pay_failed_cnt) as opay_pay_failed_cnt, --opay支付失败订单数,pay_mode=2 and pay_status in(0,2)
       order_cnt_lfw, --近四周同期下单数据均值
       finish_order_cnt_lfw,  --近四周同期完单数据
       sum(if(product_id<>99,online_pay_price,0)) as online_pay_price,  --当日线上支付成功订单金额12.18号开始,用于统计c补和gmv
       sum(if(product_id<>99,online_pay_amount,0)) as online_pay_amount,  --当日线上支付成功金额12.18号开始,用于统计c补
       sum(falsify) as falsify, --用户罚款，自12.25号开始该表接入
       sum(falsify_driver_cancel) as falsify_driver_cancel --司机罚款，自12.25号开始该表接入
       
FROM (SELECT sum(if(dt>=date_add('{pt}',-28)
              AND dt<'{pt}'
              AND from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')=from_unixtime(unix_timestamp('{pt}', 'yyyy-MM-dd'),'u'),ride_order_cnt,0)) over (partition BY '{pt}',country_code_new, city_id, product_id)/4 AS order_cnt_lfw,--近四周同期下单数据均值
       sum(if(dt>=date_add('{pt}',-28)
              AND dt<'{pt}'
              AND from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')=from_unixtime(unix_timestamp('{pt}', 'yyyy-MM-dd'),'u'),finish_order_cnt,0)) over (partition BY '{pt}',country_code_new, city_id, product_id)/4 AS finish_order_cnt_lfw,--近四周同期完单数据
       *
from (select *,if(dt<'2019-12-08' and country_code='nal','NG',country_code) as country_code_new  --此处由于新上线国家码，因此需要处理下统计近四周数据
FROM oride_dw.dm_oride_order_base_d
WHERE dt>=date_add('{pt}',-28)
  AND dt<='{pt}') t) m 
  where m.dt='{pt}'
  group by m.dt,m.country_code,
             m.city_id,
             m.product_id,
             m.order_cnt_lfw,
             m.finish_order_cnt_lfw) t
group by nvl(t.country_code,'total'),
       t.city_id,  --with cube时，默认值无效
       t.product_id
with cube),

--乘客相关
passenger_data as
(
select 'total' as country_code,
       -10000 as city_id,
       -10000 as product_id, 
       {order_data_null},
       sum(is_td_register) as new_users,  --当天注册乘客数
       sum(is_td_act) as act_users,  --当天活跃乘客数
       {passenger_order_data_null},
       {driver_data_null},
       null as map_request_num,
       {finance_data_null},
       {union_product_data_null}  
from oride_dw.dwm_oride_passenger_base_df
where dt='{pt}'
),

--乘客与订单相关
passenger_order_data as 
(
select country_code,
       city_id,
       product_id,   --乘客和订单相关的指标通过订单表的下单业务类型区分业务类型维度
       {order_data_null},
       {passenger_data_null},
       ord_users,  --当日下单乘客数
       finished_users,  --当日完单乘客数
       first_finished_users,  --当日首次完单乘客数
       (nvl(finished_users,0)-nvl(first_finished_users,0)) as old_finished_users,  --当日完单老客数
       new_user_ord_cnt,  --当日新注册乘客下单量
       new_user_finished_cnt,  --当日新注册乘客完单量
       nobeckon_paid_users,  --当日总支付乘客数，自12.18号升级逻辑去掉招手停，发邮件需要改字段名称
       nobeckon_opay_paid_users,  --当日opay支付成功乘客数，自12.18号升级逻辑去掉招手停，发邮件需要改字段名称
       nobeckon_online_paid_users,  --当日线上支付乘客数，自12.18号开始新增
      -- new_user_gmv,  --当日新注册乘客完单gmv  
       if(product_id<>99,nvl(new_user_online_pay_price,0),0)+nvl(falsify,0)+nvl(falsify_driver_cancel,0) as new_user_gmv, --当日注册乘客线上支付成功gmv，自12.18号开始逻辑升级
       {driver_data_null},
       null as map_request_num,
       {finance_data_null},
       {union_product_data_null}   
from oride_dw.dm_oride_passenger_base_cube
where dt='{pt}' and nvl(driver_serv_type,'-10000')='-10000' and country_code='total'
),    
        
--司机相关cube和非cube
driver_data as
(
select nvl(country_code,'total') as country_code,
       nvl(city_id,-10000) as city_id,
       nvl(product_id,-10000) as product_id,
       {order_data_null},
       {passenger_data_null},
       {passenger_order_data_null},
       td_audit_finish_driver_num,  --当日审核通过司机数（包含同时呼叫）
       td_online_driver_num,  --当日在线司机数（包含同时呼叫）
       td_request_driver_num_inSimulRing, --当日接单司机数（包含同时呼叫）
       td_finish_order_driver_num_inSimulRing,  --当日完单司机数（包含同时呼叫）
       td_succ_broadcast_driver_num, --被推送骑手数
       finish_driver_online_dur,  --当日完单司机在线时长
       driver_billing_dur, --当日司机计费时长
       driver_pushed_order_cnt,  --司机被推送订单数,之前统计偏小  
       null as map_request_num,
       {finance_data_null},
       {union_product_data_null}        
from(select country_code,
       city_id,
       product_id,      
       sum(td_audit_finish_driver_num) as td_audit_finish_driver_num,  --当日审核通过司机数（包含同时呼叫）
       sum(td_online_driver_num) as td_online_driver_num,  --当日在线司机数（包含同时呼叫）
       sum(td_request_driver_num) as td_request_driver_num_inSimulRing, --当日接单司机数（包含同时呼叫）
       sum(td_finish_order_driver_num) as td_finish_order_driver_num_inSimulRing,  --当日完单司机数（包含同时呼叫）
       sum(td_succ_broadcast_driver_num) as td_succ_broadcast_driver_num, --被成功播单司机数  自11.24号从show节点切为push节点
       sum(finish_driver_online_dur) as finish_driver_online_dur,  --当日完单司机在线时长
       sum(driver_billing_dur) as driver_billing_dur, --当日司机计费时长[！！！不准确]
       sum(driver_pushed_order_cnt) as driver_pushed_order_cnt  --司机被推送订单数       
from oride_dw.dm_oride_driver_base
where dt='{pt}'
group by country_code,
       city_id,
       product_id
with cube) t
),
        
--地图调用相关
map_data as
(
SELECT 'total' as country_code,
       -10000 as city_id,
       -10000 as product_id,
       {order_data_null},
       {passenger_data_null},
       {passenger_order_data_null},
       {driver_data_null},
       count(1) as map_request_num,  --地图调用次数
       {finance_data_null},
       {union_product_data_null}   
       FROM oride_source.server_magic
       WHERE dt='{pt}'
       and event_name in ('googlemap_directions', 'googlemap_nearbysearch', 'googlemap_autocomplete', 'googlemap_details', 'googlemap_geocode')),

--gmv相关  
finance_data as
(
select nvl(country_code,'total') as country_code,
       nvl(city_id,-10000) as city_id,
       nvl(product_id,-10000) as product_id,
       {order_data_null},
       {passenger_data_null},
       {passenger_order_data_null},
       {driver_data_null},
       null as map_request_num,  --地图调用次数
       recharge_amount AS recharge_amount, --资金调整金额,用于统计实际b补
       reward_amount AS reward_amount, --奖励金额,用于统计实际b补
       {union_product_data_null}   
from (select fin.country_code,
             fin.city_id,
             fin.product_id,
             sum(fin.recharge_amount) AS recharge_amount, --资金调整金额,用于统计实际b补
             sum(fin.reward_amount) AS reward_amount --奖励金额,用于统计实际b补 
        from (select * 
              from oride_dw.dwm_oride_driver_finance_di 
              where dt='{pt}' and city_id<>999001 and driver_id<>1) fin
        inner join
        (SELECT product_id1,
                       city_id
        FROM oride_dw.dim_oride_city 
        LATERAL VIEW explode(split(regexp_replace(product_id,'\\\\[|\\\\]',''),',')) s AS product_id1
        WHERE dt='{pt}' and city_id<>999001) cit
        on fin.product_id=cit.product_id1 and fin.city_id=cit.city_id
        group by fin.country_code,
               fin.city_id,
               fin.product_id
        with cube) t1
),

--混合业务线新增指标
union_product_data as
(
select nvl(country_code,'total') as country_code,
       nvl(city_id,-10000) as city_id,
       nvl(product_id,-10000) as product_id,
       {order_data_null},
       {passenger_data_null},
       {passenger_order_data_null},
       {driver_data_null},
       null as map_request_num,  --地图调用次数
       {finance_data_null},
       sum(finish_order_cnt_inSimulRing) as finish_order_cnt_inSimulRing, --当日完单量(包含同时呼叫)
       sum(td_request_driver_num) as td_request_driver_num, --当日接单司机数（不包含同时呼叫）
       sum(td_finish_order_driver_num) as td_finish_order_driver_num  --当日完单司机数（不包含同时呼叫）
      -- 0 as iph_fenzi_inSimulRing --iph分子（包含同时呼叫）
from (select nvl(a.country_code,'total') as country_code,
			 nvl(a.city_id,-10000) as city_id,
			 nvl(a.product_id,-10000) as product_id,  --包含同时呼叫的业务线
 			 finish_order_cnt_inSimulRing, --当日完单量(包含同时呼叫)
 			 null as td_request_driver_num, --当日接单司机数（不包含同时呼叫）
             null as td_finish_order_driver_num  --当日完单司机数（不包含同时呼叫）
 		from (select country_code,
			 city_id,
			 driver_serv_type as product_id,  --包含同时呼叫的业务线
 			 sum(finish_order_cnt) as finish_order_cnt_inSimulRing --当日完单量(包含同时呼叫)			 
FROM oride_dw.dm_oride_order_base_d
where dt='{pt}' 
group by country_code,city_id,driver_serv_type
with cube) a

union all
select country_code,
       city_id,
       product_id,   --不包含同时呼叫的业务线
       null as finish_order_cnt_inSimulRing, --当日完单量(包含同时呼叫)
       td_request_driver_num, --当日接单司机数(不包含同时呼叫)
       td_finish_order_driver_num  --当日完单司机数(不包含同时呼叫)
from oride_dw.dm_oride_driver_order_base_cube
where dt='{pt}' 
) m
group by nvl(country_code,'total'),
nvl(city_id,-10000),
nvl(product_id,-10000)
)
       
INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)       
SELECT nvl(city_id,-10000) as city_id,
       nvl(product_id,-10000) as product_id,
       sum(ride_order_cnt) as ride_order_cnt, --当日下单量
       sum(finish_order_cnt) as finish_order_cnt, --当日完单量
       sum(finish_pay) as finish_pay, --当日完成支付量
       sum(valid_ord_cnt) as valid_ord_cnt,  --当日有效订单量
       sum(beckoning_num) as beckoning_num, --当日招手停完单数
       sum(finish_take_order_dur) as finish_take_order_dur,  --当日完单应答时长
       sum(finish_pick_up_dur) as finish_pick_up_dur,  --当日完单接驾时长
       sum(finish_billing_dur) as finish_billing_dur,  --当日完单计费时长【跟计费时长有差异】
       sum(finish_order_onride_dis) as finish_order_onride_dis,  --当日完单送驾距离
       sum(pax_num) as pax_num,  --乘客数
       sum(price) as price, --当日完单gmv,订单状态4，5
       sum(pay_price) as pay_price,  --当日应付金额，订单状态5
       sum(pay_amount) as pay_amount,  --当日实付金额，订单状态5
       sum(opay_pay_cnt) as opay_pay_cnt, --opay支付订单数,pay_mode=2
       sum(opay_pay_failed_cnt) as opay_pay_failed_cnt, --opay支付失败订单数,pay_mode=2 and pay_status in(0,2)
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
       sum(nobeckon_paid_users) as nobeckon_paid_users,  --当日总支付乘客数，自12.18号升级逻辑去掉招手停，发邮件需要改字段名称
       sum(nobeckon_opay_paid_users) as nobeckon_opay_paid_users,  --当日opay支付乘客数，自12.18号升级逻辑去掉招手停，发邮件需要改字段名称
       sum(new_user_gmv) as new_user_gmv,  --当日新注册乘客完单gmv 
       sum(td_audit_finish_driver_num) as td_audit_finish_driver_num,  --当日审核通过司机数（包含同时呼叫）
       sum(td_online_driver_num) as td_online_driver_num,  --当日在线司机数（包含同时呼叫）
       sum(td_request_driver_num_inSimulRing) as td_request_driver_num_inSimulRing, --当日接单司机数（包含同时呼叫）
       sum(td_finish_order_driver_num_inSimulRing) as td_finish_order_driver_num_inSimulRing,  --当日完单司机数（包含同时呼叫）
       sum(td_succ_broadcast_driver_num) as td_succ_broadcast_driver_num, --被推送骑手数 
       sum(finish_driver_online_dur) as finish_driver_online_dur,  --当日完单司机在线时长
       sum(driver_billing_dur) as driver_billing_dur, --当日司机计费时长
       sum(driver_pushed_order_cnt) as driver_pushed_order_cnt,  --司机被推送订单数
       sum(map_request_num) as map_request_num,  --地图调用次数
       sum(recharge_amount) as recharge_amount, --充值金额
       sum(reward_amount) AS reward_amount, --奖励金额
       sum(online_pay_price) as online_pay_price,  --当日线上支付成功订单金额12.18号开始,用于统计c补和gmv
       sum(online_pay_amount) as online_pay_amount, --当日线上支付成功订单金额12.18号开始,用于统计c补  
       sum(falsify) as falsify, --用户罚款，自12.25号开始该表接入
       sum(falsify_driver_cancel) as falsify_driver_cancel, --司机罚款，自12.25号开始该表接入         
      -- null as recharge_users, --每天充值用户数
      -- null as user_recharge_succ_balance,  --每天用户充值真实金额
       sum(wet_ord_cnt) as wet_ord_cnt, --当日湿单订单量
       sum(bad_feedback_finish_ord_cnt) as bad_feedback_finish_ord_cnt, --当日差评完单量
       sum(finish_order_cnt_inSimulRing) as finish_order_cnt_inSimulRing, --当日完单量(包含同时呼叫)
       sum(td_request_driver_num) as td_request_driver_num, --当日接单司机数（不包含同时呼叫）
       sum(td_finish_order_driver_num) as td_finish_order_driver_num,  --当日完单司机数（不包含同时呼叫）
       --null as iph_fenzi_inSimulRing, --iph分子（包含同时呼叫）
       sum(nobeckon_online_paid_users) as nobeckon_online_paid_users,  --当日线上支付乘客数，自12.18号升级逻辑去掉招手停，发邮件需要改字段名称
       --nvl(country_code,'total') as country_code,
       if(nvl(country_code,'total')='total','nal','nal') as country_code,
       '{pt}' as dt
FROM (select * from order_data where nvl(country_code,'total')='total'
UNION ALL 
select * from passenger_data
UNION ALL 
select * from passenger_order_data
UNION ALL 
select * from driver_data where country_code='total'
UNION ALL 
select * from map_data
union all 
select * from finance_data where nvl(country_code,'total')='total'
union all
select * from union_product_data where country_code ='total') t
GROUP BY if(nvl(country_code,'total')='total','nal','nal'),
       nvl(city_id,-10000),
       nvl(product_id,-10000);
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name,
        order_data_null=order_data_null,
        passenger_data_null=passenger_data_null,
        passenger_order_data_null=passenger_order_data_null,
        driver_data_null=driver_data_null,
        finance_data_null=finance_data_null,
        union_product_data_null=union_product_data_null
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
    cf = CountriesPublicFrame("false", ds, db_name, table_name, hdfs_path, "true", "true")

    # 删除分区
    cf.delete_partition()

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + app_oride_global_operate_report_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 生产success
    cf.touchz_success()

app_oride_global_operate_report_d_task = PythonOperator(
    task_id='app_oride_global_operate_report_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

dependence_dm_oride_order_base_d_prev_day_task >> app_oride_global_operate_report_d_task
dependence_dm_oride_passenger_base_cube_prev_day_task >> app_oride_global_operate_report_d_task
dwm_oride_passenger_base_df_prev_day_task >> app_oride_global_operate_report_d_task
dependence_dm_oride_driver_base_prev_day_task >> app_oride_global_operate_report_d_task
dependence_server_magic_now_day_task >> app_oride_global_operate_report_d_task
dependence_dwm_oride_driver_finance_di_prev_day_task >> app_oride_global_operate_report_d_task
dependence_dm_oride_driver_order_base_cube_prev_day_task >> app_oride_global_operate_report_d_task



