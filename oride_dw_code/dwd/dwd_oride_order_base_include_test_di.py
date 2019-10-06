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

dag = airflow.DAG('dwd_oride_order_base_include_test_di',
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
ods_sqoop_base_data_order_df_prev_day_task = HivePartitionSensor(
    task_id="ods_sqoop_base_data_order_df_prev_day_task",
    table="ods_sqoop_base_data_order_df",
    partition="dt='{{ds}}'",
    schema="oride_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
ods_sqoop_base_data_order_payment_df_prev_day_task = HivePartitionSensor(
    task_id="ods_sqoop_base_data_order_payment_df_prev_day_task",
    table="ods_sqoop_base_data_order_payment_df",
    partition="dt='{{ds}}'",
    schema="oride_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


# 依赖前一天分区
oride_client_event_detail_prev_day_task = HivePartitionSensor(
    task_id="oride_client_event_detail_prev_day_task",
    table="oride_client_event_detail",
    partition="dt='{{ds}}'",
    schema="oride_bi",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
dependence_dispatch_tracker_server_magic_task = HivePartitionSensor(
    task_id="dispatch_tracker_server_magic_task",
    table="dispatch_tracker_server_magic",
    partition="dt='{{macros.ds_add(ds, +1)}}' and hour='00'",
    schema="oride_source",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

table_name = "dwd_oride_order_base_include_test_di"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name



##----------------------------------------- 任务超时监控 ---------------------------------------## 

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    tb = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "3600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------## 

dwd_oride_order_base_include_test_di_task = HiveOperator(
    task_id='dwd_oride_order_base_include_test_di_task',

    hql='''
SET hive.exec.parallel=TRUE;
SET hive.exec.dynamic.partition.mode=nonstrict;


INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
SELECT base.order_id,
       --订单 ID

       city_id,
       --所属城市(-999 无效数据)

       product_id,
       --订单车辆类型(0: 专快混合 1:driect[专车] 2: street[快车] 3:Otrike 99:招手停)

       passenger_id,
       --乘客 ID

       start_name,
       --(用户下单时输入)起点名称

       start_lng,
       --(用户下单时输入)起点经度

       start_lat,
       --(用户下单时输入)起点纬度

       end_name,
       --(用户下单时输入)终点名称

       end_lng,
       --(用户下单时输入)终点经度

       end_lat,
       --(用户下单时输入)终点纬度

       duration,
       --订单持续时间

       distance,
       --订单距离

       basic_fare,
       --起步价

       dst_fare,
       -- 里程费

       dut_fare,
       -- 时长费

       dut_price,
       --时长价格

       dst_price,
       --距离价格

       price,
       -- 订单价格

       reward,
       -- 司机奖励

       driver_id,
       --司机 ID

       plate_num,
       --车牌号

       take_time,
       --接单(应答)时间

       wait_time,
       --到达接送点时间

       pickup_time,
       --接到乘客时间

       arrive_time,
       --到达终点时间

       finish_time,
       --订单(支付)完成时间

       cancel_role,
       --取消人角色(1: 用户, 2: 司机, 3:系统 4:Admin)

       cancel_time,
       --取消时间

       cancel_type,
       --取消原因类型

       cancel_reason,
       --取消原因

       status,
       --订单状态 (0: wait assign, 1: pick up passenger, 2: wait passenger, 3: send passenger, 4: arrive destination, 5: finished, 6: cancel)

       create_time,
       -- 创建时间

       fraud,
       --是否欺诈(0否1是)

       driver_serv_type,
       --司机服务类型(1: Direct 2:Street 3:Otrike)

       refund_before_pay,
       --支付前资金调整

       refund_after_pay,
       --支付后资金调整

       abnormal,
       --异常状态(0 否 1 逃单)

       flag_down_phone,
       --招手停上报手机号

       zone_hash,
       --所属区域 hash

       updated_time,
       --最后更新时间

       from_unixtime(create_time,'yyyy-MM-dd') AS create_date,
       --创建日期(转换自create_time,yyyy-MM-dd)

       (CASE
            WHEN driver_id <> 0 THEN 1
            ELSE 0
        END) AS is_td_request,
       --当天是否接单(应答)

       (CASE
            WHEN status IN (4,
                            5) THEN 1
            ELSE 0
        END) AS is_td_finish,
       --当天是否完单
       

       (CASE
            WHEN pickup_time <> 0 THEN pickup_time - take_time
            ELSE 0
        END) AS td_pick_up_dur,
       --当天接驾时长（秒）

       (CASE
            WHEN take_time <> 0 THEN take_time - create_time
            ELSE 0
        END) AS td_take_dur,
       --当天应答时长（秒）

       (CASE
            WHEN cancel_time>0
                 AND take_time > 0 THEN cancel_time - take_time
            ELSE 0
        END) AS td_cannel_pick_dur,
       --当天取消接驾时长（秒）

       (CASE
            WHEN pickup_time>0
                 AND wait_time > 0 THEN pickup_time - wait_time
            ELSE 0
        END) AS td_wait_dur,
       --当天等待上车时长（秒）
       
       
       (CASE
            WHEN arrive_time>0
                 AND take_time > 0 THEN arrive_time - take_time
            ELSE 0
        END) AS td_service_dur,
       --当天服务时长（秒）


       (CASE
            WHEN arrive_time>0
                 AND pickup_time > 0 THEN arrive_time - pickup_time
            ELSE 0
        END) AS td_billing_dur,
       --当天计费时长（秒）

       (CASE
            WHEN status = 5 
                 and finish_time>0
                 AND arrive_time > 0 THEN finish_time - arrive_time
            ELSE 0
        END) AS td_pay_dur,
       --当天支付时长(秒)

       (CASE
            WHEN status = 6
                 AND (cancel_role = 3
                      OR cancel_role = 4) THEN 1
            ELSE 0
        END) AS is_td_sys_cancel,
       --是否当天系统取消

       (CASE
            WHEN status = 6
                 AND driver_id = 0
                 AND cancel_role = 1 THEN 1
            ELSE 0
        END) AS is_td_passanger_before_cancel,
       --是否当天乘客应答前取消

       (CASE
            WHEN status = 6
                 AND driver_id <> 0
                 AND cancel_role = 1 THEN 1
            ELSE 0
        END) AS is_td_passanger_after_cancel,
       --是否当天乘客应答后取消

       (CASE
            WHEN status = 5 THEN 1
            ELSE 0
        END) AS is_td_finish_pay,
       --是否当天完成支付

       nvl(pay_amount,0) as pay_amount,
       --实付金额

       nvl(pay_mode,0) as pay_mode,
       --支付方式（0: 未知, 1: 线下支付, 2: opay, 3: 余额）,

       '' as part_hour, --小时分区时间(yyyy-mm-dd HH)

       (CASE
            WHEN status = 6
                 AND driver_id <> 0
                 AND cancel_role = 2 THEN 1
            ELSE 0
        END) AS is_td_driver_after_cancel,
       --是否当天司机应答后取消

       (CASE
            WHEN status in (4,5) and arrive_time>0
                 AND pickup_time > 0 THEN arrive_time - pickup_time
            ELSE 0
        END) AS td_finish_billing_dur,
       --当天完单计费时长（分钟）

       (CASE
            WHEN driver_id <> 0 
            and take_time > 0 and finish_time>0 THEN finish_time - take_time
            ELSE 0
        END) AS td_finish_order_dur,
       --当天完成做单时长（分钟）

       trip_id, --'行程 ID'
       wait_carpool,--'是否在等在拼车'
       pay_status, --支付类型（0: 支付中, 1: 成功, 2: 失败）
       pax_num, -- 乘客数量 
       tip,  --小费
       nvl(ep.estimated_price,-1) as estimated_price,
        --预估价格区间（最小值,最大值,-1 未知）
       if(push_ord.order_id is not null,1,0) as is_strong_dispatch,  
       --是否强制派单1:是，0:否
       country_code,

       '{pt}' AS dt
FROM
     (SELECT 

             id AS order_id ,
             --订单 ID

             user_id AS passenger_id,
             --乘客 ID

             start_name,
             --起点名称

             start_lng ,
             --起点经度

             start_lat,
             --起点纬度

             end_name,
             --终点名称

             end_lng ,
             --终点经度

             end_lat ,
             --终点纬度

             duration ,
             --订单持续时间

             distance ,
             --订单距离

             basic_fare ,
             --起步价

             dst_fare ,
             --里程费

             dut_fare ,
             --时长费

             dut_price ,
             --时长价格

             dst_price ,
             --距离价格

             price ,
             --订单价格

             reward ,
             --司机奖励

             driver_id ,
             --司机 ID

             plate_num ,
             --车牌号

             take_time ,
             --接单时间

             wait_time ,
             --到达接送点时间

             pickup_time ,
             --接到乘客时间

             arrive_time ,
             --到达终点时间

             finish_time ,
             --订单完成时间

             cancel_role ,
             --取消人角色(1: 用户, 2: 司机, 3:系统 4:Admin)

             cancel_time ,
             --取消时间

             cancel_type ,
             --取消原因类型

             cancel_reason ,
             --取消原因

             status ,
             --订单状态 (0: wait assign, 1: pick up passenger, 2: wait passenger, 3: send passenger, 4: arrive destination, 5: finished, 6: cancel)

             create_time ,
             --创建时间

             fraud ,
             --是否欺诈(0否1是)

             driver_serv_type ,
             --司机服务类型(1: Direct 2:Street)

             serv_type AS product_id,
             --订单车辆类型(0: 专快混合 1:driect[专车] 2: street[快车] 99:招手停)

             refund_before_pay ,
             --支付前资金调整

             refund_after_pay ,
             --支付后资金调整

             abnormal ,
             --异常状态(0 否 1 逃单)

             flag_down_phone ,
             --招手停上报手机号

             zone_hash ,
             --所属区域 hash

             updated_at AS updated_time ,
             --最后更新时间

             nvl(city_id,-999) AS city_id,
             --所属城市(-999 无效数据)

             'nal' AS country_code,
             --国家码字段

             trip_id, --'行程 ID'
             wait_carpool,--'是否在等在拼车',
             pax_num, -- 乘客数量 
             tip  --小费

      FROM oride_dw_ods.ods_sqoop_base_data_order_df
      WHERE dt = '{pt}'
         AND from_unixtime(create_time,'yyyy-MM-dd') = '{pt}'
         ) base
LEFT OUTER JOIN
(SELECT id AS order_id,
       status AS pay_status,
       --支付类型（0: 支付中, 1: 成功, 2: 失败）

       price AS pay_price,
       --价格

       amount AS pay_amount,
       --实付金额

       `mode` AS pay_mode
       --支付方式（0: 未知, 1: 线下支付, 2: opay, 3: 余额）


FROM oride_dw_ods.ods_sqoop_base_data_order_payment_df
WHERE dt = '{pt}') pay ON base.order_id=pay.order_id
LEFT OUTER JOIN
(SELECT get_json_object(event_value, '$.order_id') AS order_id,
       min(get_json_object(event_value, '$.estimated_price')) AS estimated_price --预估价格区间（最小值,最大值）
FROM oride_bi.oride_client_event_detail
WHERE event_name='successful_order_show'
  AND dt='{pt}'
  AND length(get_json_object(event_value, '$.estimated_price'))>1
GROUP BY get_json_object(event_value, '$.order_id')) ep
ON base.order_id=ep.order_id
left outer join

--(select order_id from oride_dw.dwd_oride_order_dispatch_funnel_di
--   WHERE dt='{pt}'
--     AND event_name='dispatch_push_driver'
--     AND assign_type=1
--     group by order_id) push_ord

(SELECT order_id
FROM
  (SELECT get_json_object(event_values, '$.order_id') AS order_id,
          --订单ID

          cast(get_json_object(event_values, '$.assign_type') AS bigint) AS assign_type
          --0=非强派单，1=强派单

   FROM oride_source.dispatch_tracker_server_magic
   WHERE dt = '{pt}'
     AND event_name='dispatch_push_driver') a1
WHERE assign_type=1
GROUP BY order_id) push_ord
on base.order_id=push_ord.order_id


'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
    ),
    dag=dag
)


def check_key_data(ds, **kargs):
    # 主键重复校验
    HQL_DQC = '''
    SELECT count(1) as nm
    FROM
     (SELECT order_id,
             count(1) as cnt
      FROM oride_dw.{table}

      WHERE dt='{pt}'
      GROUP BY order_id HAVING count(1)>1) t1
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name
    )

    cursor = get_hive_cursor()
    logging.info('Executing 主键重复校验: %s', HQL_DQC)

    cursor.execute(HQL_DQC)
    res = cursor.fetchone()

    if res[0] > 1:
        raise Exception("Error The primary key repeat !", res)
    else:
        print("-----> Notice Data Export Success ......")


# 主键重复校验
task_check_key_data = PythonOperator(
    task_id='check_data',
    python_callable=check_key_data,
    provide_context=True,
    dag=dag)


#生成_SUCCESS
def check_success(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"table":"{dag_name}".format(dag_name=dag_ids),"hdfs_path": "{hdfsPath}/country_code=nal/dt={pt}".format(pt=ds,hdfsPath=hdfs_path)}
    ]

    TaskTouchzSuccess().set_touchz_success(msg)

touchz_data_success= PythonOperator(
    task_id='touchz_data_success',
    python_callable=check_success,
    provide_context=True,
    dag=dag
)

ods_sqoop_base_data_order_df_prev_day_task >> \
ods_sqoop_base_data_order_payment_df_prev_day_task >> \
oride_client_event_detail_prev_day_task >> \
dependence_dispatch_tracker_server_magic_task >>\
sleep_time >> \
dwd_oride_order_base_include_test_di_task >> \
task_check_key_data >> \
touchz_data_success
