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

# 依赖前一天分区00点
ods_binlog_data_order_hi_prev_day_tesk = HivePartitionSensor(
    task_id="ods_binlog_data_order_hi_prev_day_task",
    table="ods_binlog_data_order_hi",
    partition="dt='{{ds}}' and hour='23'",
    schema="oride_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖当天分区00点
ods_binlog_data_order_hi_now_day_tesk = HivePartitionSensor(
    task_id="ods_binlog_data_order_hi_now_day_task",
    table="ods_binlog_data_order_hi",
    partition="dt='{{macros.ds_add(ds, +1)}}' and hour='00'",
    schema="oride_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区00点
ods_binlog_data_order_payment_hi_prev_day_tesk = HivePartitionSensor(
    task_id="ods_binlog_data_order_payment_hi_prev_day_task",
    table="ods_binlog_data_order_payment_hi",
    partition="dt='{{ds}}' and hour='23'",
    schema="oride_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖当天分区00点
ods_binlog_data_order_payment_hi_now_day_tesk = HivePartitionSensor(
    task_id="ods_binlog_data_order_payment_hi_now_day_task",
    table="ods_binlog_data_order_payment_hi",
    partition="dt='{{macros.ds_add(ds, +1)}}' and hour='00'",
    schema="oride_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

table_name = "dwd_oride_order_base_include_test_di"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

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

       pay.part_hour, --小时分区时间(yyyy-mm-dd HH)

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
       wait_carpool,--'是否在等在拼车',

       country_code,

       '{pt}' AS dt
FROM
  (SELECT order_id,
          --订单 ID

          city_id,
          --所属城市(-999 无效数据)

          product_id,
          --订单车辆类型(0: 专快混合 1:driect[专车] 2: street[快车] 99:招手停)

          passenger_id,
          --乘客 ID

          start_name,
          --起点名称

          start_lng,
          --起点经度

          start_lat,
          --起点纬度

          end_name,
          --终点名称

          end_lng,
          --终点经度

          end_lat,
          --终点纬度

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
          --接单时间

          wait_time,
          --到达接送点时间

          pickup_time,
          --接到乘客时间

          arrive_time,
          --到达终点时间

          finish_time,
          --订单完成时间

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
          --司机服务类型(1: Direct 2:Street )

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

          part_hour,
          --小时分区时间(yyyy-mm-dd HH)

          trip_id, --'行程 ID'
          wait_carpool,--'是否在等在拼车',

          country_code
   FROM
     (SELECT op,
             --操作类型 c 创建 u 更新 d 删除

             ts_ms,
             --事件时间（毫秒）

             gtid ,
             --事件唯一标识

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
             --订单车辆类型(0: all 1:driect 2: street)

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
             --所属城市

             concat_ws(' ',dt,hour) AS part_hour,
             --小时分区时间(yyyy-mm-dd HH)

             'nal' AS country_code,
             --国家码字段

             trip_id, --'行程 ID'
             wait_carpool,--'是否在等在拼车',

             row_number() OVER(partition BY id
                               ORDER BY updated_at desc,pos DESC) AS rn1
      FROM oride_dw.ods_binlog_data_order_hi
      WHERE concat_ws(' ',dt,hour) BETWEEN '{pt} 00' AND '{now_day} 00'  --取昨天1天数据与今天早上00数据
        AND from_unixtime(create_time,'yyyy-MM-dd') = '{pt}'
        AND op IN ('c',
                   'u')) t1
   WHERE rn1=1) base
LEFT OUTER JOIN
  (SELECT order_id,
          pay_status,
          --支付类型（0: 支付中, 1: 成功, 2: 失败）

          pay_price,
          --价格

          pay_amount,
          --实付金额

          pay_mode,
          --支付方式（0: 未知, 1: 线下支付, 2: opay, 3: 余额）,

          part_hour --小时分区时间(yyyy-mm-dd HH)

   FROM
     (SELECT id AS order_id,
             status AS pay_status,
             price AS pay_price,
             amount AS pay_amount,
             `mode` AS pay_mode,
             driver_id,
             concat_ws(' ',dt,hour) AS part_hour, --分区时间(yyyy-mm-dd HH)
             row_number() OVER(partition BY id
                               ORDER BY updated_at desc,pos DESC) AS rn1
      FROM oride_dw.ods_binlog_data_order_payment_hi
      WHERE concat_ws(' ',dt,hour) BETWEEN '{pt} 00' AND '{now_day} 00' --取昨天1天数据与今天早上00数据
        AND op IN ('c','u')) t1
   WHERE rn1=1) pay ON base.order_id=pay.order_id
AND base.part_hour=pay.part_hour;

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

ods_binlog_data_order_hi_prev_day_tesk >> \
ods_binlog_data_order_hi_now_day_tesk >> \
ods_binlog_data_order_payment_hi_prev_day_tesk >> \
ods_binlog_data_order_payment_hi_now_day_tesk >> \
sleep_time >> \
dwd_oride_order_base_include_test_di_task >> \
task_check_key_data >> \
touchz_data_success
