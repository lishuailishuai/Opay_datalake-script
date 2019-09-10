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

dag = airflow.DAG( 'dwd_oride_order_pay_detail_di', 
    schedule_interval="00 03 * * *", 
    default_args=args,
    catchup=False) 


sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------## 

#依赖前一天分区00点
ods_binlog_data_order_payment_hi_prev_day_tesk=HivePartitionSensor(
      task_id="ods_binlog_data_order_payment_hi_prev_day_task",
      table="ods_binlog_data_order_payment_hi",
      partition="dt='{{ds}}' and hour='23'",
      schema="oride_dw_ods",
      poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
      dag=dag
    )

#依赖当天分区00点
ods_binlog_data_order_payment_hi_now_day_tesk=HivePartitionSensor(
      task_id="ods_binlog_data_order_payment_hi_now_day_task",
      table="ods_binlog_data_order_payment_hi",
      partition="dt='{{macros.ds_add(ds, +1)}}' and hour='00'",
      schema="oride_dw_ods",
      poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
      dag=dag
    )


#依赖前一天分区
dwd_oride_order_base_include_test_di_prev_day_tesk=UFileSensor(
    task_id='dwd_oride_order_base_include_test_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=nal",
        pt='{{ds}}'
        ),
    bucket_name='opay-datalake',
    poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
    dag=dag
        )


##----------------------------------------- 变量 ---------------------------------------##


table_name="dwd_oride_order_pay_detail_di"
hdfs_path="ufile://opay-datalake/oride/oride_dw/"+table_name


##----------------------------------------- 脚本 ---------------------------------------## 

dwd_oride_order_pay_detail_di_task = HiveOperator(

    task_id='dwd_oride_order_pay_detail_di_task',
    hql='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
select 
            pay.order_id,--订单 ID
            driver_id,--司机ID
            pay_mode,--支付模式（0: 未知, 1: 线下支付, 2: opay, 3: 余额）
            price,-- 价格
            coupon_id,--使用的优惠券 ID
            coupon_name ,--优惠券名称
            coupon_amount,-- 使用的优惠券金额
            pay_amount,--实付金额
            bonus,-- 使用的奖励金
            balance,--使用的余额
            opay_amount,--opay 支付的金额
            reference,-- opay 流水号
            currency,--货币类型
            country,--国家
            status,--支付模式（0: 支付中, 1: 成功, 2: 失败）
            modify_time,--最后修改时间
            create_time,--创建时间
            product_id,--订单业务类型(0: all 1:driect 2: street)
            updated_time,--最后更新时间
            pay_type, --支付类型(0:手动支付 1:自动支付)
            city_id,--所属城市
            passenger_id, --乘客 ID
            country_code,
            '{pt}' as dt
from 

(SELECT 
            order_id,--订单 ID
            driver_id,--司机ID
            mode as pay_mode,--支付模式（0: 未知, 1: 线下支付, 2: opay, 3: 余额）
            price,-- 价格
            coupon_id,--使用的优惠券 ID
            coupon_name ,--优惠券名称
            coupon_amount,-- 使用的优惠券金额
            amount as pay_amount,--实付金额
            bonus,-- 使用的奖励金
            balance,--使用的余额
            opay_amount,--opay 支付的金额
            reference,-- opay 流水号
            currency,--货币类型
            country,--国家
            status,--支付模式（0: 支付中, 1: 成功, 2: 失败）
            modify_time,--最后修改时间
            create_time,--创建时间
            product_id,--订单业务类型(0: all 1:driect 2: street)
            updated_time,--最后更新时间
            pay_type, --支付类型(0:手动支付 1:自动支付)
            part_hour
   FROM
     (SELECT 
             row_number() OVER(partition BY id
                               ORDER BY updated_at desc,pos DESC) AS rn1,

            op,--操作类型 (c 创建 u 更新 d 删除)
            ts_ms,-- 事件时间（毫秒）
            gtid,--事件唯一标识
            id as order_id,--订单 ID
            driver_id,--司机ID
            mode,--支付模式（0: 未知, 1: 线下支付, 2: opay, 3: 余额）
            price,-- 价格
            coupon_id,--使用的优惠券 ID
            coupon_name ,--优惠券名称
            coupon_amount,-- 使用的优惠券金额
            amount,--实付金额
            bonus,-- 使用的奖励金
            balance,--使用的余额
            opay_amount,--opay 支付的金额
            reference,-- opay 流水号
            currency,--货币类型
            country,--国家
            status,--支付模式（0: 支付中, 1: 成功, 2: 失败）
            modify_time,--最后修改时间
            create_time,--创建时间
            serv_type as product_id,--订单业务类型(0: all 1:driect 2: street)
            updated_at as updated_time,--最后更新时间
            pay_type, --支付类型(0:手动支付 1:自动支付)
            concat_ws(' ',dt,hour) AS part_hour
             --小时分区时间(yyyy-mm-dd HH)
    
      FROM oride_dw_ods.ods_binlog_data_order_payment_hi
      WHERE concat_ws(' ',dt,hour) BETWEEN '{pt} 00' AND '{now_day} 00'  --取昨天1天数据与今天早上00数据
        AND (op IN ('c',
                   'u') or op is null)) t1
   WHERE rn1=1 ) pay
left outer join
(SELECT 
           order_id,
           city_id,--所属城市
            passenger_id,
             --乘客 ID
             part_hour,
             country_code,
             dt
      FROM oride_dw.dwd_oride_order_base_include_test_di
WHERE dt = '{pt}'
  and status=5
  AND city_id<>'999001' --去除测试数据
  ) ord
on pay.order_id=ord.order_id
'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
        ),
schema='oride_dw',
    dag=dag)


#熔断数据，如果数据重复，报错
def check_key_data(ds,**kargs):

    #主键重复校验
    HQL_DQC='''
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

    if res[0] >1:
        raise Exception ("Error The primary key repeat !", res)
    else:
        print("-----> Notice Data Export Success ......")
    
 
task_check_key_data = PythonOperator(
    task_id='check_data',
    python_callable=check_key_data,
    provide_context=True,
    dag=dag
)

#生成_SUCCESS
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
        hdfs_data_dir=hdfs_path+'/country_code=nal/dt={{ds}}'
        ),
    dag=dag)


ods_binlog_data_order_payment_hi_prev_day_tesk>>ods_binlog_data_order_payment_hi_now_day_tesk>>dwd_oride_order_base_include_test_di_prev_day_tesk>>sleep_time>>dwd_oride_order_pay_detail_di_task>>task_check_key_data>>touchz_data_success