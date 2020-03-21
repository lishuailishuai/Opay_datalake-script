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
from airflow.sensors.web_hdfs_sensor import WebHdfsSensor
from airflow.sensors import UFileSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from plugins.CountriesPublicFrame import CountriesPublicFrame
import json
import logging
from airflow.models import Variable
import requests
import os
from airflow.sensors import OssSensor

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2020, 3, 8),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_order_payment_base_di',
                  schedule_interval="20 00 * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dwd_oride_order_payment_base_di"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 依赖 ---------------------------------------##
# 依赖前一天分区
ods_binlog_data_order_payment_hi_prev_day_task = OssSensor(
    task_id='ods_binlog_base_data_order_payment_hi_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour=23/_SUCCESS'.format(
        hdfs_path_str="oride_binlog/oride_db.oride_data.data_order_payment",
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"dag": dag, "db": "oride_dw", "table": "{dag_name}".format(dag_name=table_name),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "1200"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##
##----------------------------------------订单表使用说明-------------------------------##
# ！！！！由于数据迁移，oride订单表只支持从20200108号数据回溯
##------------------------------------------------------------------------------------##
def dwd_oride_order_payment_base_di_sql_task(ds):
    hql = '''
SET hive.exec.parallel=TRUE;
SET hive.exec.dynamic.partition.mode=nonstrict;


INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
SELECT pay.id as order_id,
       --订单ID
       pay.driver_id,
       --司机ID
       pay.mode,
       --支付模式（0: 未知, 1: 线下支付, 2: opay, 3: 余额）
       pay.price,
       --价格
       pay.coupon_id,
       --使用的优惠券 ID
       pay.coupon_name,
       --优惠券名称
       pay.coupon_amount,
       --使用的优惠券金额
       pay.amount,
       --实付金额
       pay.bonus,
       --使用的奖励金
       pay.balance,
       --使用的余额
       pay.opay_amount,
       --opay 支付的金额
       pay.reference,
       --opay 流水号
       pay.currency,
       --货币类型
       pay.country,
       --国家
       pay.status,
       --支付状态（0: 支付中, 1: 成功, 2: 失败）
       pay.modify_time,
       --最后修改时间
       pay.create_time,
       --创建时间
       pay.serv_type,
       --订单车辆类型(0: all 1:driect 2: street)
       pay.updated_at,
       --最后更新时间
       pay.pay_type,
       --支付类型(0:手动支付 1:自动支付)
       pay.tip,
       --小费
       pay.capped_mode,
       --优惠活动车型（0 ride 1 keke 2 car）
       pay.capped_type,
       --优惠活动类型（0 normal 1 novice）
       pay.capped_id,
       --优惠活动 ID
       pay.card_id,
       --支付卡号
       pay.surcharge,
       --服务费
       pay.tip_rake,
       --小费抽成
       pay.insurance_price,
       --保险费
       pay.local_modify_time,
       --本地最后修改时间
       pay.local_create_time,
       --本地创建时间
       pay.updated_time,
       --本地最后更新时间
       'nal' AS country_code,
       '{pt}' AS dt
FROM
  (SELECT *
   FROM
     (SELECT *,
             if(t.create_time=0,0,(t.create_time + 1 * 60 * 60)) AS local_create_time,
             if(t.modify_time=0,0,(t.modify_time + 1 * 60 * 60)) AS local_modify_time,
             from_unixtime((unix_timestamp(regexp_replace(regexp_replace(t.updated_at,'T',' '),'Z',''))+3600),'yyyy-MM-dd HH:mm:ss') AS updated_time,
             row_number() over(partition BY t.id
                               ORDER BY t.`__ts_ms` DESC,t.`__file` DESC,cast(t.`__pos` AS int) DESC) AS order_by
      FROM oride_dw_ods.ods_binlog_base_data_order_payment_hi t
      WHERE concat_ws(' ',dt,hour) BETWEEN '{bef_yes_day} 23' AND '{pt} 23' --取昨天1天数据与今天早上00数据

        AND (from_unixtime((t.create_time + 1 * 60 * 60 * 1),'yyyy-MM-dd') = '{pt}'
             OR from_unixtime((unix_timestamp(regexp_replace(regexp_replace(t.updated_at,'T',' '),'Z',''))+3600),'yyyy-MM-dd')='{pt}' ) ) t1
   WHERE --t1.`__deleted` = 'false' and  -- 由于订单每天会将30天之前的订单移动到data_order_history表中，因此移动走的打标为deleted，但是这些订单状态有改变是需留存的，因此不要限定删除条件
t1.order_by = 1) pay;
'''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        bef_yes_day=airflow.macros.ds_add(ds, -1),
        now_hour='{{ execution_date.strftime("%H") }}',
        table=table_name,
        db=db_name
    )
    return hql


def check_key_data_task(ds):
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
    _sql = "\n" + cf.alter_partition() + "\n" + dwd_oride_order_payment_base_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 熔断数据
    check_key_data_task(ds)

    # 生产success
    cf.touchz_success()


dwd_oride_order_payment_base_di_task = PythonOperator(
    task_id='dwd_oride_order_payment_base_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

ods_binlog_data_order_payment_hi_prev_day_task >> dwd_oride_order_payment_base_di_task