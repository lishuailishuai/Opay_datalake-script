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
from plugins.TaskHourSuccessCountMonitor import TaskHourSuccessCountMonitor
from airflow.sensors import OssSensor
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2020, 3, 11),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_passenger_estimate_records_di',
                  schedule_interval="30 00 * * *",
                  default_args=args,
                  )

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
ods_binlog_base_data_order_hi_prev_day_task = OssSensor(
    task_id='ods_binlog_base_data_order_hi_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour=23/_SUCCESS'.format(
        hdfs_path_str="oride_binlog/oride_db.oride_data.data_user_estimate_records",
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dwd_oride_passenger_estimate_records_di"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"dag": dag, "db": "oride_dw", "table": "{dag_name}".format(dag_name=table_name),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##
def dwd_oride_passenger_estimate_records_di_sql_task(ds):
    hql = '''
    SET hive.exec.parallel=TRUE;
    SET hive.exec.dynamic.partition.mode=nonstrict;
    set hive.auto.convert.join = false;

INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
select  id,          --id
        user_id,     -- 用户id
        serv_type,   --订单车辆类型(1:driect 2: street 3.keke 4.car)
        city_id,   -- 所属城市
        start_lng,   -- 起点经度
        start_lat,   -- 起点纬度
        end_lng,   -- 终点经度
        end_lat,   -- 终点纬度
        duration,  -- 订单持续时间
        distance,  -- 订单距离
        basic_fare,  --起步价
        dst_fare,  --里程费
        dut_fare,  --时长费
        dut_price,   -- 时长价格
        dst_price,   -- 距离价格
        price,   -- 订单价格
        low_price,   -- 最低订单价格
        high_price,  --最高订单价格
        low_coupon_amount,   -- 最低优惠价格
        high_coupon_amount,  --最低优惠价格
        premium_id,  --溢价配置表id
        premium_rate,  --溢价倍数
        original_price,  --溢价前费用
        premium_price_limit,   -- 溢价金额上限
        premium_adjust_price,  --溢价金额
        premium_rate_max,  --溢价倍数上限
        premium_rate_warn,   -- 溢价预警值
        create_time,   -- 创建时间
        minimum_fare,  -- 最低消费
        discount,  --动态折扣(如:70，7折)
        discount_price_max,  --可享受折扣金额上限.)
        market_price,   --市场价
        local_create_time,  -- 当地创建时间
        'nal' as country_code,
        '{pt}' as dt
from (SELECT *,
             if(t.create_time=0,0,(t.create_time + 1 * 60 * 60)) as local_create_time,

             row_number() over(partition by t.id order by t.`__ts_ms` desc,t.`__file` desc,cast(t.`__pos` as int) desc) as order_by

        FROM oride_dw_ods.ods_binlog_base_data_user_estimate_records_hi t

        WHERE concat_ws(' ',dt,hour) BETWEEN '{bef_yes_day} 23' AND '{pt} 23' --取昨天1天数据与今天早上00数据

        AND from_unixtime((t.create_time + 1 * 60 * 60 * 1),'yyyy-MM-dd') = '{pt}'
         ) t1
where t1.`__deleted` = 'false' and t1.order_by = 1;
'''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        bef_yes_day=airflow.macros.ds_add(ds, -1),
        now_hour='{{ execution_date.strftime("%H") }}',
        table=table_name,
        db=db_name
    )
    return hql

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

    v_info = [
        {"table": "oride_db.oride_data.data_user_estimate_records", "start_timeThour": "{v_day}T00".format(v_day=v_day),
         "end_dateThour": "{v_day}T23".format(v_day=v_day), "depend_dir": "oss://opay-datalake/oride_binlog"}
    ]

    hcm = TaskHourSuccessCountMonitor(ds, v_info)

    hcm.HourSuccessCountMonitor()

    # 删除分区
    # cf.delete_partition()

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dwd_oride_passenger_estimate_records_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 生产success
    cf.touchz_success()


dwd_oride_passenger_estimate_records_di_task = PythonOperator(
    task_id='dwd_oride_passenger_estimate_records_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

ods_binlog_base_data_order_hi_prev_day_task >> dwd_oride_passenger_estimate_records_di_task