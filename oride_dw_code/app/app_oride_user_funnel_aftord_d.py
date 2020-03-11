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
from airflow.sensors.s3_key_sensor import S3KeySensor
import json
import logging
from airflow.models import Variable
import requests
import os
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from airflow.sensors import OssSensor
from plugins.CountriesPublicFrame import CountriesPublicFrame

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2020, 3, 10),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_user_funnel_aftord_d',
                  schedule_interval="40 01 * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "oride_dw"
table_name = "app_oride_user_funnel_aftord_d"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 依赖 ---------------------------------------##

dwm_oride_order_base_di_prev_day_task = OssSensor(
    task_id='dwm_oride_order_base_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwm_oride_order_base_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


# ----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {
            "dag": dag, "db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
            "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "1200"
        }
    ]
    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def app_oride_user_funnel_aftord_d_sql_task(ds):
    HQL = '''
    SET hive.exec.parallel=TRUE;
    SET hive.exec.dynamic.partition.mode=nonstrict;
    set hive.mapred.mode=nonstrict;

    insert overwrite table {db}.{table} partition(country_code,dt)
    select city_id, --城市ID
           product_id, --业务线ID
           count(distinct passenger_id) as ord_user_num, --下单乘客数   
           count(distinct (if(is_request=1,passenger_id,null))) as requested_user_num, --被接单乘客量
           count(distinct (if(is_finish=1,passenger_id,null))) as finish_user_num, --完单乘客量  
           count(distinct (if(is_finished_pay=1,passenger_id,null))) as finished_pay_user_num, --支付完单乘客量  
           count(1) as ord_cnt, --下单量
           sum(is_accpet_show) as push_driver_order_accpet_show_cnt,
           -- 司机被推送订单数       两种展示方式都包包含
           sum(is_request) as request_order_cnt,
           --接单量或应答订单量
           
           sum(is_finish) as finish_ord_cnt, --完单量
           sum(is_finished_pay) as finished_pay_ord_cnt,
           --完成支付订单量
           sum(if(is_finished_pay=1 and is_succ_pay=1,1,0)) as finish_pay, 
           --当天成功完成支付，用于统计单均应付和单均实付
           sum(is_passanger_before_cancel) as user_cancel_before_reply_cnt,
           --应答前乘客取消量
           
           sum(if(is_driver_after_cancel=1 and is_arrive_receive_point=0,1,0)) as driver_arri_bef_cancel_cnt,  --应答后-司机到达接客点前取消量
           sum(if(is_driver_after_cancel=1 and is_arrive_receive_point=1,1,0)) as driver_arri_aft_cancel_cnt,  --应答后-司机到达接客点后取消量
           sum(if(is_passanger_after_cancel=1 and is_arrive_receive_point=0,1,0)) as user_aftreply_befarri_cancel_cnt,  --应答后-司机到达前乘客取消量
           sum(if(is_passanger_after_cancel=1 and is_arrive_receive_point=1,1,0)) as user_aftreply_aftarri_cancel_cnt,  --应答后-司机到达后乘客取消量 
           
           sum(if(is_passanger_before_cancel=1 and ord_to_cancel_dur>0 and ord_to_cancel_dur<3600,ord_to_cancel_dur,0)) as user_cancel_before_reply_dur,
           --司机应答前乘客取消时长
           
           sum(if(take_order_dur>0 and take_order_dur<3600,take_order_dur,0)) as take_order_dur,   
           --应答订单时长  
           sum(if(is_request=1 and driver_arrive_car_point_dur>0 and driver_arrive_car_point_dur<3600,driver_arrive_car_point_dur,0)) as driver_arrive_car_point_dur,
           --司机到达上车点时长
           
           sum(if(is_request=1 and wait_order_dur>0 and wait_order_dur<3600,wait_order_dur,0)) as wait_order_dur,
           --当天等待上车订单时长,司乘互找总时长
              
           sum(if(is_finish=1 and ord_to_arrive_dur>0 and ord_to_arrive_dur<5*3600,ord_to_arrive_dur,0)) as ord_to_arrive_dur,
           --下单送达总时长
           
           sum(if(is_finish=1 and order_service_dur>0 and order_service_dur<5*3600,order_service_dur,0)) as order_service_dur, 
           --订单服务时长（秒）
           sum(request_order_distance_inpush) as request_order_distance_inpush,
           --抢单阶段接驾距离(应答)
           
           sum(if(is_finish=1,order_onride_distance,0)) as order_onride_distance,
           --完单送驾距离
          
           sum(if(is_finished_pay=1 and is_succ_pay=1,nvl(price,0)+nvl(tip,0)+nvl(surcharge,0)+nvl(pax_insurance_price,0),0)) as pay_price, 
           --当日应付金额
           
           sum(if(is_finished_pay=1 and is_succ_pay=1,pay_amount,0)) as pay_amount, 
           -- 当日实付金额
           
           sum(if(score is not null,1,0)) as user_evaluation_order_cnt ,
           --乘客评价订单数量
           
           sum(if(score in(1,2),1,0)) as bad_feedback_finish_ord_cnt,
            -- 差评完单量,有评价的肯定是完单，但是完单了不一定评价
           country_code as country_code,
           '{pt}' as dt
    from oride_dw.dwm_oride_order_base_di 
    where dt='{pt}'
    group by city_id, --城市ID
             product_id, --业务线ID
             country_code;
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
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

    cf = CountriesPublicFrame("true", ds, db_name, table_name, hdfs_path, "true", "true")

    # 删除分区
    cf.delete_partition()

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + app_oride_user_funnel_aftord_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 生产success
    cf.touchz_success()


app_oride_user_funnel_aftord_d_task = PythonOperator(
    task_id='app_oride_user_funnel_aftord_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

dwm_oride_order_base_di_prev_day_task >> app_oride_user_funnel_aftord_d_task
