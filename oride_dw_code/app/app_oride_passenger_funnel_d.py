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
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor

args = {
    'owner': 'chenghui',
    'start_date': datetime(2019, 11, 15),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_passenger_funnel_d',
                  schedule_interval="40 02 * * *",
                  default_args=args,
                  catchup=False)


##----------------------------------------- 依赖 ---------------------------------------##

dwd_oride_order_base_include_test_di_task = UFileSensor(
    task_id='dwd_oride_order_base_include_test_di_task',
    filepath='{hdfs_path_str}/country_code=NG/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)
dependence_dwm_oride_order_base_di_task = UFileSensor(
    task_id='dwm_oride_order_base_di_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwm_oride_order_base_di",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dim_oride_city_task = HivePartitionSensor(
    task_id="dim_oride_city_task",
    table="dim_oride_city",
    partition="dt='{{ds}}'",
    schema="oride_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

#----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {
            "db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
            "partition": "country_code=nal/dt={pt}".format(pt=ds),"timeout": "1200"
        }
    ]
    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor =  PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name= "oride_dw"
table_name = "app_oride_passenger_funnel_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

def app_oride_passenger_funnel_d_sql_task(ds):
    HQL='''
    SET hive.exec.parallel=TRUE;
    SET hive.exec.dynamic.partition.mode=nonstrict;
    
    insert overwrite table {db}.{table} partition(country_code,dt)
    select  if(city.city_name is not null,city.city_name,ord.region_name) region_name,
        ord.product_id,
        ord.submit_order_cnt,--下单量
        ord.submit_order_user_num,--下单乘客量
        ord.finish_pay_order_cnt,--完成支付订单量
        ord.driver_reply_before_user_cancel_cnt,--司机应答前乘客取消订单量
        ord.driver_reply_before_user_cancel_dur,--司机应答前到乘客取消订单之间的时长
        ord.reply_dur,--应答时长
        ord.driver_order_success_cnt,--司机接单成功的订单数量(应答订单)
        ord.receive_order_user_num,--接单乘客数
        ord.reply_after_user_cancel_dur,--应答后用户取消时长
        ord.reply_after_driver_cancel_dur,--应答后司机取消时长
        ord.reply_after_user_cancle_order_cnt,--应答后乘客取消的订单数
        ord.reply_after_driver_cancel_order_cnt,--应答后司机取消的订单数
        ord.driver_arrive_car_point_dur,--司机平均到达上车点时长
        ord.finish_order_user_num,--完单乘客数
        ord.finish_order_cnt,--完单量
        ord.pay_user_num,--支付乘客数
        ord.price,--应付金额
        ord.amount,--实付金额
        ord.order_delivery_dur,--下单送达时长
        bro.broadcast,--未播单量
        bro.order_onride_dis,--完单计费距离
        bro.pick_up_dis, --接驾距离
        bro.user_evaluation_order_cnt,--乘客评价订单量
        'nal' as country_code,
        '{pt}' as dt
    from
    (
        select
            if(country_code is not null,country_code,city_id) region_name,
            product_id,
            count(distinct order_id) as submit_order_cnt,--下单量
            count(distinct passenger_id)as submit_order_user_num, --下单乘客量
            count(distinct if(is_td_finish_pay=1,order_id,null)) as finish_pay_order_cnt,--完成支付订单量
            count(distinct if(is_td_passanger_before_cancel=1,order_id,null)) as driver_reply_before_user_cancel_cnt,  --司机应答前乘客取消订单量
            sum(if(status = 6 AND driver_id = 0 AND cancel_role = 1,cancel_time-create_time,0)) as driver_reply_before_user_cancel_dur,--司机应答前到乘客取消订单之间的时长
            sum(td_take_dur) as reply_dur, --应答时长
            count(distinct if(is_td_request=1,order_id,null)) as driver_order_success_cnt, --司机接单成功的订单数量(应答订单)
            count(distinct if(is_td_request=1,passenger_id,null)) as receive_order_user_num, --接单乘客数
            sum(td_passanger_after_cancel_time_dur) as reply_after_user_cancel_dur,--应答后用户取消时长
            sum(td_driver_after_cancel_time_dur)as reply_after_driver_cancel_dur,--应答后司机取消时长
            count(if(is_td_passanger_after_cancel=1,order_id,null))as reply_after_user_cancle_order_cnt,--应答后乘客取消的订单数
            count(distinct if(is_td_driver_after_cancel=1,order_id,null)) reply_after_driver_cancel_order_cnt,--应答后司机取消的订单数
            sum(td_pick_up_dur) as driver_arrive_car_point_dur,--司机平均到达上车点时长
            count(distinct if(is_td_finish=1,passenger_id,null)) as finish_order_user_num, --完单乘客数
            count(distinct if(is_td_finish=1,order_id,null)) as finish_order_cnt, --完单量
            count(distinct if(is_td_finish_pay=1,passenger_id,null)) pay_user_num, --支付乘客数
            sum(if(is_td_finish=1,price,0)) as price,--应付金额
            sum(if(is_td_finish=1,pay_amount,0)) as amount, --实付金额
            sum(if(is_td_finish=1,arrive_time-create_time,0)) as order_delivery_dur --下单送达时长
        from oride_dw.dwd_oride_order_base_include_test_di where dt='{pt}'
        group by country_code,city_id,product_id
        grouping sets((country_code,product_id),
            (city_id,product_id)
        )
    )as ord
    left join
    (
        select if(country_code is not null,country_code,city_id) region_name,
            product_id,
            nvl(avg(pick_up_distance/order_assigned_cnt),0) as pick_up_dis,--接驾总距离
            sum(if(is_finish=1,order_onride_distance,0)) as order_onride_dis,--完单计费距离(完成订单的司机开始行程到司机送达之间的距离)
            count(if(score is not null and is_finish=1,order_id,null)) as user_evaluation_order_cnt ,--乘客评价订单数量
            count(if(is_succ_broadcast=0,order_id,null)) broadcast --未播单量
        from  oride_dw.dwm_oride_order_base_di 
        where dt='{pt}'
        group by country_code,city_id,product_id
        grouping sets((country_code,product_id),
            (city_id,product_id)
        )
    ) as bro 
    on ord.region_name=bro.region_name and ord.product_id=bro.product_id
    left join
    (
        select city_id,city_name 
        from oride_dw.dim_oride_city
        where dt='{pt}'
    )as city
    on ord.region_name=city.city_id; 
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL

#主流程
def execution_data_task_id(ds,**kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_oride_passenger_funnel_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")

app_oride_passenger_funnel_d_task = PythonOperator(
    task_id='app_oride_passenger_funnel_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)


dwd_oride_order_base_include_test_di_task>>app_oride_passenger_funnel_d_task
dependence_dwm_oride_order_base_di_task>>app_oride_passenger_funnel_d_task
dim_oride_city_task>>app_oride_passenger_funnel_d_task