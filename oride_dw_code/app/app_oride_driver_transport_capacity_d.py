# -*- coding: utf-8 -*-
"""司机运力日报"""

import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook
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
from airflow.sensors.s3_key_sensor import S3KeySensor
import json
import logging
from airflow.models import Variable
import requests
import os
from airflow.sensors import OssSensor


args = {
    'owner': 'lijialong',
    'start_date': datetime(2019, 12,16),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_driver_transport_capacity_d',
                  schedule_interval="30 1 * * *",
                  default_args=args)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "app_oride_driver_transport_capacity_d"

##----------------------------------------- 依赖 ---------------------------------------##
#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":
# 依赖前一天分区
    dwm_oride_driver_base_df_prev_day_task = UFileSensor(
        task_id='dwm_oride_driver_base_df_prev_day_task',
        filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_driver_base_df/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )


    dim_oride_city_task = UFileSensor(
        task_id='dim_oride_city_task',
        filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dim_oride_city/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    #依赖前一天分区
    dwd_oride_order_pay_detail_di_prev_day_task=UFileSensor(
        task_id='dwd_oride_order_pay_detail_di_prev_day_task',
        filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_order_pay_detail_di/country_code=NG",
            pt='{{ds}}'
            ),
        bucket_name='opay-datalake',
        poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
 #路径
    hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name
else:
    print("成功")
    # 依赖前一天分区
    dwm_oride_driver_base_df_prev_day_task = OssSensor(
        task_id='dwm_oride_driver_base_df_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_driver_base_df/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    dim_oride_city_task = OssSensor(
        task_id='dim_oride_city_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dim_oride_city/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    # 依赖前一天分区
    dwd_oride_order_pay_detail_di_prev_day_task = OssSensor(
        task_id='dwd_oride_order_pay_detail_di_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_order_pay_detail_di/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    # 路径
    hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"dag":dag,"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "2400"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def app_oride_driver_transport_capacity_d_sql_task(ds):
    HQL = '''
    SET hive.exec.parallel=TRUE;
    set hive.exec.dynamic.partition.mode=nonstrict;
    
 
 insert overwrite table oride_dw.{table}  partition(country_code,dt)
  select
    driver_base.city_id,
    nvl(city.city_name,'all') as city_name,
    driver_base.product_id,
    nvl(driver_base.succ_push_times / driver_base.succ_broadcast_driver_num,0)  as push_driver_times_avg ,----人均推单次数
    nvl(driver_base.succ_push_order_cnt / driver_base.succ_broadcast_driver_num,0) as push_driver_order_avg,--人均推送订单数
    nvl(driver_base.driver_finish_online_dur / 3600 ,0 )  as driver_finish_online_dur, --总在线时长(完单司机在线时长, 小时)
    nvl(driver_base.driver_finish_online_dur /(3600 * driver_base.finish_driver_cnt),0) as  avg_driver_finish_online_dur, --人均在线时长(完单司机人均在线时长,小时)
    nvl(driver_base.finish_billing_dur * 100 / driver_base.driver_finish_online_dur ,0) as billing_dur_rate, --计费时长占比： 完单司机计费时长 / 总在线时长(完单司机在线时长)
    nvl(driver_base.finish_order_cnt / driver_base.finish_driver_cnt,0) as avg_finish_driver_order, --司机人均完单数
    nvl(driver_base.online_driver_cnt,0) as online_driver_cnt, --在线司机数
    nvl(driver_base.request_driver_cnt,0) as request_driver_cnt, --接单司机数
    nvl(driver_base.finish_driver_cnt,0) as finish_driver_cnt,--完单司机数
    nvl(driver_base.register_driver_num,0) as register_driver_num ,--注册司机数(注册成功的司机数)
    nvl(driver_base.register_and_finish_driver_num,0) as register_and_finish_driver_num,--注册且完单司机数(注册成功且完单的司机数)
    nvl(driver_base.agg_register_driver_num,0) as agg_register_driver_num,--累计注册司机数(注册成功的司机数)
    nvl(driver_base.agg_finish_driver_cnt,0) as agg_finish_driver_cnt , --累计完单司机数
    nvl(driver_base_seven.seven_day_finish_driver_cnt,0) as seven_day_finish_driver_cnt,  --近7日完单司机数
    nvl(round(price_sum /order_pay_num,2),0) order_price_avg, --单均应付 应付金额/支付订单数
    nvl(round(pay_amount_sum / order_pay_num,2),0) order_amount_avg, --实付金额 实付金额/支付订单数
    if(finish_driver_cnt >0, round(amount_all / finish_driver_cnt,2), 0)as avg_driver_amount, --司机人均收入
    'NG' as country_code,
    '{pt}' as dt 
from 
(
    select 
        nvl(city_id,-10000) as city_id,
        product_id,
        sum(succ_push_times) as succ_push_times,--成功被播单总次数(订单不去重)
        sum(succ_push_order_cnt)  as succ_push_order_cnt ,--成功被播单量(订单去重)
        count(if(is_td_succ_broadcast = 1,driver_id,null)) as succ_broadcast_driver_num, --成功播单的司机数(driver不去重)
        sum(driver_finish_online_dur) as driver_finish_online_dur, --完单司机在线时长 --driver_onlinerange_sum
        count(distinct if(is_td_finish = 1 ,driver_id,null)) as finish_driver_cnt, --完单司机数  -- onride_driver_num
        sum(if(is_td_finish = 1,driver_billing_dur ,0)) as  finish_billing_dur,  --计费时长 / 在线时长
        sum(driver_finish_order_cnt) as finish_order_cnt, --完单量
        sum(is_td_online) as online_driver_cnt, --在线司机数
        count(distinct if(is_td_request =1 ,driver_id,null)) as request_driver_cnt, --接单司机数
        count(if(date_format(register_time,'yyyy-MM-dd') = '{pt}',driver_id,null)) as register_driver_num,--注册成功司机数(应是注册成功的司机数)
        count(if(date_format(register_time,'yyyy-MM-dd') = '{pt}' and is_td_finish = 1 ,driver_id,null)) as register_and_finish_driver_num,--注册成功且完单司机数(应是注册成功的完单司机数)
        count(driver_id) as agg_register_driver_num,--累计注册成功司机数(应是注册成功的司机数)
        count(if(first_finish_order_id is not null,driver_id,null)) as agg_finish_driver_cnt,--累计完单司机数
        sum(if(is_td_finish = 1,amount_all,0))  as amount_all,--司机总收入 (完单司机总收入)
        country_code,
        dt
    from oride_dw.dwm_oride_driver_base_df
    where dt = '{pt}' and city_id <> '999001'
    group by city_id,product_id,country_code,dt
    grouping sets(product_id,(city_id,product_id,country_code,dt))
)driver_base
left join
(
    select 
        nvl(city_id,-10000) as city_id,
        product_id,
        count(distinct if(is_td_finish = 1 ,driver_id,null)) as seven_day_finish_driver_cnt  --近7日完单司机数   
    from oride_dw.dwm_oride_driver_base_df
    where dt >= date_sub('{pt}',7) and dt <= '{pt}' and city_id <> '999001'
    group by city_id,product_id
    grouping sets(product_id,(city_id,product_id))
)driver_base_seven on driver_base.city_id = driver_base_seven.city_id and  driver_base.product_id = driver_base_seven.product_id
left join
(
    select 
        nvl(city_id,-10000) as city_id,
        product_id,
        count(order_id) as order_pay_num, --支付订单数
        sum(price) as price_sum,--应付总金额
        sum(pay_amount) as pay_amount_sum --实付总金额
    from  oride_dw.dwd_oride_order_pay_detail_di
    where dt = '{pt}' and city_id is not null
    group by city_id,product_id
    grouping sets(product_id,(city_id,product_id))

)ord_pay on driver_base.city_id = ord_pay.city_id and  driver_base.product_id = ord_pay.product_id
left join 
(
    select 
        city_id,
        city_name,
        dt
    from oride_dw.dim_oride_city
    where dt = '{pt}'
)city on driver_base.city_id = city.city_id
    
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_oride_driver_transport_capacity_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    # check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


app_oride_driver_transport_capacity_d_task = PythonOperator(
    task_id='app_oride_driver_transport_capacity_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

# dependence_dwm_oride_order_base_di_task >> dependence_dim_oride_city_task >> dependence_dim_oride_passenger_base_task >> \
# dependence_dim_oride_driver_base_task >> dependence_dwd_oride_order_finance_df_task >> dependence_dwd_oride_driver_records_day_df_task >> \
# dependence_dwd_oride_driver_recharge_records_df_task >> dependence_dm_oride_driver_base_task>>dependence_dm_oride_passenger_base_cube_d_task>>\
# dependence_dwd_oride_order_base_include_test_di_task>>app_oride_order_global_operate_to_mysql_d_task


dwm_oride_driver_base_df_prev_day_task >> app_oride_driver_transport_capacity_d_task
dim_oride_city_task >> app_oride_driver_transport_capacity_d_task
dwd_oride_order_pay_detail_di_prev_day_task >> app_oride_driver_transport_capacity_d_task
