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
from airflow.sensors import OssSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from plugins.CountriesPublicFrame import CountriesPublicFrame
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'lijialong',
    'start_date': datetime(2020, 3, 4),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_finance_revenue_monitor_d',
                  schedule_interval="30 2 * * *",
                  default_args=args,
                  )
##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "app_oride_finance_revenue_monitor_d"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 依赖 ---------------------------------------##

depedence_dwm_oride_order_base_di_prev_day_task = OssSensor(
    task_id='depedence_dwm_oride_order_base_di_prev_day_task',
    bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwm_oride_order_base_di",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dependence_dwm_oride_driver_finance_di_prev_day_task = OssSensor(
    task_id='dependence_dwm_oride_driver_finance_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwm_oride_driver_finance_di/country_code=NG",
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
        {"dag":dag,"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)



##----------------------------------------- 脚本 ---------------------------------------##
def app_oride_finance_revenue_monitor_d_sql_task(ds):
    HQL = '''

      SET hive.exec.parallel=TRUE;
      set hive.exec.dynamic.partition.mode=nonstrict; 
      
      insert overwrite table oride_dw.{table} partition(country_code,dt)

      SELECT
          ord.city_id,--城市id
          
          ord.product_id,--业务线ID
          
          ord.online_pay_price + ord.falsify + ord.falsify_driver_cancel as gmv,
          
          ord.c_subsidy_d, --C端补贴
          
          finance.b_subsidy_d,--B端补贴
          
          ord.platform_commission, --平台抽成
          
          (abs(ord.falsify) + abs(ord.falsify_driver_cancel) + abs(ord.malice_brush_driver_deduct) 
            - abs(ord.falsify_get) -abs(ord.falsify_get_driver_cancel) - abs(ord.malice_brush_user_reward) 
            + finance.complain_amount) as penalty_income  , --罚款收入(罚款 + 取消)
        
          finance.repair_amount,--补录份子钱
          
          finance.other_amount, --财务小项
          
          finance.amount_agenter,--分子钱(product_id =1 摩托车份子钱收入)
          
          finance.theory_phone_amount, --手机还款(理论)
          
          ord.finished_pay_driver_price,--完成支付订单的司机收入
          
          ord.finished_pay_driver_cnt,--完成支付的司机数量
          
          ord.finished_pay_order_price,--完成支付的订单价格
          
          ord.finished_pay_order_cnt,--完成支付的订单数量
          
          ord.country_code,
          
          ord.dt
        
        from
        (
          select  
            city_id,
            product_id,
            sum(if(is_finished_pay=1 and is_succ_pay=1  and pay_mode not in(0,1),nvl(price,0)+nvl(tip,0)+nvl(surcharge,0)+nvl(pax_insurance_price,0),0)) as online_pay_price,--线上支付成功金额
            
            sum(nvl(falsify,0)) as falsify, --用户罚款，自12.25号开始该表接入
            
            sum(nvl(falsify_driver_cancel,0)) as falsify_driver_cancel, --司机罚款，自12.25号开始该表接入
            
            sum(if(is_finished_pay=1 and is_succ_pay=1 and pay_mode not in(0,1) ,nvl(price,0)+nvl(tip,0)+nvl(surcharge,0)+nvl(pax_insurance_price,0),0)) as c_subsidy_d,--c端补贴
            
            sum(nvl(pay_amount,0)+nvl(coupon_amount,0)-nvl(driver_price,0)) as platform_commission, --平台抽成
        
            sum(nvl(malice_brush_driver_deduct,0)) as malice_brush_driver_deduct,
        
            sum(nvl(falsify_get,0)) as  falsify_get,
        
            sum(nvl(falsify_get_driver_cancel,0)) as falsify_get_driver_cancel,
        
            sum(nvl(malice_brush_user_reward,0)) as malice_brush_user_reward,
            
            sum(if(status = 5,driver_price,0)) as finished_pay_driver_price ,--完成支付订单的司机收入
            
            count(distinct if(status = 5 and (driver_id != 1 or driver_id!=0),driver_id,null)) as finished_pay_driver_cnt, --完成支付的司机数量
            
            sum(if(status = 5,price,0)) as finished_pay_order_price ,--完成支付的订单价格
            
            count(distinct if(status = 5,order_id,null)) as finished_pay_order_cnt ,--完成支付的订单数量
            
            country_code,
            
            dt  
          from oride_dw.dwm_oride_order_base_di 
          where dt = '{pt}' and city_id != 99901 and product_id <>99
          group by city_id,product_id,dt,country_code
        )ord
        left join
        (
          select 
              city_id,
              product_id,
              sum(nvl(recharge_amount,0) + nvl(reward_amount,0)) as b_subsidy_d,--B端补贴、天(实际b补)
              sum(nvl(complain_amount,0)) as complain_amount,  --司机被投诉罚款
              sum(nvl(repair_amount,0)) as  repair_amount, -- 补录份子钱
              sum(nvl(other_amount,0)) as  other_amount,--财务小项
              sum(if(balance>0 and amount_agenter>0,nvl(amount_agenter,0),0)) as amount_agenter, --份子钱部分
              sum(if(balance>0,nvl(theory_phone_amount,0),0)) as theory_phone_amount, --手机还款(理论)
              dt
          from oride_dw.dwm_oride_driver_finance_di 
          where  dt = '{pt}'  and city_id != 999001 and driver_id <> 1
          group by city_id,product_id,dt
        ) finance  on ord.city_id = finance.city_id and ord.product_id = finance.product_id;   

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
    cf = CountriesPublicFrame("True", ds, db_name, table_name, hdfs_path, "true", "true")

    # 删除分区
    cf.delete_partition()

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + app_oride_finance_revenue_monitor_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 生产success
    cf.touchz_success()


app_oride_finance_revenue_monitor_d_task = PythonOperator(
    task_id='app_oride_finance_revenue_monitor_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

depedence_dwm_oride_order_base_di_prev_day_task >> app_oride_finance_revenue_monitor_d_task
dependence_dwm_oride_driver_finance_di_prev_day_task >> app_oride_finance_revenue_monitor_d_task




