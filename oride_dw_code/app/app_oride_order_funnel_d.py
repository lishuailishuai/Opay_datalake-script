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
from airflow.sensors import OssSensor

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2019, 12, 10),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_order_funnel_d',
                  schedule_interval="00 02 * * *",
                  default_args=args)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "app_oride_order_funnel_d"

##----------------------------------------- 依赖 ---------------------------------------##
#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":
    # 依赖前一天分区
    dwm_oride_order_base_di_prev_day_task = UFileSensor(
        task_id='dwm_oride_order_base_di_prev_day_task',
        filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_order_base_di/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )


    dwm_oride_passenger_event_di_prev_day_task = UFileSensor(
        task_id='dwm_oride_passenger_event_di_prev_day_task',
        filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_passenger_event_di/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

else:
    print("成功")
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

    dwm_oride_passenger_event_di_prev_day_task = OssSensor(
        task_id='dwm_oride_passenger_event_di_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_passenger_event_di/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"dag":dag,"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)



##----------------------------------------- 脚本 ---------------------------------------##

def app_oride_order_funnel_d_sql_task(ds):
    HQL = '''
    SET hive.exec.parallel=true;
    SET hive.exec.dynamic.partition=true;
    SET hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table {db}.{table} partition(country_code,dt)
    select city_id,
       product_id,
       driver_serv_type,
       is_strong_dispatch,
       act_num, --乘客端打开页面活跃量
       choose_end_point_num, --地址选择次数
       valuation_num,  --估价次数,即所谓的冒泡数量
       order_cnt,  --下单量
       broadcast_ord_cnt,  --播单量
       succ_broadcast_ord_cnt,  --成功播单量
       driver_show_ord_cnt,  --司机端推送给骑手订单量
       accpet_show_ord_cnt,  --司机端前端show订单量
       request_ord_cnt,  --接单量，理论和骑手端司机应答量一致，由于骑手端click打点数据缺失，所以用订单表统计
       driver_arri_bef_cancel_cnt,  --司机到达接客点前取消量
       driver_arri_aft_cancel_cnt,  --司机到达接客点后取消量
       user_befreply_cancel_cnt,  --应答前乘客取消量
       user_aftreply_befarri_cancel_cnt,  --应答后-司机到达前乘客取消量
       user_aftreply_aftarri_cancel_cnt,  --应答后-司机到达后乘客取消量
       finish_ord_cnt,  --完单量
       finished_pay_ord_cnt,  --完成支付订单量   
       if(m.country_code='total','nal','nal') as country_code,
       '{pt}' as dt
    from (select nvl(ord.city_id,-10000) as city_id,
       nvl(ord.product_id,-10000) as product_id,
       nvl(ord.driver_serv_type,-10000) as driver_serv_type,
       nvl(ord.is_strong_dispatch,-10000) as is_strong_dispatch,
       user_event.act_num, --乘客端打开页面活跃量
       user_event.choose_end_point_num, --地址选择次数
       user_event.valuation_num,  --估价次数,即所谓的冒泡数量
       ord.order_cnt,  --下单量
       ord.broadcast_ord_cnt,  --播单量
       ord.succ_broadcast_ord_cnt,  --成功播单量
       ord.driver_show_ord_cnt,  --司机端推送给骑手订单量
       ord.accpet_show_ord_cnt,  --司机端前端show订单量
       ord.request_ord_cnt,  --接单量，理论和骑手端司机应答量一致，由于骑手端click打点数据缺失，所以用订单表统计
       ord.driver_arri_bef_cancel_cnt,  --司机到达接客点前取消量
       ord.driver_arri_aft_cancel_cnt,  --司机到达接客点后取消量
       ord.user_befreply_cancel_cnt,  --应答前乘客取消量
       ord.user_aftreply_befarri_cancel_cnt,  --应答后-司机到达前乘客取消量
       ord.user_aftreply_aftarri_cancel_cnt,  --应答后-司机到达后乘客取消量
       ord.finish_ord_cnt,  --完单量
       ord.finished_pay_ord_cnt,  --完成支付订单量   
       nvl(ord.country_code,'total') as country_code

        from (select country_code,
               city_id,
               product_id, 
               driver_serv_type,
               is_strong_dispatch,
               count(1) as order_cnt, --下单量
               sum(is_valid) as valid_order_cnt, --有效订单量
               sum(is_broadcast) as broadcast_ord_cnt,  --播单量
               sum(is_succ_broadcast) as succ_broadcast_ord_cnt,  --成功播单量
               sum(is_accpet_show) as driver_show_ord_cnt,  --司机端推送给骑手订单量
               sum(is_driver_accept_show) as accpet_show_ord_cnt,  --司机端前端show订单量
               sum(is_request) as request_ord_cnt,  --接单量，理论和骑手端司机应答量一致，由于骑手端click打点数据缺失，所以用订单表统计
               sum(if(is_driver_after_cancel=1 and is_arrive_receive_point=0,1,0)) as driver_arri_bef_cancel_cnt,  --司机到达接客点前取消量
               sum(if(is_driver_after_cancel=1 and is_arrive_receive_point=1,1,0)) as driver_arri_aft_cancel_cnt,  --司机到达接客点后取消量
               sum(is_passanger_before_cancel) as user_befreply_cancel_cnt,  --应答前乘客取消量
               sum(if(is_passanger_after_cancel=1 and is_arrive_receive_point=0,1,0)) as user_aftreply_befarri_cancel_cnt,  --应答后-司机到达前乘客取消量
               sum(if(is_passanger_after_cancel=1 and is_arrive_receive_point=1,1,0)) as user_aftreply_aftarri_cancel_cnt,  --应答后-司机到达后乘客取消量
               sum(is_finish) as finish_ord_cnt,  --完单量
               sum(is_finished_pay) as finished_pay_ord_cnt  --完成支付订单量
        from oride_dw.dwm_oride_order_base_di
        where dt='{pt}'     
        group by country_code,
               city_id,
               product_id, 
               driver_serv_type,
               is_strong_dispatch
        with cube) ord
        
        left join 
        
        (select 'total' as country_code,
               -10000 as city_id,
               -10000 as product_id,
               -10000 as driver_serv_type,
               -10000 as is_strong_dispatch,
               act_num,  --乘客端打开页面活跃量
               choose_end_point_num, --地址选择次数
               valuation_num  --估价次数,即所谓的冒泡数量
        from oride_dw.dwm_oride_passenger_event_di
        where dt='{pt}') user_event
        on nvl(ord.country_code,'total')=user_event.country_code 
        and nvl(ord.city_id,-10000)=user_event.city_id
        and nvl(ord.product_id,-10000)=user_event.product_id
        and nvl(ord.driver_serv_type,-10000)=user_event.driver_serv_type
        and nvl(ord.is_strong_dispatch,-10000)=user_event.is_strong_dispatch) m
        where m.country_code='total';
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
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
    cf = CountriesPublicFrame("false", ds, db_name, table_name, hdfs_path, "true", "true")

    # 删除分区
    cf.delete_partition()

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + app_oride_order_funnel_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 生产success
    cf.touchz_success()


app_oride_order_funnel_d_task = PythonOperator(
    task_id='app_oride_order_funnel_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

dwm_oride_order_base_di_prev_day_task >> app_oride_order_funnel_d_task
dwm_oride_passenger_event_di_prev_day_task >> app_oride_order_funnel_d_task
