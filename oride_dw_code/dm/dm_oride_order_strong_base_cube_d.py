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
    'owner': 'lili.chen',
    'start_date': datetime(2019, 9, 25),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dm_oride_order_strong_base_cube_d',
                  schedule_interval="30 01 * * *",
                  default_args=args)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##


# 依赖前一天分区
dependence_dwd_oride_order_base_include_test_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_order_base_include_test_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
# 依赖前一天分区
dependence_dim_oride_driver_base_prev_day_task = UFileSensor(
    task_id='dim_oride_driver_base_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dim_oride_driver_base/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
# 依赖前一天分区
dependence_dwd_oride_order_dispatch_funnel_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_order_dispatch_funnel_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_dispatch_funnel_di/country_code=nal",
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
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "1800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)
##----------------------------------------- 变量 ---------------------------------------##

table_name = "dm_oride_order_strong_base_cube_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

dm_dm_oride_order_strong_base_cube_d_task = HiveOperator(

    task_id='dm_oride_order_strong_base_cube_d_task',
    hql='''set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
          select nvl(b.city_id,-10000) as city_id,
                 nvl(b.product_id,-10000) as product_id,
                 sum(a.strong_dispatch_driver_cnt) as strong_dispatch_driver_cnt, --强派单司机数
                 sum(a.strong_dispatch_order_cnt) as strong_dispatch_order_cnt, --强派单调度推单数
                 sum(b.finished_driver_cnt) as finished_driver_cnt, --完单司机数 
                 sum(b.strong_finished_driver_cnt) as strong_finished_driver_cnt, --强派单完单司机数
                 --sum(b.finish_driver_online_dur) as finish_driver_online_dur,--完单司机在线时长
                 --sum(b.strong_finish_driver_online_dur) as strong_finish_driver_online_dur,--强派单完单司机在线时长
                 sum(b.strong_finished_order_cnt) as strong_finished_order_cnt, --强派单完单数
                 sum(b.strong_finish_order_pick_up_dis) as strong_finish_order_pick_up_dis, --强派单完单接驾距离(米)
                 sum(b.strong_finish_order_pick_up_assigned_cnt) as strong_finish_order_pick_up_assigned_cnt, --强派单订单被分配次数（计算平均接驾距离使用）
                 sum(b.strong_user_cancel_order_cnt) as strong_user_cancel_order_cnt, --强派单乘客取消订单数
                 sum(b.strong_driver_cancel_order_cnt) as strong_driver_cancel_order_cnt, --强派单司机取消订单数
                 sum(b.strong_paid_order_cnt) as strong_paid_order_cnt, --强派单支付订单数
                 sum(b.strong_paid_price) as strong_paid_price,  --强派单应付金额
                 sum(b.strong_paid_amount) as strong_paid_amount,  --强派单实付金额
                 sum(c.push_show_ord_cnt) as push_show_ord_cnt, --push到达单数（派单）
                 sum(c.accept_show_ord_cnt) as accept_show_ord_cnt, --展示单数（派单）
                 sum(c.show_ord_cnt) as show_ord_cnt, --推送订单数（派单）
                 sum(c.accept_click_ord_cnt) as accept_click_ord_cnt, --接单数（派单）
                 'nal' as country_code,
                 '{pt}' as dt
                from 
                (SELECT nvl(ord.city_id,-10000) as city_id,
                nvl(if(ord.city_id=1001 and driver.product_id is not null,driver.product_id,ord.product_id),-10000) as product_id,
                count(distinct (if(ord.status in(4,5),ord.driver_id,null))) as finished_driver_cnt, --完单司机数
                count(distinct (if(ord.status in(4,5) and ord.is_strong_dispatch=1,ord.driver_id,null))) as strong_finished_driver_cnt, --强派单完单司机数
                count((if(ord.status in(4,5) and ord.is_strong_dispatch=1,ord.order_id,null))) as strong_finished_order_cnt, --强派单完单数
                sum(if(ord.is_td_finish=1 and ord.is_strong_dispatch=1,pick_up_distance,0)) as strong_finish_order_pick_up_dis, --完单接驾距离(米)
                sum(if(ord.is_td_finish=1 and ord.is_strong_dispatch=1,order_assigned_cnt,0)) as strong_finish_order_pick_up_assigned_cnt, --订单被分配次数（计算平均接驾距离使用）
                count(if(ord.status=6 and ord.cancel_role=1 and ord.is_strong_dispatch=1,ord.order_id,null)) as strong_user_cancel_order_cnt, --强派单乘客取消订单数
                count(if(ord.status=6 and ord.cancel_role=2 and ord.is_strong_dispatch=1,ord.order_id,null)) as strong_driver_cancel_order_cnt, --强派单司机取消订单数
                count(if(ord.status=5 and ord.is_strong_dispatch=1,ord.order_id,null)) as strong_paid_order_cnt, --强派单支付订单数
                sum(if(ord.status=5 and ord.is_strong_dispatch=1,ord.price,0.0)) as strong_paid_price,  --强派单应付金额
                sum(if(ord.status=5 and ord.is_strong_dispatch=1,ord.pay_amount,0.0)) as strong_paid_amount  --强派单实付金额
                from (select *
                   FROM oride_dw.dwd_oride_order_base_include_test_di
                   WHERE dt='{pt}'
                     AND city_id<>999001 --去除测试数据
                     AND driver_id<>1) ord
                 
                     left join
                     (SELECT  
                        order_id,
                        count(1) as order_assigned_cnt, --订单被分配次数（计算平均接驾距离使用）
                        sum(distance) AS pick_up_distance --接驾总距离
                        FROM oride_dw.dwd_oride_order_dispatch_funnel_di
                        WHERE dt='{pt}' and event_name='dispatch_assign_driver'
                        GROUP BY order_id) assign_ord
                        on ord.order_id=assign_ord.order_id
                      left join
                      (select * from oride_dw.dim_oride_driver_base   --判断业务线
                       where dt='{pt}') driver
                       on ord.driver_id=driver.driver_id
                     
                group by nvl(ord.city_id,-10000),
                nvl(if(ord.city_id=1001 and driver.product_id is not null,driver.product_id,ord.product_id),-10000)
                with cube) b
                left join
                (SELECT nvl(city_id,-10000) as city_id,nvl(product_id,-10000) as product_id,count(distinct driver_id) as strong_dispatch_driver_cnt, --强派单司机数
                count(distinct order_id) as strong_dispatch_order_cnt --强派单调度推单数
                   FROM oride_dw.dwd_oride_order_dispatch_funnel_di
                   WHERE dt='{pt}'
                     AND event_name='dispatch_push_driver'
                     AND assign_type=1
                     and city_id<>999001
                     group by nvl(city_id,-10000),nvl(product_id,-10000)
                     with cube) a
                on nvl(a.city_id,-10000)=nvl(b.city_id,-10000) and nvl(a.product_id,-10000)=nvl(b.product_id,-10000)
                left join 
                (SELECT nvl(city_id,-10000) AS city_id,
                       nvl(product_id,-10000) AS product_id,
                      -- nvl(country_code,-10000) AS country_code,
                       count(DISTINCT (if(event_name='order_push_show',order_id,NULL))) AS push_show_ord_cnt, --push到达单数（派单）
                       count(DISTINCT (if(event_name='accept_order_show',order_id,NULL))) AS accept_show_ord_cnt, --展示单数（派单）
                       count(DISTINCT (if(event_name IN('order_push_show','accept_order_show'),order_id,NULL))) AS show_ord_cnt, --推送订单数（派单）
                       count(DISTINCT (if(event_name='accept_order_click',order_id,NULL))) AS accept_click_ord_cnt --接单数（派单）
                
                FROM oride_dw.dwd_oride_driver_accept_order_funnel_di
                WHERE dt='{pt}'
                  AND isAssign=1
                GROUP BY nvl(city_id,-10000),
                         nvl(product_id,-10000)
                       --  nvl(country_code,-10000)
                WITH CUBE) c
                on nvl(a.city_id,-10000)=nvl(c.city_id,-10000) and nvl(a.product_id,-10000)=nvl(c.product_id,-10000)
                group by nvl(b.city_id,-10000),
                         nvl(b.product_id,-10000);
                 '''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
    ),
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

dependence_dwd_oride_order_base_include_test_di_prev_day_task >> \
dependence_dim_oride_driver_base_prev_day_task >> \
dependence_dwd_oride_order_dispatch_funnel_di_prev_day_task >> \
sleep_time >> \
dm_dm_oride_order_strong_base_cube_d_task >> \
touchz_data_success


