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
    'owner': 'liushuzhen',
    'start_date': datetime(2019, 11, 07),
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

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
dependence_dwd_oride_client_event_detail_hi_prev_day_task = UFileSensor(
    task_id='dependence_dwd_oride_client_event_detail_hi_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_client_event_detail_hi",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
dependence_dwm_oride_order_base_di_prev_day_task = UFileSensor(
    task_id='dependence_dwm_oride_order_base_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwm_oride_order_base_di",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
dependence_ods_sqoop_base_data_order_payment_di_prev_day_task = UFileSensor(
    task_id='dependence_ods_sqoop_base_data_order_payment_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_order_payment",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
dependence_ods_sqoop_base_data_user_comment_df_prev_day_task = UFileSensor(
    task_id='dependence_ods_sqoop_base_data_user_comment_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_user_comment",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
##----------------------------------------- 变量 ---------------------------------------##

table_name = "app_oride_passenger_funnel_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

app_oride_passenger_funnel_d_task = HiveOperator(

    task_id='app_oride_passenger_funnel_d_task',
    hql='''
      with base as 
            (select user_id,event_name,event_value,country_code,event_time,
             get_json_object(event_value, '$.serv_type') AS product_id,
             get_json_object(event_value, '$.order_id') AS order_id
             from oride_dw.dwd_oride_client_event_detail_hi 
             where from_unixtime(cast(event_time as int),'yyyy-MM-dd')='{pt}' and dt='{pt}' and app_version>='4.4.405' and 
             event_name in('radar_map_show','successful_order_show','successful_order_cancelorder_click','rider_arrive_show',
                 'rider_arrive_cancelorder_click','start_ride_show',
                 'complete_the_order_show','complete_the_payment_show','oride_show','choose_end_point_click',
                 'request_a_ride_show','request_a_ride_click','searching_cancel_click')
            ),
          base1 as 
             (select order_id,passenger_id,country_code,is_succ_broadcast,--是否播单
               is_driver_after_cancel,--应答后司机是否取消
               cannel_pick_order_dur,--取消时长
               pick_up_distance,--接驾总距离
               order_assigned_cnt,--订单被分配次数
               order_onride_distance, --送架距离
               user_order_total_dur --下单到完单时长
             from oride_dw.dwm_oride_order_base_di where dt='{pt}'
             )
    INSERT overwrite TABLE oride_dw.{table} partition(dt='{pt}')
    select m.country_code,m1.product_id,a,b,c,d,e,f,j,h,i,j,k,l,m,n,o,p,q,m2.broadcast,j2,j3,l1,p1,m3.q1,m3.q2,m1.q3
from 
   (select country_code,
       sum(case when event_name='oride_show' then u else 0 end) a ,--活跃乘客数
       sum(case when event_name='choose_end_point_click' then u else 0 end) b,--点击选择终点
       sum(case when event_name='request_a_ride_show' then u else 0 end) c,--估价人数
       sum(case when event_name='request_a_ride_show' then c else 0 end) d,--估价次数
       sum(case when event_name='request_a_ride_click' then u else 0 end) e,--下单人数
       sum(case when event_name='request_a_ride_click' then c else 0 end) f,--下单次数
       sum(case when event_name='searching_cancel_click' then u else 0 end)g --应答前取消订单
      
    from 
       (select country_code,event_name,count(1) c,count(distinct user_id) u from 
           (select user_id,event_name,event_value,country_code
             from base
              where event_name in ('oride_show',
                         'choose_end_point_click',
                         'request_a_ride_show', 
                         'request_a_ride_click',
                         'searching_cancel_click'
                      )
             )mm 
         group by country_code,event_name) mmm
    group by country_code
   )m
left join 

  (select country_code,product_id,count(distinct user_id) h,--接单乘客数
       count(distinct case when xd is not null or xd='' then user_id end) i,--应答后取消乘客数
       avg(xd_t) j,--`应答后用户平均取消时长`,
       count(distinct case when is_driver_after_cancel='1' then user_id end) j2,--应答后司机取消乘客数
       avg(case when is_driver_after_cancel='1' then cannel_pick_order_dur end)j3,--应答后司机平均取消时长
       avg(sj_t) k,--`司机平均到达上车点时长`,
       avg(sc_t) l,--`司乘互找时长`,
       avg(pick_up_distance/order_assigned_cnt) l1,--平均接驾距离
       count(distinct case when time5 is not null or time5='' then user_id end) m,--完成打车订单乘客数
       count(distinct case when time6 is not null or time6='' then user_id end) n,--支付成功乘客数
       sum(price) o,--应付金额
       sum(amount) p, --实付金额
       avg(order_onride_distance) p1,--平均送架距离
       count(distinct case when score is not null or score='' then user_id end) q, --评价乘客数
       avg(user_order_total_dur) q3--下单到完单时长
  from 
      ( select  b.user_id,b.order_id,b.country_code,a.product_id,b.event_time time1, --接单时间
                 COALESCE(c.event_time,dd.event_time) xd,--应答后用户取消
                 COALESCE(c.event_time,dd.event_time)-b.event_time xd_t,--应答后用户取消时长
                 d.event_time-b.event_time sj_t,--司机到达上车点时长
                 e.event_time-d.event_time sc_t,--司乘互找时长
                 f.event_time time5,--完成打车订单
                  j.event_time time6, --支付成功
                  h.price,  --应付金额
                  h.amount,--实付金额
                  i.score,--订单评分
                  o.is_driver_after_cancel,--应答后司机是否取消
                  o.cannel_pick_order_dur,--取消时长
                  o.pick_up_distance,--接驾总距离
                  o.order_assigned_cnt,--订单被分配次数
                  o.order_onride_distance, --送架距离
                  o.user_order_total_dur --下单到完单时长
        from 
           (select order_id,min(product_id) product_id from base where event_name='radar_map_show' group by order_id) a  --雷达显示
        join 
           (select order_id,min(event_time) event_time,min(user_id) user_id,min(country_code) country_code,
              min(product_id) product_id from base where event_name='successful_order_show' group by order_id) b --打车成功
        on a.order_id=b.order_id
        left join 
           (select order_id,min(event_time) event_time from base where event_name='successful_order_cancelorder_click' group by order_id) c --应答后到达前取消
        on b.order_id=c.order_id
        left join 
           (select order_id,min(event_time) event_time from base where event_name='rider_arrive_show' group by order_id) d --骑手到达乘客上车点
        on b.order_id=d.order_id
        left join 
           (select order_id,min(event_time) event_time from base where event_name='rider_arrive_cancelorder_click' group by order_id) dd --取消订单
        on b.order_id=dd.order_id
        left join 
           (select order_id,min(event_time) event_time from base where event_name='start_ride_show' group by order_id) e --行程开始
        on b.order_id=e.order_id
        left join 
           (select order_id,min(event_time) event_time from base where event_name='complete_the_order_show' group by order_id) f --完成打车订单
        on b.order_id=f.order_id
        left join 
           (select order_id,min(event_time) event_time from base where event_name='complete_the_payment_show' group by order_id) j --支付完成
        on b.order_id=j.order_id
        left join 
           (select * from oride_dw_ods.ods_sqoop_base_data_order_payment_di where dt='{pt}') h 
        on b.order_id=h.id
        left join 
           (select * from oride_dw_ods.ods_sqoop_base_data_user_comment_df where dt='{pt}') i
        on b.order_id=i.order_id
        left join 
           base1 o
        on b.order_id=o.order_id
       )n 
     group by country_code,product_id
     )m1
  on m.country_code=m1.country_code
 left join 
     (select country_code,count(distinct passenger_id) broadcast --未播单人数
      from base1 where is_succ_broadcast='0'
      group by country_code
      ) m2
  on m.country_code=m2.country_code
  left join 
     (select country_code,
             avg(case when gj_time-ac_time < 15*60 then gj_time-ac_time end) q1,--登陆到估价时长
             avg(case when gj_time-zd_time <15*60 then gj_time-zd_time end)q2 --选择终点到估价时长
      from 
          (select country_code,user_id,min(case when event_name='oride_show' then event_time end) ac_time,
               min(case when event_name='choose_end_point_click' then event_time end) zd_time,
               min(case when event_name='request_a_ride_show' then event_time end) gj_time
           from base where event_name in('oride_show','choose_end_point_click','request_a_ride_show')
           group by country_code,user_id) qqq
     group by country_code
    )m3
   on m.country_code=m3.country_code;

'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
    ),
    schema='oride_dw',
    dag=dag)


# 生成_SUCCESS
touchz_data_success = BashOperator(

    task_id='touchz_data_success',

    bash_command="""
    line_num=`$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk '{{print $1}}'`

    if [ $line_num -eq 0 ]
    then
        echo "FATAL {hdfs_data_dir} is empty"
        $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
    else
        echo "DATA EXPORT Successed ......"
        $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
    fi
    """.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        hdfs_data_dir=hdfs_path + '/dt={{ds}}'
    ),
    dag=dag)

dependence_dwd_oride_client_event_detail_hi_prev_day_task >> \
dependence_dwm_oride_order_base_di_prev_day_task >> \
dependence_ods_sqoop_base_data_order_payment_di_prev_day_task >> \
dependence_ods_sqoop_base_data_user_comment_df_prev_day_task >> \
sleep_time >> \
app_oride_passenger_funnel_d_task >> \
touchz_data_success
