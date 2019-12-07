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
from plugins.CountriesPublicFrame import CountriesPublicFrame
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

dag = airflow.DAG( 'dwd_oride_finance_driver_repayment_extend_df', 
    schedule_interval="30 02 * * *", 
    default_args=args,
    catchup=False) 


sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 10',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------## 

#依赖前一天分区
ods_sqoop_base_data_driver_balance_extend_df_prev_day_tesk = UFileSensor(
    task_id='ods_sqoop_base_data_driver_balance_extend_df_prev_day_tesk',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_balance_extend",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
) 



#依赖前一天分区
ods_sqoop_base_data_driver_records_day_df_prev_day_tesk=UFileSensor(
    task_id="ods_sqoop_base_data_driver_records_day_df_prev_day_tesk",
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_records_day",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态 
    dag=dag
    )


#依赖前一天分区
ods_sqoop_base_data_driver_recharge_records_df_prev_day_tesk=UFileSensor(
    task_id="ods_sqoop_base_data_driver_recharge_records_df_prev_day_tesk",
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_recharge_records",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态 
    dag=dag
    )


#依赖前一天分区
ods_sqoop_base_data_driver_repayment_df_prev_day_tesk=UFileSensor(
    task_id="ods_sqoop_base_data_driver_repayment_df_prev_day_tesk",
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_repayment",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
    dag=dag
    )


# 依赖前一天分区
dim_oride_driver_base_prev_day_task = UFileSensor(
    task_id='dim_oride_driver_base_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dim_oride_driver_base/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
) 

##----------------------------------------- 变量 ---------------------------------------## 

table_name="dwd_oride_finance_driver_repayment_extend_df"
hdfs_path="ufile://opay-datalake/oride/oride_dw/"+table_name

##----------------------------------------- 任务超时监控 ---------------------------------------## 

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    tb = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "2400"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------## 

def dwd_oride_finance_driver_repayment_extend_df_sql_task(ds):

    hql='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)

SELECT dri.city_id,
       city_name,
       --城市名称

       dri.driver_id,
       --司机ID(非专车有手机的骑手)

       dri.driver_name,
       --司机名称

       dri.phone_number,
       --手机号

       dri.product_id,
       --司机类型

       (CASE
            WHEN dri.product_id=1 THEN ogreen.total_ * ogreen.amount_service
            ELSE ostreet.total_ * ostreet.amount_service
        END) AS repayment_all,
       --还款总额

       (CASE
            WHEN dri.product_id=1 THEN ogreen.total_
            ELSE ostreet.total_
        END) AS numbers,
       --还款次数(分期总数)

       (CASE
            WHEN dri.product_id=1 THEN ogreen.first_
            ELSE ostreet.first_
        END) AS start_date,
       --开始日期

       (CASE
            WHEN dri.product_id=1 THEN ogreen.last_
            ELSE ostreet.last_
        END) AS last_date,
       --最后还款

       (CASE
            WHEN dri.product_id=1 THEN ogreen.amount_service
            ELSE ostreet.amount_service
        END) AS repayment_amount,
       --还款金额

       (CASE
            WHEN bal.balance IS NULL THEN 0
            ELSE bal.balance
        END) AS balance,
       --余额

       (CASE
            WHEN dri.product_id=1 THEN nvl(ogreen.settled_,0)
            ELSE nvl(ostreet.settled_,0)
        END) AS settled_numbers,
       --还款期数

       nvl(oer.overdue_payment_cnt,0) AS overdue_payment_cnt,
       --违约期数

       nvl((CASE
            WHEN ((dri.product_id=1
                   AND ogreen.settled_=ogreen.total_)
                  OR (dri.product_id=2
                      AND ostreet.settled_=ostreet.total_)) THEN 0
            ELSE 1
        END),0) AS status,
       --0还完 1未还完

       dri.fault,
       --骑手状态

       dri.plate_number,
       --车牌号

       dri.register_time,
       --司机注册时间

       nvl(aud.address,-1) as driver_address,
       --司机地址（-1 未知）

       nvl(avg1.last_week_daily_due,0) as last_week_daily_due,
       --上周日均应还款金额

       dri.country_code,
       --国家码字段

       '{pt}' AS dt
FROM  

  (SELECT *
   FROM oride_dw.dim_oride_driver_base
   WHERE dt='{pt}'
     AND city_id<>'999001') dri
LEFT OUTER JOIN --所有骑手的余额表

  (SELECT driver_id,
          balance--余额

   FROM oride_dw_ods.ods_sqoop_base_data_driver_balance_extend_df
   WHERE dt='{pt}') bal ON dri.driver_id=bal.driver_id
LEFT OUTER JOIN --专车司机扣款记录

  (SELECT driver_id,
          from_unixtime(min(DAY)) AS first_,
          from_unixtime(max(DAY)) AS last_,
          count(1) AS settled_,
          365 AS total_ ,
          max(amount_service) AS amount_service
   FROM oride_dw_ods.ods_sqoop_base_data_driver_records_day_df
   WHERE amount_service>0
     AND dt='{pt}'
   GROUP BY driver_id) ogreen ON dri.driver_id = ogreen.driver_id
LEFT OUTER JOIN --快车司机扣款记录
  (SELECT ta.driver_id,
          from_unixtime(min(ta.created_at)) AS first_,
          from_unixtime(max(ta.created_at)) AS last_,
          count(1) AS settled_,
          min(tb.numbers) AS total_ ,
          abs(min(ta.amount)) AS amount_service
   FROM (select *
         FROM oride_dw_ods.ods_sqoop_base_data_driver_recharge_records_df
         WHERE dt = '{pt}'
           AND from_unixtime(updated_at,'yyyy-MM-dd HH')<='{now_day} 00'
           AND amount_reason=6
           AND amount<>0) ta
   LEFT OUTER JOIN
     (SELECT *
      FROM oride_dw_ods.ods_sqoop_base_data_driver_repayment_df
      WHERE dt='{pt}'
        AND substring(updated_at,1,13)<='{now_day} 00'
        AND repayment_type=0) tb ON ta.driver_id = tb.driver_id
   GROUP BY ta.driver_id ) ostreet ON dri.driver_id = ostreet.driver_id
LEFT OUTER JOIN --所有骑手的违约期数

  (SELECT driver_id,
          IF(INSTR(concat_ws('',collect_list(false_id)),'0')=0, LENGTH(concat_ws('',collect_list(false_id))), INSTR(concat_ws('',collect_list(false_id)),'0')-1) AS overdue_payment_cnt --违约期数
   FROM
     (SELECT driver_id,
             dt,
             (CASE WHEN balance<0 THEN '1' ELSE '0' END) AS false_id
      FROM oride_dw_ods.ods_sqoop_base_data_driver_balance_extend_df
      WHERE dt BETWEEN DATE_ADD('{pt}', -15) AND DATE_ADD('{pt}',0)
      ORDER BY driver_id,
               dt DESC) AS slack_det
   GROUP BY driver_id) oer ON dri.driver_id=oer.driver_id
LEFT OUTER JOIN
  (
    select * from oride_dw.dim_oride_driver_audit_base where dt='{pt}'
    and status=2
  ) aud
ON dri.driver_id=aud.driver_id
LEFT OUTER JOIN


(select 
  age.driver_id,
  sum(nvl(amount_agenter,0)+nvl(amount,0)) as fenzi, ----上周日均应还款金额分子
  sum(if((nvl(amount_agenter,0)+nvl(amount,0))>0,1,0)) as fenmu,   --上周日均应还款金额分母
  sum(nvl(amount_agenter,0)+nvl(amount,0))/sum(if((nvl(amount_agenter,0)+nvl(amount,0))>0,1,0)) as last_week_daily_due  --上周日均应还款金额
from
(SELECT driver_id,from_unixtime(day,'yyyy-MM-dd') as day,
        sum(amount_agenter) as amount_agenter
   FROM oride_dw_ods.ods_sqoop_base_data_driver_records_day_df
   WHERE dt='{pt}' and from_unixtime(day,'yyyy-MM-dd') BETWEEN DATE_ADD('{pt}', -6) AND DATE_ADD('{pt}',0)
   GROUP BY driver_id,from_unixtime(day,'yyyy-MM-dd')) age
LEFT OUTER JOIN 
(select driver_id,from_unixtime(created_at,'yyyy-MM-dd') as created_at,
sum(abs(amount)) as amount
         FROM oride_dw_ods.ods_sqoop_base_data_driver_recharge_records_df
         WHERE dt = '{pt}' and from_unixtime(created_at,'yyyy-MM-dd') BETWEEN DATE_ADD('{pt}', -6) AND DATE_ADD('{pt}',0)
           AND from_unixtime(updated_at,'yyyy-MM-dd HH')<='2019-11-12 00'
           AND amount_reason=6
           AND amount<>0 group by driver_id,from_unixtime(created_at,'yyyy-MM-dd')) tb1
ON age.driver_id = tb1.driver_id and age.day = tb1.created_at
group by age.driver_id
) avg1
on dri.driver_id=avg1.driver_id

'''.format(
        pt=ds,
        now_day=macros.ds_add(ds, +1),
        prev_6_day='{{macros.ds_add(ds, -6)}}',
        table=table_name
        )

 
#熔断数据，如果数据重复，报错
def check_key_data_task(ds):

    #主键重复校验
    HQL_DQC='''
    SELECT count(1)-count(distinct driver_id) as cnt
      FROM oride_dw.{table}
      WHERE dt='{pt}'
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name
        )
    return hql

    cursor = get_hive_cursor()
    logging.info('Executing 主键重复校验: %s', HQL_DQC)

    cursor.execute(HQL_DQC)
    res = cursor.fetchone()

    if res[0] >1:
        raise Exception ("Error The primary key repeat !", res)
    else:
        print("-----> Notice Data Export Success ......")
    


#主流程
def execution_data_task_id(ds,**kwargs):

    v_date=kwargs.get('v_execution_date')
    v_day=kwargs.get('v_execution_day')
    v_hour=kwargs.get('v_execution_hour')

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

    cf=CountriesPublicFrame("true",ds,db_name,table_name,hdfs_path,"true","true")

    #删除分区
    #cf.delete_partition()

    #读取sql
    _sql="\n"+cf.alter_partition()+"\n"+dwd_oride_finance_driver_repayment_extend_df_sql_task(ds)

    logging.info('Executing: %s',_sql)

    #执行Hive
    hive_hook.run_cli(_sql)

    #熔断数据，如果数据不能为0
    #check_key_data_cnt_task(ds)

    #熔断数据
    check_key_data_task(ds)

    #生产success
    cf.touchz_success()

    
dwd_oride_finance_driver_repayment_extend_df_task= PythonOperator(
    task_id='dwd_oride_finance_driver_repayment_extend_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date':'{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day':'{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour':'{{execution_date.strftime("%H")}}'
    },
    dag=dag
)


ods_sqoop_base_data_driver_balance_extend_df_prev_day_tesk>>dwd_oride_finance_driver_repayment_extend_df_task
ods_sqoop_base_data_driver_records_day_df_prev_day_tesk>>dwd_oride_finance_driver_repayment_extend_df_task
ods_sqoop_base_data_driver_recharge_records_df_prev_day_tesk>>dwd_oride_finance_driver_repayment_extend_df_task
ods_sqoop_base_data_driver_repayment_df_prev_day_tesk>>dwd_oride_finance_driver_repayment_extend_df_task
dim_oride_driver_base_prev_day_task>>dwd_oride_finance_driver_repayment_extend_df_task