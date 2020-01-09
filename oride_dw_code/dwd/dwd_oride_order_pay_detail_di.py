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
from plugins.CountriesPublicFrame import CountriesPublicFrame
from airflow.sensors.s3_key_sensor import S3KeySensor
import json
import logging
from airflow.models import Variable
import requests
import os
from airflow.sensors import OssSensor


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

dag = airflow.DAG( 'dwd_oride_order_pay_detail_di', 
    schedule_interval="30 00 * * *",
    default_args=args,
    catchup=False) 


##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name="dwd_oride_order_pay_detail_di"

##----------------------------------------- 依赖 ---------------------------------------##
#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":
    # 依赖前一天分区

    ods_sqoop_base_data_order_payment_df_tesk = UFileSensor(
        task_id='ods_sqoop_base_data_order_payment_df_tesk',
        filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride_dw_sqoop/oride_data/data_order_payment",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    dwd_oride_order_base_include_test_di_tesk = S3KeySensor(
        task_id='dwd_oride_order_base_include_test_di_tesk',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-bi',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
#路径
    hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name
else:
    print("成功")
    ods_sqoop_base_data_order_payment_df_tesk = OssSensor(
        task_id='ods_sqoop_base_data_order_payment_df_tesk',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride_dw_sqoop/oride_data/data_order_payment",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    dwd_oride_order_base_include_test_di_tesk = OssSensor(
        task_id='dwd_oride_order_base_include_test_di_tesk',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
#路径
    hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name
##----------------------------------------- 脚本 ---------------------------------------## 

def dwd_oride_order_pay_detail_di_sql_task(ds):

    HQL='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
select 
            pay.order_id,--订单 ID
            driver_id,--司机ID
            pay_mode,--支付模式（0: 未知, 1: 线下支付, 2: opay, 3: 余额）
            price,-- 价格
            coupon_id,--使用的优惠券 ID
            coupon_name ,--优惠券名称
            coupon_amount,-- 使用的优惠券金额
            pay_amount,--实付金额
            bonus,-- 使用的奖励金
            balance,--使用的余额
            opay_amount,--opay 支付的金额
            reference,-- opay 流水号
            currency,--货币类型
            pay.country_code as country,--国家
            status,--支付模式（0: 支付中, 1: 成功, 2: 失败）
            modify_time,--最后修改时间
            create_time,--创建时间
            product_id,--订单业务类型(0: all 1:driect 2: street)
            updated_time,--最后更新时间
            pay_type, --支付类型(0:手动支付 1:自动支付)
            city_id,--所属城市
            passenger_id, --乘客 ID
            tip        ,--小费                                    
            capped_mode,--优惠活动车型（0 ride 1 keke 2 car）           
            capped_type,--优惠活动类型（0 normal 1 novice）             
            capped_id  ,--优惠活动 ID                               
            card_id,    --支付卡号 
            nvl(ord.country_code,'nal') as country_code,
            '{pt}' as dt
from 
     (SELECT 
            
            id as order_id,--订单 ID
            driver_id,--司机ID
            mode as pay_mode,--支付模式（0: 未知, 1: 线下支付, 2: opay, 3: 余额）
            price,-- 价格
            coupon_id,--使用的优惠券 ID
            coupon_name ,--优惠券名称
            coupon_amount,-- 使用的优惠券金额
            amount as pay_amount,--实付金额
            bonus,-- 使用的奖励金
            balance,--使用的余额
            opay_amount,--opay 支付的金额
            reference,-- opay 流水号
            currency,--货币类型
            country as country_code,--国家
            status,--支付模式（0: 支付中, 1: 成功, 2: 失败）
            (modify_time + 1*60*60*1) as modify_time,--最后修改时间
            (create_time + 1*60*60*1) as modify_time,--创建时间
            serv_type as product_id,--订单业务类型(0: all 1:driect 2: street)
            updated_at as updated_time,--最后更新时间
            pay_type, --支付类型(0:手动支付 1:自动支付)
            tip        ,--小费                                    
            capped_mode,--优惠活动车型（0 ride 1 keke 2 car）           
            capped_type,--优惠活动类型（0 normal 1 novice）             
            capped_id  ,--优惠活动 ID                               
            card_id    --支付卡号  

      FROM oride_dw_ods.ods_sqoop_base_data_order_payment_df
      WHERE dt='{pt}') pay
left outer join
(SELECT 
           order_id,
           city_id,--所属城市
            passenger_id,
             --乘客 ID
             part_hour,
             country_code,
             dt
      FROM oride_dw.dwd_oride_order_base_include_test_di
WHERE dt = '{pt}'
  and status=5
  AND city_id<>'999001' --去除测试数据
  ) ord
on pay.order_id=ord.order_id
and pay.country_code=ord.country_code

'''.format(
        pt=ds,
        #now_day='{{macros.ds_add(ds, +1)}}',
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
        )
    return HQL


#熔断数据，如果数据重复，报错
def check_key_data_task(ds):

    #主键重复校验
    HQL_DQC='''
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
    cf.delete_partition()

    #读取sql
    _sql="\n"+cf.alter_partition()+"\n"+dwd_oride_order_pay_detail_di_sql_task(ds)

    logging.info('Executing: %s',_sql)

    #执行Hive
    hive_hook.run_cli(_sql)

    #熔断数据，如果数据不能为0
    #check_key_data_cnt_task(ds)

    #熔断数据
    check_key_data_task(ds)

    #生产success
    cf.touchz_success()

    
dwd_oride_order_pay_detail_di_task= PythonOperator(
    task_id='dwd_oride_order_pay_detail_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date':'{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day':'{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour':'{{execution_date.strftime("%H")}}'
    },
    dag=dag
)


ods_sqoop_base_data_order_payment_df_tesk>>dwd_oride_order_pay_detail_di_task
dwd_oride_order_base_include_test_di_tesk>>dwd_oride_order_pay_detail_di_task