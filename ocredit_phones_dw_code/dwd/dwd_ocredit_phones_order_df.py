# -*- coding: utf-8 -*-
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
from plugins.CountriesPublicFrame import CountriesPublicFrame
import json
import logging
from airflow.models import Variable
import requests
import os
from airflow.sensors import OssSensor

args = {
    'owner': 'lishuai',
    'start_date': datetime(2020, 2, 18),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_ocredit_phones_order_df',
                  schedule_interval="30 02 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

ods_sqoop_base_t_order_df_task = OssSensor(
    task_id='ods_sqoop_base_t_order_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones_dw_sqoop/oloan/t_order",

        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "ocredit_phones_dw"
table_name = "dwd_ocredit_phones_order_df"
hdfs_path = "oss://opay-datalake/ocredit_phones/ocredit_phones_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"dag": dag, "db": "ocredit_phones_dw", "table": "{dag_name}".format(dag_name=dag_ids),
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

def dwd_ocredit_phones_order_df_sql_task(ds):
    HQL = '''


    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE ocredit_phones_dw.{table} partition(country_code,dt)

    select
          
          id, --无业务含义主键 
          order_id, --订单号 
          business_type, --订单类型  0:手机 1:车 
          user_id, --销售端用户ID 
          opay_id, --用户opayId 
          order_status, --订单状态：10 "等待初审"  11 "初核通过"  12 "初审失败"   30 "等待终审"  31 "复审通过"  32 "复审失败"   50 "待支付"  51 "支付中"  52 "支付失败"   70 "等待合同上传"  71 "合同等待审核"  72 "合同审核失败"  80 "等待放款"  81 "放款成功" 82 "放款中" 83 "放款失败" 99 "异常" 
          pay_status, --0等待支付 1成功2失败 3支付回调中 
          country, --国家 
          city, --城市 
          currency, --货币 
          brand_id, --品牌id 
          product_id, --产品ID 
          product_type, --经营形式 1 自营 2 合作商 
          product_num, --产品数量 
          product_name, --产品名称 
          product_pic, --产品图片 
          product_version, --产品数据版本 
          product_price, --产品售价 
          down_payment, --首付金额 
          loan_amount, --借款金额 
          order_fee, --订单总金额=产品售价*产品数量(商品金额) 
          terms, --期数 
          payment_rate, --首付比例 
          monthly_payment, --月还款金额 
          monthly_principal, --月还本金 
          monthly_fee, --月服务费 
          unpaid_principal, --剩余本金 
          f_product_id, --关联金融产品ID 
          f_product_version, --关联金融产品数据版本 
          merchant_id, --商户ID 
          merchant_name, --商户名称 
          store_id, --门店ID 
          store_name, --门店名称 
          is_delete, --0:未删除 1:已删除 
          from_unixtime(unix_timestamp(create_time)+3600,'yyyy-MM-dd HH:mm:ss'), --创建时间 
          from_unixtime(unix_timestamp(update_time)+3600,'yyyy-MM-dd HH:mm:ss'),--更新时间
          from_unixtime(unix_timestamp(loan_time)+3600,'yyyy-MM-dd HH:mm:ss'),--放款时间 
          opr_id, --操作更新用户ID 
          risk_status, --风控审核状态：1通过 0拒绝 
          risk_reason, --风控审核结果 
          sale_name, --销售名称 
          sale_phone, --销售电话 
          remark, --备注、失败原因 
          payment_status, --还款状态： 0未结清 3已结清 
          loan_price, --手机价格(销售录入) 
          channel, --渠道： 1=销售 2=用户 
          product_category, --产品类型： 1 手机 2 汽车 3 摩托车 4 家电 5 电脑
          substr((case when order_id='012020011001240073' then '2020-01-04' else to_date(create_time) end),1,10), --进件日期
          'nal' as country_code,
           dt

    from ocredit_phones_dw_ods.ods_sqoop_base_t_order_df
    where dt='{pt}' and
user_id not in 
(
'1209783514507214849', 
'1209126038292123650',
'1210903150317494274',
'1214471918163460097',
'1215642304343425026',
'1226878328587288578'
)
and business_type = '0'
    
    

    
    

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
        第一个参数true: 数据目录是有country_code分区。false 没有
        第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

        #读取sql
        %_sql_task(ds,v_hour)

        第一个参数ds: 天级任务
        第二个参数v_hour: 小时级任务，需要使用

    """

    cf = CountriesPublicFrame("false", ds, db_name, table_name, hdfs_path, "true", "true")

    # 删除分区
    # cf.delete_partition()

    # 拼接SQL

    _sql = "\n" + cf.alter_partition() + "\n" + dwd_ocredit_phones_order_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 熔断数据
    # check_key_data_task(ds)

    # 生产success
    cf.touchz_success()


dwd_ocredit_phones_order_df_task = PythonOperator(
    task_id='dwd_ocredit_phones_order_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

ods_sqoop_base_t_order_df_task >> dwd_ocredit_phones_order_df_task