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
    'start_date': datetime(2020, 2, 29),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_ocredit_phones_repayment_detail_df',
                  schedule_interval="30 01 * * *",
                  default_args=args)

##----------------------------------------- 依赖 ---------------------------------------##

ods_sqoop_base_t_repayment_detail_df_task = OssSensor(
    task_id='ods_sqoop_base_t_repayment_detail_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones_dw_sqoop/oloan/t_repayment_detail",

        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "ocredit_phones_dw"
table_name = "dwd_ocredit_phones_repayment_detail_df"
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

def dwd_ocredit_phones_repayment_detail_df_sql_task(ds):
    HQL = '''


    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE ocredit_phones_dw.{table} partition(country_code,dt)
    
    select
          id,--'主键'
          pay_id,--'支付流水号'
          repayment_id, -- '还款计划ID'
          order_id, -- '订单ID'
          user_id, -- '用户id'
          contract_id, -- '合同编号' 
          financial_product_id, -- '金融产品id'
          business_type, -- '业务类型' 
          sale_mode, -- '销售类型'
          current_period, -- '当前还款期数'
          current_repayment_status, -- '当前还款状态:(0:未还清，1:已还清)'
          month_total_amount, -- '月还总额'
          month_amount, -- '月还本金'
          month_service_fee, -- '月服务费'
          poundage, -- '手续费' 
          current_interest_rate, -- '利息'
          penalty_interest_rate, -- '罚息'
          from_unixtime(unix_timestamp(repayment_time)+3600,'yyyy-MM-dd HH:mm:ss') as repayment_time,--'预计还款时间'
          real_total_amount, -- '实还总额'
          real_amount, -- '实还本金' 
          real_service_fee, -- '实还服务费'
          real_interest, -- '实还利息' 
          real_poundage, -- '实还手续费'
          real_penalty_interest, -- '实还罚息' 
          from_unixtime(unix_timestamp(real_repayment_time)+3600,'yyyy-MM-dd HH:mm:ss') as real_repayment_time,-- '实际还款时间'
          not_return_amount, -- '剩余本金' 
          version, -- '版本号'
          remark, -- '备注' 
          from_unixtime(unix_timestamp(create_time)+3600,'yyyy-MM-dd HH:mm:ss') as create_time, --创建时间 
          from_unixtime(unix_timestamp(update_time)+3600,'yyyy-MM-dd HH:mm:ss') as update_time,--更新时间
          pay_type, -- '还款方式 0未扣款 1系统划扣 2人工减免结清'
          allocated, -- '催收是否已分配  0未分配 1已分配'
          allocated_user_id, -- '催收分配接收人用户id' 
          allocated_user_name, -- '催收分配接收人用户名称' 
          from_unixtime(unix_timestamp(allocated_time)+3600,'yyyy-MM-dd HH:mm:ss') as allocated_time,-- '催收分配时间'
          collection_status, -- '催收状态 0未分配 1催收中 2已结案 3已关闭'
          'nal' as country_code,
          '{pt}' as dt
    from ocredit_phones_dw_ods.ods_sqoop_base_t_repayment_detail_df
    where dt='{pt}' 
    and business_type = '0';
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
    if datetime.strptime(ds,'%Y-%m-%d').weekday() == 6:
        cf = CountriesPublicFrame("false", ds, db_name, table_name, hdfs_path, "true", "false")
    else:
        cf = CountriesPublicFrame("false", ds, db_name, table_name, hdfs_path, "true", "true")

    # 删除分区
    # cf.delete_partition()

    # 拼接SQL

    _sql = "\n" + cf.alter_partition() + "\n" + dwd_ocredit_phones_repayment_detail_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 熔断数据
    # check_key_data_task(ds)

    # 生产success
    cf.touchz_success()


dwd_ocredit_phones_repayment_detail_df_task = PythonOperator(
    task_id='dwd_ocredit_phones_repayment_detail_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

ods_sqoop_base_t_repayment_detail_df_task >> dwd_ocredit_phones_repayment_detail_df_task