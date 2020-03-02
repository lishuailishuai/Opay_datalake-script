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
    'start_date': datetime(2020, 3, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_ocredit_phones_overdue_cube_d',
                  schedule_interval="30 02 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

dwd_ocredit_phones_order_base_df_task = OssSensor(
    task_id='dwd_ocredit_phones_order_base_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones/ocredit_phones_dw/dwd_ocredit_phones_order_base_df/country_code=nal",

        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_ocredit_phones_repayment_detail_df_task = OssSensor(
    task_id='dwd_ocredit_phones_repayment_detail_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones/ocredit_phones_dw/dwd_ocredit_phones_repayment_detail_df/country_code=nal",

        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


##----------------------------------------- 变量 ---------------------------------------##

db_name = "ocredit_phones_dw"
table_name = "app_ocredit_phones_overdue_cube_d"
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

def app_ocredit_phones_overdue_cube_d_sql_task(ds):
    HQL = '''


    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE ocredit_phones_dw.{table} partition(country_code,dt)
    
    select
          main.loan_time as loan_time,--'放款日'
          main.loan_month as loan_month,--'放款月份',
          main.loan_week as loan_week,--'放宽周',
          main.merchant_name as merchant_name,--'商户名称',
          sum(case when t2.order_id is not null then main.loan_amount*0.2712/100 else null end) as loan_amount_usd_expire,--'贷款金额USD_到期',
          count(DISTINCT case when t2.order_id is not null then t2.order_id else null end) as loan_cnt_expire,--'放款件数_到期',
          
          count(DISTINCT case when t2.overdue_days >0 then t2.order_id else null end) as overdue_cnt,--'逾期件数',
          count(DISTINCT case when t2.overdue_days between 1 and 7 then t2.order_id else null end) as 1_between_7DPD,--'当前逾期1-7天件数',
          count(DISTINCT case when t2.overdue_days between 8 and 15 then t2.order_id else null end) as 8_between_15DPD,--当前逾期8-15天件数
          count(DISTINCT case when t2.overdue_days between 16 and 30 then t2.order_id else null end) as 16_between_30DPD,--当前逾期16-30天件数
          count(DISTINCT case when t2.overdue_days >30 then t2.order_id else null end) as overdue_over_30_cnt,--逾期30+天件数
          
          
          count(DISTINCT case when t2.overdue_days >7 then t2.order_id else null end) as Existing_overdue7_cnt,--当前逾期7+天件数
          count(DISTINCT case when t2.overdue_days >15 then t2.order_id else null end) as Existing_overdue15_cnt,--当前逾期15+天件数
          count(DISTINCT case when t2.overdue_days >30 then t2.order_id else null end) as Existing_overdue30_cnt,--当前逾期30+天件数
          
          count(DISTINCT case when t3.overdue_days >0 then t3.order_id else null end) as downpay_overdue0_cnt,--首期逾期0+天件数
          count(DISTINCT case when t3.overdue_days >7 then t3.order_id else null end) as downpay_overdue7_cnt,--首期逾期7+天件数
          count(DISTINCT case when t3.overdue_days >15 then t3.order_id else null end) as downpay_overdue15_cnt,--首期逾期15+天件数
          count(DISTINCT case when t3.overdue_days >30 then t3.order_id else null end) as downpay_overdue30_cnt,--首期逾期30+天件数
          
          count(DISTINCT case when t3.fzrq > 0 then t3.order_id end) as downpay_fzrq0_cnt,--当前与预计还款时间差值0+件数
          count(DISTINCT case when t3.fzrq > 7 then t3.order_id end) as downpay_fzrq7_cnt,--当前与预计还款时间差值7+件数
          count(DISTINCT case when t3.fzrq > 15 then t3.order_id end) as downpay_fzrq15_cnt,--当前与预计还款时间差值15+件数
          count(DISTINCT case when t3.fzrq > 30 then t3.order_id end) as downpay_fzrq30_cnt,  --当前与预计还款时间差值30+件数
          'nal' as country_code,
          '{pt}' as dt
          
          from
          (
          select
          order_id,--订单号
          user_id,--销售端用户ID
          order_status,--订单状态
          pay_status,--支付状态
          payment_status,--还款状态
          product_name,--产品名称
          merchant_name,--商户
          (case when product_type='1' then '自营' when product_type='2' then '合作商' else null end) as product_type,--经营形式
          (case when order_id='012020011001240073' then '2020-01-04' else to_date(loan_time) end) as loan_time,--放款时间
          substr((case when order_id='012020011001240073' then '2020-01-04' else to_date(loan_time) end),1,7) as loan_month,--放款月份
          concat(date_add(next_day((case when order_id='012020011001240073' then '2020-01-04' else to_date(loan_time) end),'MO'),-7),'_',date_add(next_day((case when order_id='012020011001240073' then '2020-01-04' else to_date(loan_time) end),'MO'),-1)) as loan_week,--放款周
          terms as total_period_no,--期数
          down_payment/100 as down_payment,--首付金额
          loan_amount/100 as loan_amount,--借款金额
          date_sub(current_date(),1) as jzrq,--最新有数据的一天
          add_months(date_sub(current_date(),1),-1) as dqrq--最新有数据的一天的前一个月的日期
          from ocredit_phones_dw.dwd_ocredit_phones_order_base_df
          where dt='{pt}'
          and date_add(from_unixtime(unix_timestamp(create_time)+3600,'yyyy-MM-dd'),0)>='2019-12-28'
          and order_status='81' and loan_time is not null
          --放款成功
          )main
          
          left join
          (select
          t1.order_id,max(t1.overdue_days) as overdue_days,--用户分期，最大的未还钱的逾期天数
          count(t1.period_no) as due_period_no,--总分期数
          count(case when t1.plan_status='OEVERDUE' then t1.period_no else null end) as overdue_period_no,--逾期且没有还钱的分期有几个
          count(case when t1.plan_status='PERMANENT' then t1.period_no else null end) as fact_period_no,--还钱的分期有几个（不知道有没有逾期）
          sum(t1.due_total_amount) as due_total_amount,--按理应该还的所有钱（每月相加 月还总额）
          sum(t1.due_capital_amount) as due_capital_amount,--按理应该还的所有钱（每月相加 月还本金）
          sum(case when t1.plan_status='PERMANENT' then t1.fact_total_amount else null end) as fact_total_amount,--已经实际还的所有钱（实际月还总额）（不知道有没有逾期）
          sum(case when t1.plan_status='PERMANENT' then t1.fact_capital_amount else null end) as fact_capital_amount,--已经实际还的所有钱（实际月还本金）（不知道有没有逾期）
          sum(case when t1.plan_status='OEVERDUE' then t1.due_total_amount else null end) as overdue_total_amount,--逾期且没有还的所有钱（月还总额）
          sum(case when t1.plan_status='OEVERDUE' then t1.due_capital_amount else null end) as overdue_capital_amount--逾期且没有还的所有钱（月还本金）
          from
          (select
          order_id,--订单id
          user_id,--用户id
          contract_id,--合同编号
          current_period as period_no,--当前还款期数
          to_date(repayment_time) as due_tm,--预计还款时间
          month_total_amount  as due_total_amount,--月还总额
          month_amount as due_capital_amount,--月还本金
          to_date(real_repayment_time) as fact_tm,--实际还款时间
          real_total_amount as fact_total_amount,--实还总额
          real_amount as fact_capital_amount,--实还本金
          current_repayment_status as repayment_status,--当前还款状态:(0:未还清，1:已还清)
          case when current_repayment_status='3' then 'PERMANENT' else 'OVERDUE' end as plan_status,
          case when current_repayment_status='3' then 0 else datediff(current_date(),to_date(repayment_time)) end as overdue_days --未还钱的逾期天数
          from  ocredit_phones_dw.dwd_ocredit_phones_repayment_detail_df
          where dt='{pt}'
          and to_date(repayment_time) <= '{pt}'
          )t1 group by t1.order_id
          )t2 on main.order_id=t2.order_id
          
          left join
          (
          select
          date_sub(current_date(),1) as jzrq,----最新有数据的一天
          datediff(current_date(),to_date(date(repayment_time))) as fzrq,--没还钱，就是逾期天数
          order_id,
          user_id,
          contract_id,--
          current_period as period_no,
          date(repayment_time) as due_tm,--预计还款时间
          month_total_amount  as due_total_amount,--月还总额
          month_amount as due_capital_amount,--月还本金
          poundage as  due_poundage_amount,--手续费
          current_interest_rate as current_interest_rate,--利息
          real_repayment_time as fact_tm,--实际还款时间
          real_total_amount as fact_total_amount,--实还总额
          real_amount as fact_capital_amount,--实还本金
          real_service_fee as  fact_service_amount,--实还服务费
          real_interest as fact_interest_amount,--实还利息
          real_poundage as fact_poundage_amount,--实还手续费
          real_penalty_interest as fact_penalty_amount,--实还罚息
          current_repayment_status as repayment_status,--当前还款状态:(0:未还清，1:已还清)
          (case  
               when current_repayment_status in ('3') and datediff(to_date(real_repayment_time),to_date(repayment_time))>0 then 'OVERDUE_CLEARED'--已还但逾期
               when current_repayment_status in ('3') and datediff(to_date(real_repayment_time),to_date(repayment_time))<=0 then 'CLEARED'--已还没有逾期
               else 'OVERDUE' end) as plan_status,--逾期，没还钱
          (case  
               when current_repayment_status in ('3') and datediff(to_date(real_repayment_time),to_date(repayment_time))>0 then datediff(to_date(real_repayment_time),to_date(repayment_time))--已还的逾期天数
               when current_repayment_status in ('3') and datediff(to_date(real_repayment_time),to_date(repayment_time))<=0 then 0
               else datediff(current_date(),to_date(repayment_time)) end) as overdue_days    --未还的逾期天数
          from  ocredit_phones_dw.dwd_ocredit_phones_repayment_detail_df
          where dt='{pt}'
          and to_date(repayment_time) <= '{pt}'
          and current_period='1'--当钱还款期数为1
          )t3 on main.order_id=t3.order_id
          where t2.order_id is not null
          group by
          main.loan_time,
          main.loan_month,
          main.loan_week,
          main.merchant_name
          grouping sets(main.loan_time,main.loan_month,main.loan_week,main.merchant_name)

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

    if datetime.strptime(ds, '%Y-%m-%d').weekday() == 6:
        cf = CountriesPublicFrame("false", ds, db_name, table_name, hdfs_path, "true", "false")
    else:
        cf = CountriesPublicFrame("false", ds, db_name, table_name, hdfs_path, "true", "true")

    # 删除分区
    # cf.delete_partition()

    # 拼接SQL

    _sql = "\n" + cf.alter_partition() + "\n" + app_ocredit_phones_overdue_cube_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 熔断数据
    # check_key_data_task(ds)

    # 生产success
    cf.touchz_success()


app_ocredit_phones_overdue_cube_d_task = PythonOperator(
    task_id='app_ocredit_phones_overdue_cube_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)


dwd_ocredit_phones_order_base_df_task >> app_ocredit_phones_overdue_cube_d_task
dwd_ocredit_phones_repayment_detail_df_task >> app_ocredit_phones_overdue_cube_d_task