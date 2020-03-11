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
    'start_date': datetime(2020, 3, 10),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_ocredit_phones_overdue_cube_d',
                  schedule_interval="30 02 * * *",
                  default_args=args)

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
          t3.loan_tm  as loan_tm, --放款日
          t3.loan_month as loan_month, --放款月份
          t3.loan_week as loan_week, --放款周
          sum(case when t2.order_id is not null then main.loan_amount*0.2712/100 else null end) as loan_amount_usd_expire, --'贷款金额USD_到期',
          count(DISTINCT case when t2.order_id is not null then t2.order_id else null end) as loan_cnt_expire, --'放款件数_到期',
          
          
          count(DISTINCT case when t2.overdue_days >0 then t2.order_id else null end) as existing_overdue_cnt,--`CPD0+件数，也就是现存逾期件数`,（合同数）
             
          count(DISTINCT case when t2.overdue_days >3 then t2.order_id else null end) as existing_overdue3_cnt,--现存逾期超过3天的件数（合同数）
          count(DISTINCT case when t2.overdue_days >5 then t2.order_id else null end) as existing_overdue5_cnt,--现存逾期超过5天的件数 （合同数）       
          count(DISTINCT case when t2.overdue_days >7 then t2.order_id else null end) as existing_overdue7_cnt,--现存逾期超过7天的件数 （合同数）     
          count(DISTINCT case when t2.overdue_days >15 then t2.order_id else null end) as existing_overdue15_cnt,--现存逾期超过15天的件数 （合同数）    
          count(DISTINCT case when t2.overdue_days >30 then t2.order_id else null end) as existing_overdue30_cnt,--现存逾期超过30天的件数 （合同数）    
          
          count(DISTINCT case when t2.fzrq > 0 then t2.order_id end) as existing_expire0_cnt,--现存逾期,放款到期超过0天的合同数
          count(DISTINCT case when t2.fzrq > 3 then t2.order_id end) as existing_expire3_cnt,--现存逾期,放款到期超过3天的合同数    
          count(DISTINCT case when t2.fzrq > 5 then t2.order_id end) as existing_expire5_cnt,--现存逾期,放款到期超过5天的合同数    
          count(DISTINCT case when t2.fzrq > 7 then t2.order_id end) as existing_expire7_cnt,--现存逾期,放款到期超过7天的合同数    
          count(DISTINCT case when t2.fzrq > 15 then t2.order_id end) as existing_expire15_cnt,--现存逾期,放款到期超过15天的合同数    
          count(DISTINCT case when t3.fzrq > 30 then t2.order_id end) as existing_expire30_cnt,--现存逾期,放款到期超过30天的合同数   
          
          
          
          count(DISTINCT case when t3.overdue_days >0 then t3.order_id else null end) as downpay_overdue_cnt,--`FPD0+件数，也就是首期逾期件数`,（合同数）
                 
          count(DISTINCT case when t3.overdue_days >3 then t3.order_id else null end) as downpay_overdue3_cnt,--首期逾期超过3天的件数 （合同数）    
          count(DISTINCT case when t3.overdue_days >5 then t3.order_id else null end) as downpay_overdue5_cnt,--首期逾期超过5天的件数 （合同数）         
          count(DISTINCT case when t3.overdue_days >7 then t3.order_id else null end) as downpay_overdue7_cnt,--首期逾期超过7天的件数 （合同数）         
          count(DISTINCT case when t3.overdue_days >15 then t3.order_id else null end) as downpay_overdue15_cnt,--首期逾期超过15天的件数 （合同数）        
          count(DISTINCT case when t3.overdue_days >30 then t3.order_id else null end) as downpay_overdue30_cnt,--首期逾期超过30天的件数 （合同数）        
          
          count(DISTINCT case when t3.fzrq > 0 then t3.order_id end) as downpay_expire0_cnt,--首期逾期,放款到期超过0天的合同数       
          count(DISTINCT case when t3.fzrq > 3 then t3.order_id end) as downpay_expire3_cnt,--首期逾期,放款到期超过3天的合同数           
          count(DISTINCT case when t3.fzrq > 5 then t3.order_id end) as downpay_expire5_cnt,--首期逾期,放款到期超过5天的合同数           
          count(DISTINCT case when t3.fzrq > 7 then t3.order_id end) as downpay_expire7_cnt,--首期逾期,放款到期超过7天的合同数           
          count(DISTINCT case when t3.fzrq > 15 then t3.order_id end) as downpay_expire15_cnt,--首期逾期,放款到期超过15天的合同数          
          count(DISTINCT case when t3.fzrq > 30 then t3.order_id end) as downpay_expire30_cnt,--首期逾期,放款到期超过30天的合同数          
          
          'nal' as country_code,
          '{pt}' as dt
          
          from
          (
          select
          order_id,--订单号
          opay_id,--用户opayId
          user_id,--销售端用户ID
          order_status,--订单状态
          pay_status,--支付状态
          payment_status,--还款状态
          product_name,--产品名称
          merchant_id,--商户ID
          merchant_name,--商户名称
          (case when product_type='1' then '自营' when product_type='2' then '合作商' else null end) as product_type,--经营形式
          to_date(loan_time) as loan_time,--放款时间
          substr(to_date(loan_time),1,7) as loan_month,--放款月份
          terms as total_period_no,--期数
          down_payment/100 as down_payment,--首付金额
          loan_amount/100 as loan_amount,--借款金额
          date_sub(current_date(),1) as jzrq,--昨天
          add_months(date_sub(current_date(),1),-1) as dqrq--昨天日期的前一个月的日期
          from ocredit_phones_dw.dwd_ocredit_phones_order_base_df
          where dt='{pt}'
          and date_add(from_unixtime(unix_timestamp(create_time)+3600,'yyyy-MM-dd'),0)>='2019-12-28'
          and order_status='81' and loan_time is not null
          )main
          
          left join
          (select
          t1.order_id,
          max(t1.overdue_days) as overdue_days,--用户分期，最大的未还钱的逾期天数
          min(case when t1.current_repayment_status = '2' then t1.due_tm else null end) as overdue_tm,--逾期最小的预计还款时间
          count(t1.due_tm) as due_period_no,--总分期数
          count(case when t1.current_repayment_status = '3' then t1.period_no else null end) as fact_period_no,--完成支付的分期个数
          count(case when t1.current_repayment_status = '2' then t1.period_no else null end) as over_period_no,--未完成支付的分期个数
          max(fzrq) as fzrq--最大的差值   --当前时间-预计还款时间
          from
          (select
          date_sub(current_date(),1) as jzrq,--昨天
          datediff(current_date(),to_date(date(repayment_time))) as fzrq,--当前时间-预计还款时间
          order_id,--订单id
          user_id,--用户id
          contract_id,--合同编号
          current_period as period_no,--当前还款期数
          to_date(repayment_time) as due_tm,--预计还款时间
          month_total_amount/100  as due_total_amount,--月还总额
          month_amount/100 as due_capital_amount,--月还本金
          to_date(real_repayment_time) as fact_tm,--实际还款时间
          real_total_amount/100 as fact_total_amount,--实还总额
          real_amount/100 as fact_capital_amount,--实还本金
          current_repayment_status,--当前还款状态:(0:未还清，1:已还清)
          (case when current_repayment_status='3' then 'PERMANENT' else 'OVERDUE' end) as plan_status,
          (case when current_repayment_status='3' then 0 else datediff(current_date(),to_date(repayment_time)) end) as overdue_days--未还钱的逾期天数
          from  ocredit_phones_dw.dwd_ocredit_phones_repayment_detail_df
          where dt='{pt}'
          and to_date(repayment_time) <= '{pt}'
          )t1
          group by t1.order_id
          )t2 on main.order_id=t2.order_id
          
          
          left join
          (select
          date_sub(current_date(),1) as jzrq,----昨天
          substr(ADD_MONTHS(to_date(repayment_time),-1),1,7) as loan_month,--放款月
          ADD_MONTHS(to_date(repayment_time),-1) as loan_tm,----放款时间
          concat(date_add(next_day(ADD_MONTHS(to_date(repayment_time),-1),'MO'),-7),'_',date_add(next_day(ADD_MONTHS(to_date(repayment_time),-1),'MO'),-1)) as loan_week,--放款周
          datediff(current_date(),to_date(date(repayment_time))) as fzrq,--当前时间-预计还款时间
          order_id,
          user_id,
          contract_id,
          current_period as period_no,
          to_date(repayment_time) as due_tm,--预计还款时间
          month_total_amount/100  as due_total_amount,--月还总额
          month_amount/100 as due_capital_amount,--月还本金
          poundage/100 as  due_poundage_amount,--手续费
          current_interest_rate/100 as current_interest_rate,--利息
          real_repayment_time as fact_tm,--实际还款时间
          real_total_amount/100 as fact_total_amount,--实还总额
          real_amount/100 as fact_capital_amount,--实还本金
          real_service_fee/100 as  fact_service_amount,--实还服务费
          real_interest/100 as fact_interest_amount,--实还利息
          real_poundage/100 as fact_poundage_amount,--实还手续费
          real_penalty_interest/100 as fact_penalty_amount,--实还罚息
          current_repayment_status,--当前还款状态:(0:未还清，1:已还清)
          (case when current_repayment_status in ('3') and datediff(to_date(real_repayment_time),to_date(repayment_time))>0 then 'OVERDUE_CLEARED'--已还但逾期
                when current_repayment_status in ('3') and datediff(to_date(real_repayment_time),to_date(repayment_time))<=0 then 'CLEARED'--已还没有逾期
                else 'OVERDUE' end) as plan_status,--逾期，没还钱
          (case 
          when current_repayment_status in ('3') and datediff(to_date(real_repayment_time),to_date(repayment_time))>0 then datediff(to_date(real_repayment_time),to_date(repayment_time))--已还的逾期天数
          when current_repayment_status in ('3') and datediff(to_date(real_repayment_time),to_date(repayment_time))<=0 then 0
          else datediff(current_date(),to_date(repayment_time)) end) as overdue_days      --未还的逾期天数
          
          from  ocredit_phones_dw.dwd_ocredit_phones_repayment_detail_df
          where dt='{pt}'
          and to_date(repayment_time) <= '{pt}'
          and current_period='1'--当钱还款期数为1
          )t3 on main.order_id=t3.order_id
          where t2.order_id is not null
          group by 
          t3.loan_tm,
          t3.loan_month,
          t3.loan_week
          grouping sets(t3.loan_tm,t3.loan_month,t3.loan_week)


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