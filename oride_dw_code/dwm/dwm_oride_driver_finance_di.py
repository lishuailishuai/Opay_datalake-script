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
from airflow.sensors.s3_key_sensor import S3KeySensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from plugins.CountriesPublicFrame import CountriesPublicFrame
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2019, 12, 13),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwm_oride_driver_finance_di',
                  schedule_interval="30 01 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dwm_oride_driver_finance_di"

##----------------------------------------- 依赖 ---------------------------------------##
#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":
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

    dwd_oride_driver_records_day_df_prev_day_task = UFileSensor(
        task_id='dwd_oride_driver_records_day_df_prev_day_task',
        filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_driver_records_day_df/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    dwd_oride_driver_recharge_records_df_prev_day_task = UFileSensor(
        task_id='dwd_oride_driver_recharge_records_df_prev_day_task',
        filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_driver_recharge_records_df/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    dwd_oride_driver_reward_di_prev_day_task = UFileSensor(
        task_id='dwd_oride_driver_reward_di_prev_day_task',
        filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_driver_reward_di/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    ods_data_driver_pay_records_prev_day_task = UFileSensor(
        task_id='ods_data_driver_pay_records_prev_day_task',
        filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_pay_records",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    dwd_oride_finance_driver_repayment_df_prev_day_task = UFileSensor(
        task_id='dwd_oride_finance_driver_repayment_df_prev_day_task',
        filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_finance_driver_repayment_df/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    dwd_oride_driver_balance_extend_df_prev_day_task = UFileSensor(
        task_id='dwd_oride_driver_balance_extend_df_prev_day_task',
        filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_driver_balance_extend_df/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    #路径
    hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name
else:
    print("成功")
    dim_oride_driver_base_prev_day_task = OssSensor(
        task_id='dim_oride_driver_base_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dim_oride_driver_base/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    dwd_oride_driver_records_day_df_prev_day_task = OssSensor(
        task_id='dwd_oride_driver_records_day_df_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_driver_records_day_df/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    dwd_oride_driver_recharge_records_df_prev_day_task = OssSensor(
        task_id='dwd_oride_driver_recharge_records_df_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_driver_recharge_records_df/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    dwd_oride_driver_reward_di_prev_day_task = OssSensor(
        task_id='dwd_oride_driver_reward_di_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_driver_reward_di/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    ods_data_driver_pay_records_prev_day_task = OssSensor(
        task_id='ods_data_driver_pay_records_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_pay_records",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    dwd_oride_finance_driver_repayment_df_prev_day_task = OssSensor(
        task_id='dwd_oride_finance_driver_repayment_df_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_finance_driver_repayment_df/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    dwd_oride_driver_balance_extend_df_prev_day_task = OssSensor(
        task_id='dwd_oride_driver_balance_extend_df_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_driver_balance_extend_df/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    # 路径
    hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def dwm_oride_driver_finance_di_sql_task(ds):
    HQL = '''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    insert overwrite table {db}.{table} partition(country_code,dt)
    select dri.driver_id,
           dri.city_id,
           dri.product_id,
           dri.driver_name, --司机姓名
           dri.phone_number, --手机号
           dri.plate_number, --车牌号
           dri.fault, --司机状态 0:正常1:修理2无资料3事故4扣除5欠缴6离职
           day.amount_all,  --司机总收入
           day.amount_true, -- 当日骑手-实际到手收入（用作打款）
           day.amount_pay_online, --当日总收入-线上支付金额(opay线上支付，余额支付，卡支付)   
           day.amount_pay_offline,  --当日总收入-线下支付金额(现金支付)
           day.amount_service,  --(份子钱+抽成)总数
           day.amount_platform,  --平台抽成
           day.amount_agenter, --份子钱部分
           day.amount_tip,  --当日-小费收入 
           day.amount_tip_offline, --当日-小费收入(线下支付)   
           day.amount_additional, --附加费-线上
           day.amount_additional_offline, --附加费-线下 
           recharge.recharge_amount, --资金调整金额，用于统计理论b端补贴
           reward.reward_amount,  --司机奖励金，用于统计理论b端补贴
           pay.amount_recharge,  --资金调整金额，用于统计实际b端补贴
           pay.amount_reward,  --司机奖励金，用于统计实际b端补贴
           repay.start_date, --司机实际开始还款日期
           repay.theory_start_date, --司机理论开始还款日期
           repay.amount,  --司机每期还款金额
           repay.numbers, --总期数,总贷款金额=amount*numbers，理论应还金额=amount*repaid_numbers
           repay.repaid_numbers,  --已还款期数,剩余还款期数=numbers-repaid_numbers
           repay.balance,  --司机账户余额
           repay.theory_repay_amount,  --理论应还金额
           nvl(repay.act_repay_amount, 0 ) act_repay_amount, --实际已还款金额
           nvl(repay.act_repay_amount / repay.amount ,0) as act_repaid_numbers , --实际已还款期数
           nvl(if(repay.balance < 0,(repay.all_repay_amount-repay.act_repay_amount) / repay.amount,0),0) as conversion_overdue_days, --换算逾期天数
           --nvl(repay.all_repay_amount,0) as all_repay_amount, --总贷款金额=amount*numbers         
           
           dri.country_code,
           '{pt}' as dt

        from (SELECT *
           FROM oride_dw.dim_oride_driver_base
           WHERE dt='{pt}') dri 
           
        left join
        
        --司机收入表中对应当日司机收入
        (
        select driver_id,
              -- from_unixtime(day,'yyyy-MM-dd') as dt, --日期
               amount_all,  --司机总收入
               amount_true, -- 当日骑手-实际到手收入（用作打款）
               amount_pay_online, --当日总收入-线上支付金额(opay线上支付，余额支付，卡支付)   
               amount_pay_offline,  --当日总收入-线下支付金额(现金支付)
               amount_reward,  --当日总收入-奖励金,只是包含未离职的司机，因此如果要统计所有司机的奖励金需要从司机奖励表统计    
               amount_service,  --(份子钱+抽成)总数
               amount_platform,  --平台抽成
               amount_agenter, --份子钱部分
               amount_recharge,  --资金调整
               amount_tip,  --当日-小费收入 
               amount_tip_offline, --当日-小费收入(线下支付)   
               amount_additional, --附加费-线上
               amount_additional_offline --附加费-线下 
        from oride_dw.dwd_oride_driver_records_day_df
        where dt='{pt}' and from_unixtime(day,'yyyy-MM-dd')='{pt}'
        ) day
        on dri.driver_id=day.driver_id
        
        left join
        
        --资金调整金额,统计理论b端补贴用
        (
        select driver_id,
               sum(amount) as recharge_amount  --资金调整金额
        from oride_dw.dwd_oride_driver_recharge_records_df 
        where dt='{pt}'
        and from_unixtime(created_at,'yyyy-MM-dd')='{pt}'
        and amount_reason in(4,5,7)
        group by driver_id
        ) recharge
        on dri.driver_id=recharge.driver_id
        
        left join
        --司机奖励金,统计理论b端补贴用
        
        (
        select driver_id,
               sum(amount) as reward_amount  --司机奖励金
        from oride_dw.dwd_oride_driver_reward_di  --目前是全量表，后续会变成增量表
        where dt='{pt}'
        and from_unixtime(create_time,'yyyy-MM-dd')='{pt}'  --后续该限制条件要去除
        group by driver_id
        ) reward
        on dri.driver_id=reward.driver_id
        
        left join 
        --司机实际b补，实际手机还款相关逻辑
        
        (
        SELECT pay.driver_id,
              -- FROM_UNIXTIME(pay.created_at,'yyyy-MM-dd') dt,  --实际金额时间以该字段为准
               sum(day.amount_reward) AS amount_reward, --司机奖励金，用于统计实际b端补贴
               sum(recharge.amount_recharge) AS amount_recharge, --用于统计实际b端补贴资金调整金额
               sum(recharge.phone_amount) as phone_amount  --手机贷款实际还款（不全，只有司机余额大于0部分）
        FROM
          (SELECT record_day,
                  driver_id,
                  created_at
           FROM oride_dw_ods.ods_sqoop_base_data_driver_pay_records_df LATERAL VIEW explode(split(record_days,',')) record_days AS record_day
           WHERE dt='{pt}'
             and FROM_UNIXTIME(created_at,'yyyy-MM-dd')='{pt}'  --统计增量数据【即当日快照数据】
             AND status=1   
             ) pay
        LEFT JOIN
          (SELECT *
           FROM oride_dw.dwd_oride_driver_records_day_df
           WHERE dt='{pt}') DAY ON pay.driver_id=day.driver_id
        AND pay.record_day=day.day
        LEFT JOIN
          (SELECT driver_id,
                  from_unixtime(created_at,'yyyy-MM-dd') as create_date,
                  sum(if(amount_reason in(4,5,7),amount,0)) as amount_recharge,
                  sum(if(amount_reason=6,amount,0)) as phone_amount           
           FROM oride_dw.dwd_oride_driver_recharge_records_df
           WHERE dt='{pt}'
             AND amount_reason IN(4,
                                  5,
                                  6, --手机还款
                                  7)
           group by driver_id,
                  from_unixtime(created_at,'yyyy-MM-dd')) recharge ON day.driver_id=recharge.driver_id
        AND from_unixtime(day.day,'yyyy-MM-dd')=recharge.create_date
        group by pay.driver_id) pay
        on dri.driver_id=pay.driver_id
        
        left join 
        -- 司机手机还款相关
        
        (
            select repay.driver_id,
                   repay.dt,  --日期
                   recharge.start_date, --司机实际开始还款日期
                   repay.start_date as theory_start_date, --司机理论开始还款日期
                   repay.amount,  --司机每期还款金额
                   repay.numbers, --总期数,总贷款金额=amount*numbers，理论应还金额=amount*repaid_numbers
                   repay.repaid_numbers,  --已还款期数,剩余还款期数=numbers-repaid_numbers
                   repay.theory_repay_amount, --理论应该还款金额
                   all_repay_amount,--总贷款金额
                   balance.balance,  --司机账户余额
                   if(
                        if(balance.balance<0,repay.theory_repay_amount +balance.balance,theory_repay_amount)>0,
                        if(balance.balance<0,repay.theory_repay_amount+balance.balance,theory_repay_amount),
                        0
                    ) as act_repay_amount  --实际已还款金额
                   
            from 
            (   select 
                     driver_id,
                     dt,
                     start_date,--司机理论开始还款日期
                     amount,--司机每期还款金额
                     numbers,--还款总期数
                     repaid_numbers,--已还款期数
                     amount * repaid_numbers as theory_repay_amount, --理论应该已还款金额
                     amount * numbers as all_repay_amount--总贷款金额
                from oride_dw.dwd_oride_finance_driver_repayment_df
                where dt='{pt}' and repayment_type=0
            ) repay
            left join
            (
                select driver_id,
                    from_unixtime(min(created_at),'yyyy-MM-dd') as start_date
                from oride_dw.dwd_oride_driver_recharge_records_df 
                where dt='{pt}'
                and amount_reason=6 
                and amount<0 
                group by driver_id
            ) recharge on repay.driver_id=recharge.driver_id
            left join
            (
                select
                    driver_id,
                    balance 
                from oride_dw.dwd_oride_driver_balance_extend_df
                where dt='{pt}'
            ) balance on repay.driver_id=balance.driver_id
        ) repay
        on dri.driver_id=repay.driver_id;
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )
    return HQL


# 熔断数据，如果数据重复，报错
def check_key_data_task(ds):
    cursor = get_hive_cursor()

    # 主键重复校验
    check_sql = '''
    SELECT count(1)-count(distinct driver_id) as cnt
      FROM {db}.{table}
      WHERE dt='{pt}'
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )

    logging.info('Executing 主键重复校验: %s', check_sql)

    cursor.execute(check_sql)

    res = cursor.fetchone()

    if res[0] > 1:
        flag = 1
        raise Exception("Error The primary key repeat !", res)
        sys.exit(1)
    else:
        flag = 0
        print("-----> Notice Data Export Success ......")

    return flag


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
    cf = CountriesPublicFrame("true", ds, db_name, table_name, hdfs_path, "true", "true")

    # 删除分区
    cf.delete_partition()

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dwm_oride_driver_finance_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 熔断数据
    check_key_data_task(ds)

    # 生产success
    cf.touchz_success()


dwm_oride_driver_finance_di_task = PythonOperator(
    task_id='dwm_oride_driver_finance_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

dim_oride_driver_base_prev_day_task >> dwm_oride_driver_finance_di_task
dwd_oride_driver_records_day_df_prev_day_task >> dwm_oride_driver_finance_di_task
dwd_oride_driver_recharge_records_df_prev_day_task >> dwm_oride_driver_finance_di_task
dwd_oride_driver_reward_di_prev_day_task >> dwm_oride_driver_finance_di_task
ods_data_driver_pay_records_prev_day_task >> dwm_oride_driver_finance_di_task
dwd_oride_finance_driver_repayment_df_prev_day_task >> dwm_oride_driver_finance_di_task
dwd_oride_driver_balance_extend_df_prev_day_task >> dwm_oride_driver_finance_di_task
