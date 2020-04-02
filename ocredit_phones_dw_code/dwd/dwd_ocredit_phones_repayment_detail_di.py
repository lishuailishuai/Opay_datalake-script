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
from airflow.sensors import OssSensor
from plugins.CountriesPublicFrame_dev import CountriesPublicFrame_dev

from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os
from utils.get_local_time import GetLocalTime

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2020, 4, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_ocredit_phones_repayment_detail_di',
                  schedule_interval="30 00 * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "ocredit_phones_dw"
table_name = "dwd_ocredit_phones_repayment_detail_di"
hdfs_path = "oss://opay-datalake/opay/ocredit_phones_dw/" + table_name
config = eval(Variable.get("ocredit_time_zone_config"))
time_zone = config['NG']['time_zone']
##----------------------------------------- 依赖 ---------------------------------------##

### 检查当前小时的分区依赖
ods_binlog_base_t_repayment_detail_all_hi_check_task = OssSensor(
    task_id='ods_binlog_base_t_repayment_detail_all_hi_check_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour=23/_SUCCESS'.format(
        hdfs_path_str="ocredit_phones_all_hi/ods_binlog_base_t_repayment_detail_all_hi",
        pt='{{ds}}',
        hour='{{ execution_date.strftime("%H") }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds, dag, execution_date, **op_kwargs):
    dag_ids = dag.dag_id

    # 监控国家
    v_country_code = 'NG'

    # 时间偏移量
    v_gap_hour = 0

    v_date = GetLocalTime("ocredit", execution_date.strftime("%Y-%m-%d %H"), v_country_code, v_gap_hour)['date']
    v_hour = GetLocalTime("ocredit", execution_date.strftime("%Y-%m-%d %H"), v_country_code, v_gap_hour)['hour']

    # 小时级监控
    tb_hour_task = [
        {"dag": dag, "db": "ocredit_phones_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code={country_code}/dt={pt}".format(country_code=v_country_code,
                                                                                   pt=v_date, now_hour=v_hour),
         "timeout": "1200"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb_hour_task)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


def dwd_ocredit_phones_repayment_detail_di_sql_task(ds, v_date):
    HQL = '''

    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;

    insert overwrite table {db}.{table} 
    partition(country_code, dt)

    select id as repay_detail_id,            --  还款明细ID                                                                                 
            pay_id,                     --  支付流水号                                                                                  
            repayment_id,               --  还款计划ID                                                                                 
            order_id,                   --  订单ID                                                                                   
            order_type,                 -- 订单渠道 汽车(0:销售app 1:以租代售 2:汽车质押 3:汽车非质押) 其他(0:默认)                                         
            user_id,                    --  用户id                                                                                   
            contract_id,                --  合同编号                                                                                   
            financial_product_id,       --  金融产品id                                                                                 
            business_type,              --  业务类型                                                                                   
            sale_mode,                  --  销售类型                                                                                   
            current_period,             --  当前还款期数                                                                                 
            current_repayment_status,   --         当前还款状态:(0:未还清，1:已还清)                                                            
            month_total_amount,         -- 月还总额                                                                                    
            month_amount,               -- 月还本金                                                                                    
            month_service_fee,          -- 月服务费                                                                                    
            poundage,                   -- 手续费                                                                                     
            current_interest_rate,      -- 利息                                                                                      
            penalty_interest_rate,      -- 罚息                                                                                      
            repayment_time,             -- 预计还款时间                                                                                  
            real_total_amount,          -- 实还总额                                                                                    
            real_amount,                -- 实还本金                                                                                    
            real_service_fee,           -- 实还服务费                                                                                   
            real_interest,              -- 实还利息                                                                                    
            real_poundage,              -- 实还手续费                                                                                   
            real_penalty_interest,      -- 实还罚息                                                                                    
            real_repayment_time,        -- 实际还款时间                                                                                  
            not_return_amount,          -- 剩余本金                                                                                    
            version,                    -- 版本号                                                                                     
            remark,                     -- 备注                                                                                      
            create_time,                -- 创建时间                                                                                    
            update_time,                -- 更新时间                                                                                    
            pay_type,                   -- 还款方式 0未扣款 1系统划扣 2人工减免结清                                                                 
            allocated,                  -- 催收是否已分配  0未分配 1已分配                                                                      
            allocated_user_id,          -- 催收分配接收人用户id                                                                             
            allocated_user_name,        -- 催收分配接收人用户名称                                                                             
            allocated_time,             -- 催收分配时间                                                                                  
            collection_status,          -- 催收状态 0未分配 1催收中 2已结案 3已关闭                                                                
            allocated_over_time,        -- 催收结案时间                                                                                  
            total_deductions,           -- 减免总额                                                                                    
            allocated_user_leader_name, --          催收分配接收人组长名称                                                                    
            recent_penalty_time,        -- 最新更新罚息时间                                                                                
            repay_version,               -- 支付版本号                                                   
        t1.utc_date_hour,  --utc时间字段 
        'NG' country_code,  --如果表中有国家编码直接上传国家编码
        date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'yyyy-MM-dd') as dt

    from (select * from (select *,
                 date_format('{v_date}', 'yyyy-MM-dd HH') as utc_date_hour,
                 row_number() over(partition by id order by `__ts_ms` desc,`__file` desc,cast(`__pos` as int) desc) rn
             from ocredit_phones_dw_ods.ods_binlog_base_t_repayment_detail_all_hi
            where 
                dt >= '{bef_yes_day}' --有可能多个国家时区不一样，如果要取昨天一天的本地数据，需要尽可能多的限定全采集的数据,有可能在前天分区或今天分区
                and (substr(default.localTime("{config}",'NG',create_time,0),1,10)=date_format('{v_date}', 'yyyy-MM-dd') --按本地取昨天数据
                or default.localTime("{config}",'NG',update_time,0)=date_format('{v_date}', 'yyyy-MM-dd'))
                and `__deleted` = 'false') m
        where rn=1
    ) t1 ;
    '''.format(
        pt=ds,
        v_date=v_date,
        bef_yes_day=airflow.macros.ds_add(ds, -1),
        table=table_name,
        db=db_name,
        config=config
    )
    return HQL


# 主流程
def execution_data_task_id(ds, dag, **kwargs):
    v_date = kwargs.get('v_execution_date')
    v_day = kwargs.get('v_execution_day')
    v_hour = kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

    """
        #功能函数
            alter语句: alter_partition()
            删除分区: delete_partition()
            生产success: touchz_success()

        #参数
            is_countries_online --是否开通多国家业务 默认(true 开通)
            db_name --hive 数据库的名称
            table_name --hive 表的名称
            data_oss_path --oss 数据目录的地址
            is_country_partition --是否有国家码分区,[默认(true 有country_code分区)]
            is_result_force_exist --数据是否强行产出,[默认(true 必须有数据才生成_SUCCESS)] false 数据没有也生成_SUCCESS 
            execute_time --当前脚本执行时间(%Y-%m-%d %H:%M:%S)
            is_hour_task --是否开通小时级任务,[默认(false)]
            frame_type --模板类型(只有 is_hour_task:'true' 时生效): utc 产出分区为utc时间，local 产出分区为本地时间,[默认(utc)]。

        #读取sql
            %_sql(ds,v_hour)

    """

    args = [
        {
            "dag": dag,
            "is_countries_online": "true",
            "db_name": db_name,
            "table_name": table_name,
            "data_oss_path": hdfs_path,
            "is_country_partition": "true",
            "is_result_force_exist": "false",
            "execute_time": v_date,
            "is_hour_task": "false",
            "frame_type": "local"
        }
    ]

    cf = CountriesPublicFrame_dev(args)

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dwd_ocredit_phones_repayment_detail_di_sql_task(ds, v_date)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dwd_ocredit_phones_repayment_detail_di_task = PythonOperator(
    task_id='dwd_ocredit_phones_repayment_detail_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)

ods_binlog_base_t_repayment_detail_all_hi_check_task >> dwd_ocredit_phones_repayment_detail_di_task
