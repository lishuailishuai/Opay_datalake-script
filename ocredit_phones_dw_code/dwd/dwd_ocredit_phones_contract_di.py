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
from plugins.CountriesAppFrame import CountriesAppFrame

from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os
from utils.get_local_time import GetLocalTime

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2020, 4, 14),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_ocredit_phones_contract_di',
                  schedule_interval="30 00 * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "ocredit_phones_dw"
table_name = "dwd_ocredit_phones_contract_di"
hdfs_path = "oss://opay-datalake/ocredit_phones/ocredit_phones_dw/" + table_name
config = eval(Variable.get("ocredit_time_zone_config"))
time_zone = config['NG']['time_zone']
##----------------------------------------- 依赖 ---------------------------------------##

### 检查当前小时的分区依赖
ods_binlog_base_t_contract_all_hi_check_task = OssSensor(
    task_id='ods_binlog_base_t_contract_all_hi_check_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour=23/_SUCCESS'.format(
        hdfs_path_str="ocredit_phones_all_hi/ods_binlog_base_t_contract_all_hi",
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


def dwd_ocredit_phones_contract_di_sql_task(ds, v_date):
    HQL = '''

    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;

    insert overwrite table {db}.{table} 
    partition(country_code, dt)

    select id,
            contract_id,            --合同号                                                                                                
            order_id,               --订单号                                                                                                
            create_user_id,         --创建人                                                                                                
            create_time,            --创建时间                                                                                               
            update_user_id,         --修改人                                                                                                
            update_time,            --修改时间                                                                                               
            contract_version,       --版本号                                                                                                
            down_payment_amount,    --首付金额                                                                                               
            loan_amount,            --贷款总额                                                                                               
            periods_number_total,   --分期总期数                                                                                              
            product_id,             --产品标识                                                                                               
            product_name,           --产品名称（品牌+型号）                                                                                        
            product_price,          --产品价格                                                                                               
            f_product_id,           --金融产品标识                                                                                             
            f_product_version,      --金融产品版本号                                                                                            
            merchant_id,            --商户标识                                                                                               
            retailer_id,            --门店标识                                                                                               
            user_id,                --用户标识                                                                                               
            business_type,          --业务类型，0：手机分期；1：汽车分期                                                                                 
            user_real_name,         --用户姓名                                                                                               
            user_bvn,               --用户身份标识                                                                                             
            user_birthday,          --用户生日                                                                                               
            user_phone,             --用户手机号                                                                                              
            user_grade,             --客户等级                                                                                               
            user_country,           --用户国家                                                                                               
            user_city,              --用户城市                                                                                               
            user_home_address,      --用户家庭地址                                                                                             
            inviter_name,           --邀请人姓名                                                                                              
            inviter_phone,          --邀请人手机号                                                                                             
            apply_time,             --申请时间                                                                                               
            auditor_id,             --审核员标识                                                                                              
            auditor_name,           --审核员姓名                                                                                              
            last_audit_time_begin,  --最后一次审核开始时间                                                                                         
            last_audit_time,        --最后一次审核结束时间                                                                                         
            final_audit_pass,       --最终审核通过时间                                                                                           
            contract_status,        --状态，0：补充材料中；1：一审审核中；2：一审审核已通过；3：一审审核未通过；4：二审审核已通过；5：二审审核未通过；6：关闭；                                   
            is_lock,                --是否锁定，0：否；1：是；                                                                                      
            lock_holder_id,         --锁持有者                                                                                               
            voucher_upload_time,     --合同凭证上传时间 
            country_code,  --如果表中有国家编码直接上传国家编码
            dt

    from (select *,row_number() over(partition by id order by utc_date_hour desc) as rn 
          from ocredit_phones_dw.dwd_ocredit_phones_contract_hi 
          where dt=date_format("{v_date}", 'yyyy-MM-dd') 
          and (substr(create_time,1,10)=date_format("{v_date}", 'yyyy-MM-dd') or
          substr(update_time,1,10)=date_format("{v_date}", 'yyyy-MM-dd'))  --后续正常上线后，这个条件可以不限定，只是初始化当天需要限定
          and business_type = '0'  --目前建模分业务线
    ) t1 where t1.rn=1;
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
            "frame_type": "local",
            "business_key": "ocredit"
        }
    ]

    cf = CountriesAppFrame(args)

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dwd_ocredit_phones_contract_di_sql_task(ds, v_date)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dwd_ocredit_phones_contract_di_task = PythonOperator(
    task_id='dwd_ocredit_phones_contract_di_task',
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

ods_binlog_base_t_contract_all_hi_check_task >> dwd_ocredit_phones_contract_di_task
