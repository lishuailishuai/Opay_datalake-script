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
    'start_date': datetime(2020, 4, 9),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
     'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_ocredit_merchant_info_hf',
                  schedule_interval="30 * * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "ocredit_phones_dw"
table_name = "dwd_ocredit_merchant_info_hf"
hdfs_path = "oss://opay-datalake/ocredit_phones/ocredit_phones_dw/" + table_name
config = eval(Variable.get("ocredit_time_zone_config"))
time_zone = config['NG']['time_zone']
##----------------------------------------- 依赖 ---------------------------------------##

### 检查当前小时的分区依赖
ods_binlog_base_t_merchant_info_h_his_check_task = OssSensor(
    task_id='ods_binlog_base_t_merchant_info_h_his_check_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="ocredit_phones_h_his/ods_binlog_base_t_merchant_info_h_his",
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
         "partition": "country_code={country_code}/dt={pt}/hour={now_hour}".format(country_code=v_country_code,
                                                                                   pt=v_date, now_hour=v_hour),
         "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb_hour_task)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


def dwd_ocredit_merchant_info_hf_sql_task(ds, v_date):
    HQL = '''

    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;

    insert overwrite table {db}.{table} 
    partition(country_code, dt,hour)

    select id,
            merchant_id,                    --商户业务ID                                        
            business_type,                  --业务类型   0:手机    1:车                            
            merchant_code,                  --商家编号                                          
            merchant_name,                  --商家名称                                          
            register_code,                  --注册区域编号                                        
            business_address,               --注册地址                                          
            category_type,                  --0:个体户    1：连锁                                 
            business_life,                  --经营年限                                          
            employee_number,                --店员人数                                          
            branch_information,             --分店信息                                          
            contacter_name,                 --商户主要联系人姓名                                     
            contacter_position,             --商户主要联系人职位                                     
            contacter_phone,                --商户主要联系人电话                                     
            merchant_tele,                  --商户座机号码                                        
            inviter1_name,                  --邀请人1姓名                                        
            inviter1_phone,                 --邀请人1电话                                        
            inviter2_name,                  --邀请人2姓名                                        
            inviter2_phone,                 --邀请人2电话                                        
            country,                        --国家                                            
            city,                           --城市                                            
            corporation_name,               --法人姓名                                          
            corporation_sex,                --0:女   1:男                                     
            corporation_phone,              --法人电话                                          
            corporation_bvn,                --法人bvn号码                                       
            corporation_home_address,       --            法人居住地址                            
            corporation_email,              --  法人email                                     
            bank_user_name,                 --  收款方姓名                                       
            bank_name,                      --  收款方银行                                       
            bank_number,                    --  收款方银行卡号                                     
            merchant_status,                --  0:待审核    1:审核通过   2:审核拒绝    3暂停             
            audit_desc,                     --  审核描述                                        
            store_photo_url,                --  门店图片url                                     
            counter_photo_url,              --  柜台图片url                                     
            license_photo_url,              --  经营许可证图片url                                  
            corporation_election_card_url,  --            法人选举证照片                           
            shopkeeper_sa_photo_url,        --店主与销售经理合照                                     
            other_photo_url,                --其他照片                                          
            is_delete,                      --是否删除  1删除   0未删除                              
            default.localTime("{config}",'NG',create_time,0) as create_time,                    --创建时间                                          
            create_user_id,                 --创建人                                           
            default.localTime("{config}",'NG',update_time,0) as update_time,                    --更新时间                                          
            update_user_id,                 --更新人                                           
            is_lock,                        --是否锁定，0：否；1：是；                                 
            lock_holder_id,                 --锁持有者                                          
            payment_method,                 --收款方式：1.bank 2.OPAYAgent                       
            bank_code,                      --银行Code                                        
            is_collection,                  --是否统一收款 0否 1是                                  
            bank_confirmation_url,           --收款至银行卡确认函                                      
        utc_date_hour,
        'NG' country_code,  --如果表中有国家编码直接上传国家编码
        date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'yyyy-MM-dd') as dt,
        date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'HH') as hour

    from (select *,
                 date_format('{v_date}', 'yyyy-MM-dd HH') as utc_date_hour,
                 row_number() over(partition by id order by `__ts_ms` desc,`__file` desc,cast(`__pos` as int) desc) rn
             from ocredit_phones_dw_ods.ods_binlog_base_t_merchant_info_h_his
            where 
                concat(dt, " ", hour) = date_format('{v_date}', 'yyyy-MM-dd HH')
                and `__deleted` = 'false') m
        where rn=1;
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
            "is_hour_task": "true",
            "frame_type": "local",
            "business_key": "ocredit"
        }
    ]

    cf = CountriesAppFrame(args)

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dwd_ocredit_merchant_info_hf_sql_task(ds, v_date)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dwd_ocredit_merchant_info_hf_task = PythonOperator(
    task_id='dwd_ocredit_merchant_info_hf_task',
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

ods_binlog_base_t_merchant_info_h_his_check_task >> dwd_ocredit_merchant_info_hf_task
