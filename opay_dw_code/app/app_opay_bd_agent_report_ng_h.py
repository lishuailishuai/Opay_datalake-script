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
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os
from utils.get_local_time import GetLocalTime
from plugins.CountriesAppFrame import CountriesAppFrame

args = {
    'owner': 'xiedong',
    'start_date': datetime(2020, 3, 27),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opay_bd_agent_report_ng_h',
                  schedule_interval="40 * * * *",
                  default_args=args,
                  )
##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw"
table_name = "app_opay_bd_agent_report_ng_h"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name
config = eval(Variable.get("opay_time_zone_config"))
time_zone = config['NG']['time_zone']
##----------------------------------------- 依赖 ---------------------------------------##
dwm_opay_bd_agent_trans_hf_check_task = OssSensor(
    task_id='dwm_opay_bd_agent_trans_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwm_opay_bd_agent_trans_hf",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)

    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dim_opay_bd_relation_hf_check_task = OssSensor(
    task_id='dim_opay_bd_relation_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dim_opay_bd_relation_hf",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)

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

    v_date = GetLocalTime("opay", execution_date.strftime("%Y-%m-%d %H"), v_country_code, v_gap_hour)['date']
    v_hour = GetLocalTime("opay", execution_date.strftime("%Y-%m-%d %H"), v_country_code, v_gap_hour)['hour']

    # 小时级监控
    tb_hour_task = [
        {"dag": dag, "db": "opay_dw", "table": "{dag_name}".format(dag_name=dag_ids),
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


def app_opay_bd_agent_report_ng_h_sql_task(ds, v_date, v_date_hour):
    HQL = '''

    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    create table if not exists test_db.bd_report_temp_{v_date_hour} as
        select 
            nvl(job_bd_user_id, 'ALL') as job_bd_user_id, 
            nvl(job_bdm_user_id, 'ALL') as job_bdm_user_id, 
            nvl(job_rm_user_id, 'ALL') as job_rm_user_id,
            nvl(job_cm_user_id, 'ALL') as job_cm_user_id,
            nvl(job_hcm_user_id, 'ALL') as job_hcm_user_id, 
            nvl(job_pic_user_id, 'ALL') as job_pic_user_id, 
                 
            sum(audit_suc_cnt) as audited_agent_cnt,
            sum(audit_fail_cnt) as rejected_agent_cnt,
            
            sum(ci_suc_order_cnt) as ci_suc_order_cnt,
            sum(ci_suc_order_amt) as ci_suc_order_amt,
                    
            sum(co_suc_order_cnt) as co_suc_order_cnt,
            sum(co_suc_order_amt) as co_suc_order_amt,
                         
            sum(pos_suc_amt) as pos_suc_amt,
            sum(pos_suc_cnt) as pos_suc_cnt
        from (
            select 
                bd_admin_user_id, 
                audit_suc_cnt, 
                audit_fail_cnt,
                ci_suc_order_cnt,
                ci_suc_order_amt,
                co_suc_order_cnt,
                co_suc_order_amt,
                pos_suc_amt,
                pos_suc_cnt
            from opay_dw.dwm_opay_bd_agent_trans_hf 
            where  country_code='NG'
                and concat(dt,' ',hour) = date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'yyyy-MM-dd HH')
        ) t1 join (
            select 
                bd_admin_user_id, bd_admin_user_name, bd_admin_job_id, bd_admin_status, 
                job_bd_user_id, job_bdm_user_id, job_rm_user_id, job_cm_user_id, job_hcm_user_id, job_pic_user_id
            from opay_dw.dim_opay_bd_relation_hf
            where  country_code='NG'
                and concat(dt,' ',hour) = date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'yyyy-MM-dd HH')
        ) t2 on t1.bd_admin_user_id = t2.bd_admin_user_id
        group by job_bd_user_id, job_bdm_user_id, job_rm_user_id, job_cm_user_id, job_hcm_user_id, job_pic_user_id
        grouping sets(
            job_bd_user_id, job_bdm_user_id, job_rm_user_id, 
            job_cm_user_id, job_hcm_user_id, job_pic_user_id
        );
    with 
        bd_data as (
            select 
                bd_admin_user_id, bd_admin_user_name, bd_admin_mobile,
                department_id, bd_admin_job_id, bd_admin_leader_id
            from opay_dw.dim_opay_bd_admin_user_hf 
            where country_code='NG'
                and concat(dt,' ',hour) = date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'yyyy-MM-dd HH') 
                and bd_admin_job_id > 0
        ),
        cube_data as (
            select 
                job_bd_user_id, 
                job_bdm_user_id, 
                job_rm_user_id,
                job_cm_user_id,
                job_hcm_user_id, 
                job_pic_user_id, 
                audited_agent_cnt,
                rejected_agent_cnt,
                ci_suc_order_cnt,
                ci_suc_order_amt,
                co_suc_order_cnt,
                co_suc_order_amt,
                pos_suc_amt, 
                pos_suc_cnt
            from test_db.bd_report_temp_{v_date_hour}
        )
    
    INSERT overwrite TABLE {db}.{table} partition (country_code, dt,hour)
    select 
        date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'yyyy-MM-dd HH'),
        bd.bd_admin_user_id, bd.bd_admin_user_name, bd.bd_admin_mobile, 
        bd.department_id, bd.bd_admin_job_id, bd.bd_admin_leader_id,
        audited_agent_cnt, rejected_agent_cnt,
        ci_suc_order_cnt, ci_suc_order_amt,
        co_suc_order_cnt, co_suc_order_amt,
        pos_suc_amt, pos_suc_cnt,
        'NG' as country_code,
        date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'yyyy-MM-dd'),
        date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'HH')
    from (
        select 
            job_bd_user_id as bd_admin_user_id, 
            audited_agent_cnt, rejected_agent_cnt,
            ci_suc_order_cnt, ci_suc_order_amt,
            co_suc_order_cnt, co_suc_order_amt,
            pos_suc_amt, pos_suc_cnt
        from cube_data 
        where job_bd_user_id != 'ALL' and job_bd_user_id != '-'
        union all
        select 
            job_bdm_user_id as bd_admin_user_id, 
            audited_agent_cnt, rejected_agent_cnt,
            ci_suc_order_cnt, ci_suc_order_amt,
            co_suc_order_cnt, co_suc_order_amt,
            pos_suc_amt, pos_suc_cnt
        from cube_data 
        where job_bdm_user_id != 'ALL' and job_bdm_user_id != '-'
        union all
        select 
            job_rm_user_id as bd_admin_user_id, 
            audited_agent_cnt, rejected_agent_cnt,
            ci_suc_order_cnt, ci_suc_order_amt,
            co_suc_order_cnt, co_suc_order_amt,
            pos_suc_amt, pos_suc_cnt
        from cube_data 
        where job_rm_user_id != 'ALL' and job_rm_user_id != '-'
        union all
        select 
            job_cm_user_id as bd_admin_user_id,
            audited_agent_cnt, rejected_agent_cnt,
            ci_suc_order_cnt, ci_suc_order_amt,
            co_suc_order_cnt, co_suc_order_amt,
            pos_suc_amt, pos_suc_cnt
        from cube_data 
        where job_cm_user_id != 'ALL' and job_cm_user_id != '-'
        union all
        select 
            job_hcm_user_id as bd_admin_user_id,
            audited_agent_cnt, rejected_agent_cnt,
            ci_suc_order_cnt, ci_suc_order_amt,
            co_suc_order_cnt, co_suc_order_amt,
            pos_suc_amt, pos_suc_cnt
        from cube_data 
        where job_hcm_user_id != 'ALL' and job_hcm_user_id != '-'
        union all
        select 
            job_pic_user_id as bd_admin_user_id,
            audited_agent_cnt, rejected_agent_cnt,
            ci_suc_order_cnt, ci_suc_order_amt,
            co_suc_order_cnt, co_suc_order_amt,
            pos_suc_amt, pos_suc_cnt
            from cube_data 
            where job_pic_user_id != 'ALL' and job_pic_user_id != '-'
        ) report 
        left join bd_data bd on report.bd_admin_user_id = bd.bd_admin_user_id ;

        DROP TABLE IF EXISTS test_db.bd_report_temp_{v_date_hour}
    '''.format(
        pt=ds,
        v_date=v_date,
        table=table_name,
        db=db_name,
        config=config,
        v_date_hour=v_date_hour
    )
    return HQL


def execution_data_task_id(ds, dag, **kwargs):
    v_date = kwargs.get('v_execution_date')
    v_date_hour = kwargs.get('v_date_hour')
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
            is_offset --是否开启时间前后偏移(影响success 文件)
            execute_time_offset --执行时间偏移值(-1、0、1),在当前执行时间上，前后偏移原有时间，用于产出前后小时分区
            business_key --产品线名称

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
            "is_offset": "true",
            "execute_time_offset": -1,
            "business_key": "opay"
        }
    ]

    cf = CountriesAppFrame(args)

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + app_opay_bd_agent_report_ng_h_sql_task(ds, v_date, v_date_hour)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


app_opay_bd_agent_report_ng_h_task = PythonOperator(
    task_id='app_opay_bd_agent_report_ng_h_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}',
        'v_date_hour': '{{execution_date.strftime("%Y%m%d%H")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)

dwm_opay_bd_agent_trans_hf_check_task >> app_opay_bd_agent_report_ng_h_task
dim_opay_bd_relation_hf_check_task >> app_opay_bd_agent_report_ng_h_task

