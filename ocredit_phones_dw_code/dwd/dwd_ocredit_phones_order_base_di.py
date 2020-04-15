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

dag = airflow.DAG('dwd_ocredit_phones_order_base_di',
                  schedule_interval="30 00 * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "ocredit_phones_dw"
table_name = "dwd_ocredit_phones_order_base_di"
hdfs_path = "oss://opay-datalake/ocredit_phones/ocredit_phones_dw/" + table_name
config = eval(Variable.get("ocredit_time_zone_config"))
time_zone = config['NG']['time_zone'] #这里要依赖最晚时区的国家
##----------------------------------------- 依赖 ---------------------------------------##

### 检查本地时间t-1的依赖
dwd_ocredit_phones_order_base_hi_check_task = OssSensor(
    task_id='dwd_ocredit_phones_order_base_hi_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={dt}/hour=23/_SUCCESS'.format(
        hdfs_path_str="ocredit_phones/ocredit_phones_dw/dwd_ocredit_phones_order_base_hi",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        dt='{{ds}}'
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
         "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb_hour_task)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


def dwd_ocredit_phones_order_base_di_sql_task(ds, v_date):
    HQL = '''

    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;

    insert overwrite table {db}.{table} 
    partition(country_code, dt)

    select id, --无业务含义主键 
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
          create_time, --创建时间 
          update_time, --更新时间 
          loan_time, --放款时间 
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
          date_of_entry, --进件日期
          country_code,  --如果表中有国家编码直接上传国家编码
          dt
    from (select *,row_number() over(partition by id order by utc_date_hour desc) as rn 
          from ocredit_phones_dw.dwd_ocredit_phones_order_base_hi
          where dt = date_format("{v_date}", 'yyyy-MM-dd') 
          and (date_of_entry='{pt}' or substr(update_time,1,10)='{pt}')  --后续正常上线后，这个条件可以不限定，只是初始化当天需要限定
          and user_id not in 
        (
        '1209783514507214849', 
        '1209126038292123650',
        '1210903150317494274',
        '1214471918163460097',
        '1215642304343425026',
        '1226878328587288578'
        )
        and business_type = '0'
    ) t where t.rn=1;
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
    _sql = "\n" + cf.alter_partition() + "\n" + dwd_ocredit_phones_order_base_di_sql_task(ds, v_date)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dwd_ocredit_phones_order_base_di_task = PythonOperator(
    task_id='dwd_ocredit_phones_order_base_di_task',
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

dwd_ocredit_phones_order_base_hi_check_task >> dwd_ocredit_phones_order_base_di_task
