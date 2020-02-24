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
from airflow.sensors import OssSensor
from airflow.sensors.s3_key_sensor import S3KeySensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.CountriesPublicFrame_dev import CountriesPublicFrame_dev
from plugins.TaskHourSuccessCountMonitor import TaskHourSuccessCountMonitor
import json
import logging
from airflow.models import Variable
import requests
import os
from utils.get_local_time import GetLocalTime

args = {
        'owner': 'yangmingze',
        'start_date': datetime(2019, 5, 20),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=2),
        'email': ['bigdata_dw@opay-inc.com'],
        'email_on_failure': True,
        'email_on_retry': False,
} 

dag = airflow.DAG( 'test_dim_oride_city', 
    schedule_interval="00 01 * * *", 
    default_args=args,
    catchup=False) 
 

##----------------------------------------- 变量 ---------------------------------------## 
    
db_name="test_db"
table_name="test_dim_oride_city"
    
hdfs_path="oss://opay-datalake/oride/test_db/"+table_name
##----------------------------------------- 依赖 ---------------------------------------## 


#获取变量
#code_map=eval(Variable.get("sys_flag"))

config = eval(Variable.get("utc_locale_time_config"))
time_zone = config['NG']['time_zone']


test_snappy_dev_01_tesk = OssSensor(
    task_id='test_snappy_dev_01_tesk',
    bucket_key='{hdfs_path_str}/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="test",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(time_zone=time_zone,gap_hour=-1),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(time_zone=time_zone,gap_hour=-1)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


# test_snappy_dev_02_tesk = OssSensor(
#     task_id='test_snappy_dev_02_tesk',
#     bucket_key='{hdfs_path_str}/dt={pt}/hour={hour}/_SUCCESS'.format(
#         hdfs_path_str="test",
#         #pt=pt_date,
#         #pt=GetLocalTime('{{{{(execution_date+macros.timedelta(hours={time_zone})).strftime("%Y-%m-%d")}}}}', 'NG',-1)['date'],
#         #pt='{{{{(execution_date+macros.timedelta(hours={time_zone})).strftime("%Y-%m-%d")}}}}'.format(time_zone=2),
#         hour='{{{{(execution_date+macros.timedelta(hours={time_zone})).strftime("%H")}}}}'.format(time_zone=2)
#     ),
#     bucket_name='opay-datalake',
#     poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
#     dag=dag
# )


ods_sqoop_base_data_city_conf_df_tesk = UFileSensor(
    task_id='ods_sqoop_base_data_city_conf_df_tesk',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_city_conf",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------## 


# def fun_task_timeout_monitor(ds,dag,**op_kwargs):

#     dag_ids=dag.dag_id

#     tb = [
#         {"dag":dag,"db": "test_db", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "600"}
#     ]

#     # tb = [
#     #     {"db": "test_db", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "600"}
#     # ]

#     #TaskTimeoutMonitor().set_task_monitor(tb)

#     print(GetLocalTime('2020-02-20 10',"NG",-1))
#     print(GetLocalTime('2020-02-20 10',"NG",0))
#     print(GetLocalTime('2020-02-20 10',"NG",1))
#     print(GetLocalTime('2020-02-20 10',"NG",2))

#     print(GetLocalTime('2020-02-20 10',"NC",1))

# task_timeout_monitor= PythonOperator(
#     task_id='task_timeout_monitor',
#     python_callable=fun_task_timeout_monitor,
#     provide_context=True,
#     dag=dag
# )





def test_dim_oride_city_sql_task(ds):

    HQL='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite TABLE {db}.{table} partition(country_code,dt)

SELECT city_id,
       --城市 ID

       city_name,
       --城市名称

       nvl(cty.name,-1) as country_name,
       --国家名称

       shape,
       --形状：1 圆形, 2 多边形

       area,
       --区域设置数据

       product_id,
       --开启的服务类型[1,2,99] 1 专车 2 快车 99 招手停

       avoid_highway_type,
       --可设置避开高速的服务类型[1,2] 1 专车 2 快车

       validate,
       --本条数据是否有效 0 无效，1 有效
       
       weather.weather, 
       --该城市当天的天气

       opening_time,--开启时间       

       assign_type, --强派的服务类型[1,2,3] 1 专车 2 快车 3 keke车

       nvl(cty.country_code,'nal') as country_code,
       --二位国家码
       
       '{pt}' AS dt
FROM
  (SELECT id AS city_id,
          --城市 ID

          name AS city_name,
          --城市名称

          country,
          --国家

          shape,
          --形状：1 圆形, 2 多边形

          area,
          --区域设置数据

          serv_type AS product_id,
          --开启的服务类型[1,2,99] 1 专车 2 快车 99 招手停

          avoid_highway_type,
          --可设置避开高速的服务类型[1,2] 1 专车 2 快车

          validate, --本条数据是否有效 0 无效，1 有效

          opening_time,--开启时间                
          assign_type,--强派的服务类型[1,2,3] 1 专车 2 快车 3 keke车

          allow_flagdown_type, --允许招手停的服务类型 
                                
          country_id --国家ID 

FROM oride_dw_ods.ods_sqoop_base_data_city_conf_df
   WHERE dt='{pt}') cit
LEFT OUTER JOIN
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_base_data_country_conf_df 
   WHERE dt='{pt}') cty ON cit.country_id=cty.id
left outer join
(SELECT t.city AS city,
              t.weather AS weather
FROM
  ( SELECT t.city,
           t.weather,
           row_number() over(partition BY t.city
                             ORDER BY t.counts DESC) row_num  --城市一天的天气状况取一天当中某种天气持续最长时间的
   FROM
     ( SELECT city,
              weather,
              count(1) counts
      FROM oride_dw_ods.ods_sqoop_base_weather_per_10min_df
      WHERE dt = '{pt}'
        AND daliy = '{pt}'
      GROUP BY city,
               weather ) t ) t
WHERE t.row_num = 1) weather
on lower(cit.city_name)=lower(weather.city)

'''.format(
        pt=ds,
        #now_day='{{macros.ds_add(ds, +1)}}',
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
        )
    return HQL


#熔断数据，如果数据为0，报错
def check_key_data_cnt_task(ds):

    cursor = get_hive_cursor()

    #主键重复校验
    check_sql='''
    SELECT count(1) as cnt
      FROM {db}.{table}
      WHERE dt='{pt}'
      and country_code in ('NG')
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
        )

    logging.info('Executing 主键重复校验: %s', check_sql)

    cursor.execute(check_sql)

    res = cursor.fetchone()
 
    if res[0] ==0:
        flag=1
        raise Exception ("Error The primary key repeat !", res)
        sys.exit(1)
    else:
        flag=0
        print("-----> Notice Data Export Success ......")

    return flag


#熔断数据，如果数据重复，报错
def check_key_data_task(ds):

    cursor = get_hive_cursor()

    #主键重复校验
    check_sql='''
    SELECT count(1)-count(distinct city_id) as cnt
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
 
    if res[0] >1:
        flag=1
        raise Exception ("Error The primary key repeat !", res)
        sys.exit(1)
    else:
        flag=0
        print("-----> Notice Data Export Success ......")

    return flag



#主流程
def execution_data_task_id(ds,dag,**kwargs):

    v_date=kwargs.get('v_execution_date')
    v_day=kwargs.get('v_execution_day')
    v_hour=kwargs.get('v_execution_hour')

    #hive_hook = HiveCliHook()

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

    args=[
            {
            "dag":dag,
            "is_countries_online":"true",
            "db_name":db_name,
            "table_name":table_name,
            "data_oss_path":hdfs_path,
            "is_country_partition":"true",
            "is_result_force_exist":"true",
            "execute_time":v_date,
            "is_hour_task":"true",
            "frame_type":"local"
            }
    ]


    cf=CountriesPublicFrame_dev(args)

    #删除分区
    #cf.delete_partition()

    #读取sql
    #_sql="\n"+cf.alter_partition()+"\n"+test_dim_oride_city_sql_task(ds)

    #logging.info('Executing: %s',_sql)

    #执行Hive
    #hive_hook.run_cli(_sql)

    #熔断数据，如果数据不能为0
    #check_key_data_cnt_task(ds)

    #熔断数据
    #check_key_data_task(ds)

    #生产success
    cf.touchz_success()

    
    
test_dim_oride_city_task= PythonOperator(
    task_id='test_dim_oride_city_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date':'{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day':'{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour':'{{execution_date.strftime("%H")}}',
        'owner':'{{owner}}'
    },
    dag=dag
)

# test_snappy_dev_01_tesk>>test_dim_oride_city_task
# ods_sqoop_base_data_city_conf_df_tesk>>test_dim_oride_city_task
# ods_sqoop_base_data_country_conf_df_tesk>>test_dim_oride_city_task
# ods_sqoop_base_weather_per_10min_df_task>>test_dim_oride_city_task
