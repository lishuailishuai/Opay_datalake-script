# -*- coding: utf-8 -*-
import airflow
from datetime import datetime, timedelta
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors import UFileSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import logging
import os,sys
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook


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


sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------## 


ods_sqoop_base_data_city_conf_df_tesk = UFileSensor(
    task_id='ods_sqoop_base_data_city_conf_df_prev_day_tesk',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_city_conf",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
ods_sqoop_base_weather_per_10min_df_prev_day_task = UFileSensor(
    task_id='ods_sqoop_base_weather_per_10min_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/bi/weather_per_10min",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------## 

table_name="test_dim_oride_city"
hdfs_path="ufile://opay-datalake/oride/oride_dw/"+table_name


##----------------------------------------- 任务超时监控 ---------------------------------------## 

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"db": "test_db", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)



##----------------------------------------- 脚本 ---------------------------------------## 


def test_dim_oride_city_sql_task(ds):

    HQL='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite TABLE test_db.{table} partition(country_code,dt)

SELECT city_id,
       --城市 ID

       city_name,
       --城市名称

       country_name,
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

       cty.country_code,
       --二位国家码
       
       '{pt}' AS dt
FROM
  (SELECT id AS city_id,
          --城市 ID

          name AS city_name,
          --城市名称

          country AS country_name,
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
          assign_type --强派的服务类型[1,2,3] 1 专车 2 快车 3 keke车

FROM oride_dw_ods.ods_sqoop_base_data_city_conf_df
   WHERE dt='{pt}') cit
LEFT OUTER JOIN
  (SELECT country_name_en,
          country_code
   FROM oride_dw.dim_oride_country_base) cty ON cit.country_name=cty.country_name_en
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
        now_day='macros.ds_add(ds, +1)',
        table=table_name
        )

    #logging.info(HQL)

    return HQL


#熔断数据，如果数据重复，报错
def check_key_data_task(ds):

    cursor = get_hive_cursor()

    #主键重复校验
    check_sql='''
    SELECT count(1)-count(distinct city_id) as cnt
      FROM test_db.{table}
      WHERE dt='{pt}'
      and country_code in ('NG')
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name
        )

    logging.info('Executing 主键重复校验: %s', check_sql)

    cursor.execute(check_sql)

    res = cursor.fetchone()
    print(res)

    if res[0] >1:
        flag=1
        raise Exception ("Error The primary key repeat !", res)
        sys.exit(1)
    else:
        flag=0
        print("-----> Notice Data Export Success ......")

    return flag


#主流程
def execution_data_task_id(ds,**kargs):
    hive_hook = HiveCliHook()
    #读取sql
    _sql=test_dim_oride_city_sql_task(ds)

    logging.info('Executing: %s', _sql)
    hive_hook.run_cli(_sql)
    #执行hive

    #sys.exit(0)

    #读取验证sql
    _check=check_key_data_task(ds)

    #生成_SUCCESS
    msg = [
        {"table":"{dag_name}".format(dag_name=dag.dag_id),"hdfs_path": "{hdfs_path}/country_code=NG/dt={pt}".format(pt=ds,hdfs_path=hdfs_path)}
    ]

    TaskTouchzSuccess().set_touchz_success(msg)
    

dim_oride_city_task= PythonOperator(
    task_id='dim_oride_city_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

ods_sqoop_base_data_city_conf_df_tesk>>sleep_time
ods_sqoop_base_weather_per_10min_df_prev_day_task>>sleep_time
sleep_time>>dim_oride_city_task