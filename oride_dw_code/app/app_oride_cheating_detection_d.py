# -*- coding: utf-8 -*-
"""
中台promoter_data_day
需求 王德慧
"""
import airflow
from datetime import datetime, timedelta
from utils.connection_helper import get_hive_cursor, get_db_conn
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.sensors.s3_key_sensor import S3KeySensor
import logging
from airflow.sensors import OssSensor
from airflow.models import Variable
from airflow.sensors import UFileSensor




args = {
    'owner': 'wuduo',
    'start_date': datetime(2019, 10, 16),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'app_oride_cheating_detection_d',
    schedule_interval="30 3 * * *",
    default_args=args
)
##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "app_oride_cheating_detection_d"

##----------------------------------------- 依赖 ---------------------------------------##
#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":

    dwd_oride_order_base_include_test_di_task = S3KeySensor(
        task_id='dependence_dwd_oride_order_base_include_test_di_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-bi',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
        )

    # 依赖hive表分区

    dwd_oride_driver_cheating_detection_hi_task = UFileSensor(
        task_id="dwd_oride_driver_cheating_detection_hi_task",
        filepath='{hdfs_path_str}/dt={pt}/hour=23'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_driver_cheating_detection_hi/country_code=nal",
            pt='{{ ds }}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )



    ods_sqoop_promoter_promoter_user_df_task = UFileSensor(
        task_id='ods_sqoop_promoter_promoter_user_df_tesk',
        filepath="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
            hdfs_path_str="oride_dw_sqoop/opay_spread/promoter_user",
            pt="{{ds}}"
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )


    ods_sqoop_mass_rider_signups_df_task = UFileSensor(
        task_id='ods_sqoop_mass_rider_signups_df_task',
        filepath="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
            hdfs_path_str="oride_dw_sqoop/opay_spread/rider_signups",
            pt="{{ds}}"
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
else:
    print("成功")

    dwd_oride_order_base_include_test_di_task = OssSensor(
        task_id='dwd_oride_order_base_include_test_di_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    dwd_oride_driver_cheating_detection_hi_task = OssSensor(
        task_id="dwd_oride_driver_cheating_detection_hi_task",
        bucket_key='{hdfs_path_str}/dt={pt}/hour=23/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_driver_cheating_detection_hi/country_code=nal",
            pt='{{ ds }}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    ods_sqoop_promoter_promoter_user_df_task = OssSensor(
        task_id='ods_sqoop_promoter_promoter_user_df_tesk',
        bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
            hdfs_path_str="oride_dw_sqoop/opay_spread/promoter_user",
            pt="{{ds}}"
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    ods_sqoop_mass_rider_signups_df_task = OssSensor(
        task_id='ods_sqoop_mass_rider_signups_df_task',
        bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
            hdfs_path_str="oride_dw_sqoop/opay_spread/rider_signups",
            pt="{{ds}}"
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )


sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 10',
    dag=dag
)


def base_data(**op_kwargs):
    cursor = get_hive_cursor()
    dt = op_kwargs.get('ds')
    cursor.execute("SET mapred.job.queue.name=root.users.airflow")
    cursor.execute("SET hive.exec.parallel=true")
    hql = """
        SELECT
            t.code,
            from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'), 'yyyyMMdd') as day,
            COUNT(DISTINCT t.bind_number) as users_count,
            COUNT(DISTINCT if (length(t.bind_device)>0, t.bind_device, NULL)) as device_count, 
            unix_timestamp() 
        FROM oride_dw.dwd_oride_driver_cheating_detection_hi 
        LATERAL VIEW json_tuple(event_value, 'bind_refferal_code', 'bind_number', 'bind_device_id') t AS code, bind_number, bind_device 
        WHERE dt = '{ds}'
        GROUP BY t.code, dt
    """.format(ds=dt)
    logging.info(hql)
    cursor.execute(hql)
    res = cursor.fetchall()
    mconn = get_db_conn('opay_spread_mysql')
    mysql = mconn.cursor()
    sql = 'insert into promoter_data_day (code, day, users_count, device_count, create_time) values '
    ext = """ on duplicate key update 
        users_count=values(users_count), 
        device_count=values(device_count), 
        create_time=values(create_time)
    """
    vals = []
    for (code, day, users, device, t) in res:
        vals.append("('{code}', '{day}', '{user}', '{d}', '{t}')".format(
            code=code,
            day=day,
            user=users,
            d=device,
            t=t
        ))
        if len(vals) >= 1000:
            # logging.info(sql + ",".join(vals) + ext)
            mysql.execute(sql + ",".join(vals) + ext)
            vals = []

    if len(vals) > 0:
        # logging.info(sql + ",".join(vals) + ext)
        mysql.execute(sql + ",".join(vals) + ext)

    mysql.close()
    cursor.close()


base_data_task = PythonOperator(
    task_id='base_data_task',
    python_callable=base_data,
    provide_context=True,
    op_kwargs={
        "ds": '{{ ds }}'
    },
    dag=dag
)


def finish_data(**op_kwargs):
    cursor = get_hive_cursor()
    dt = op_kwargs.get('ds')
    cursor.execute("SET mapred.job.queue.name=root.users.airflow")
    cursor.execute("SET hive.exec.parallel=true")
    hql = """
        SELECT
            c.code,
            from_unixtime(unix_timestamp(o.dt,'yyyy-MM-dd'), 'yyyyMMdd') AS day,
            COUNT(DISTINCT o.orders_f) as orders_f, 
            unix_timestamp() 
        FROM (SELECT 
                user_id,
                get_json_object(event_value, '$.bind_refferal_code') AS code 
            FROM oride_dw.dwd_oride_driver_cheating_detection_hi 
            ) AS c 
        JOIN (SELECT 
                dt,
                passenger_id as user_id,
                order_id as orders_f 
            FROM oride_dw.dwd_oride_order_base_include_test_di  
            WHERE dt = '{ds}' AND 
                status IN (4,5) AND 
                from_unixtime(arrive_time, 'yyyy-MM-dd') = '{ds}' 
            ) AS o 
        ON c.user_id = o.user_id 
        GROUP BY c.code, o.dt 
    """.format(ds=dt)
    logging.info(hql)
    cursor.execute(hql)
    res = cursor.fetchall()
    mconn = get_db_conn('opay_spread_mysql')
    mysql = mconn.cursor()
    sql = 'insert into promoter_data_day (code, day, order_numbers, create_time) values '
    ext = ' on duplicate key update order_numbers=values(order_numbers), create_time=values(create_time)'
    vals = []
    for (c, d, o, t) in res:
        vals.append("('{c}', '{d}', '{o}', '{t}')".format(
            c=c,
            d=d,
            o=o,
            t=t
        ))
        if len(vals) >= 1000:
            # logging.info(sql + ",".join(vals) + ext)
            mysql.execute(sql + ",".join(vals) + ext)
            vals = []

    if len(vals) > 0:
        # logging.info(sql + ",".join(vals) + ext)
        mysql.execute(sql + ",".join(vals) + ext)

    mysql.close()
    cursor.close()


finish_data_task = PythonOperator(
    task_id='finish_data_task',
    python_callable=finish_data,
    provide_context=True,
    op_kwargs={
        "ds": '{{ ds }}'
    },
    dag=dag
)


def first_user_data(**op_kwargs):
    cursor = get_hive_cursor()
    dt = op_kwargs.get('ds')
    cursor.execute("SET mapred.job.queue.name=root.users.airflow")
    cursor.execute("SET hive.exec.parallel=true")
    hql = """
        SELECT 
            uc.code,
            from_unixtime(unix_timestamp(uo.dt,'yyyy-MM-dd'), 'yyyyMMdd') AS day,
            COUNT(DISTINCT uo.user_id) AS u, 
            unix_timestamp() 
        FROM (SELECT 
                user_id,
                get_json_object(event_value, '$.bind_refferal_code') AS code 
            FROM oride_dw.dwd_oride_driver_cheating_detection_hi 
            ) AS uc 
        JOIN (SELECT 
                dt,
                passenger_id as user_id,
                arrive_time,
                row_number() over(partition by passenger_id order by arrive_time) orders
            FROM oride_dw.dwd_oride_order_base_include_test_di 
            WHERE status IN (4,5) AND 
                dt = '{ds}' 
            ) AS uo 
        ON uc.user_id = uo.user_id 
        WHERE uo.orders = 1 and 
            from_unixtime(uo.arrive_time,'yyyy-MM-dd') = '{ds}' 
        GROUP BY uc.code, uo.dt
    """.format(ds=dt)
    logging.info(hql)
    cursor.execute(hql)
    res = cursor.fetchall()
    mconn = get_db_conn('opay_spread_mysql')
    mysql = mconn.cursor()
    sql = 'insert into promoter_data_day (code, day, pft, create_time) values '
    ext = ' on duplicate key update pft=values(pft), create_time=values(create_time)'
    vals = []
    for (c, d, p, t) in res:
        vals.append("('{c}', '{d}', '{p}', '{t}')".format(
            c=c,
            d=d,
            p=p,
            t=t
        ))
        if len(vals) >= 1000:
            # logging.info(sql + ",".join(vals) + ext)
            mysql.execute(sql + ",".join(vals) + ext)
            vals = []

    if len(vals) > 0:
        # logging.info(sql + ",".join(vals) + ext)
        mysql.execute(sql + ",".join(vals) + ext)

    mysql.close()
    cursor.close()


first_user_data_task = PythonOperator(
    task_id='first_user_data_task',
    python_callable=first_user_data,
    provide_context=True,
    op_kwargs={
        "ds": '{{ ds }}'
    },
    dag=dag
)


def first_driver_data(**op_kwargs):
    cursor = get_hive_cursor()
    dt = op_kwargs.get('ds')
    cursor.execute("SET mapred.job.queue.name=root.users.airflow")
    cursor.execute("SET hive.exec.parallel=true")
    hql = """
        SELECT 
            uc.code,
            from_unixtime(unix_timestamp(ro.dt,'yyyy-MM-dd'), 'yyyyMMdd') AS day,
            COUNT(distinct ro.driver_id) as u, 
            unix_timestamp() 
        FROM (SELECT  
                r.driver_id,
                p.code 
            FROM (SELECT 
                    driver_id,
                    know_orider_extend  
                FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df 
                WHERE dt = '{ds}' and 
                    know_orider = 4
                ) AS r 
            JOIN (select 
                    code, 
                    name 
                FROM oride_dw_ods.ods_sqoop_promoter_promoter_user_df 
                WHERE dt='{ds}' 
                ) AS p
            ON r.know_orider_extend = p.name 
            ) AS uc 
        JOIN (SELECT 
                dt,
                driver_id,
                arrive_time,
                row_number() over(partition by driver_id order by arrive_time) orders
            FROM oride_dw.dwd_oride_order_base_include_test_di 
            WHERE status IN (4,5) AND 
                dt = '{ds}' 
            ) as ro 
        ON uc.driver_id = ro.driver_id 
        WHERE ro.orders = 1 AND 
            from_unixtime(ro.arrive_time,'yyyy-MM-dd')='{ds}' 
        GROUP BY uc.code, ro.dt
    """.format(ds=dt)
    logging.info(hql)
    cursor.execute(hql)
    res = cursor.fetchall()
    mconn = get_db_conn('opay_spread_mysql')
    mysql = mconn.cursor()
    sql = 'insert into promoter_data_day (code, day, dft, create_time) values '
    ext = ' on duplicate key update dft=values(dft), create_time=values(create_time)'
    vals = []
    for (c, d, f, t) in res:
        vals.append("('{c}', '{d}', '{f}', '{t}')".format(
            c=c,
            d=d,
            f=f,
            t=t
        ))
        if len(vals) >= 1000:
            # logging.info(sql + ",".join(vals) + ext)
            mysql.execute(sql + ",".join(vals) + ext)
            vals = []

    if len(vals) > 0:
        # logging.info(sql + ",".join(vals) + ext)
        mysql.execute(sql + ",".join(vals) + ext)

    mysql.close()
    cursor.close()


first_driver_data_task = PythonOperator(
    task_id='first_driver_data_task',
    python_callable=first_driver_data,
    provide_context=True,
    op_kwargs={
        "ds": '{{ ds }}'
    },
    dag=dag
)

dwd_oride_order_base_include_test_di_task >> sleep_time
dwd_oride_driver_cheating_detection_hi_task >> sleep_time

ods_sqoop_promoter_promoter_user_df_task >> sleep_time
ods_sqoop_mass_rider_signups_df_task >> sleep_time

sleep_time >> base_data_task
sleep_time >> finish_data_task
sleep_time >> first_user_data_task
sleep_time >> first_driver_data_task
