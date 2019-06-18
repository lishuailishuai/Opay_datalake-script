import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.hive_hooks import HiveCliHook
import json
import logging
from airflow.models import Variable
import requests
import os


args = {
    'owner': 'root',
    'start_date': datetime(2019, 5, 20),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    'oride_daily',
    schedule_interval="00 02 * * *",
    default_args=args)

create_oride_driver_overview  = HiveOperator(
    task_id='create_oride_driver_overview',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_driver_overview(
          driver_id bigint,
          order_status int,
          payment_mode int,
          payment_status int,
          order_num_total bigint,
          price_total decimal(10,2),
          distance_total bigint,
          duration_total bigint
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET

    """,
    schema='dashboard',
    dag=dag)

insert_oride_driver_overview  = HiveOperator(
    task_id='insert_oride_driver_overview',
    hql="""
        INSERT OVERWRITE TABLE oride_driver_overview PARTITION (dt='{{ ds }}')
        SELECT
            o.driver_id,
            o.status,
            p.mode,
            p.status,
            count(o.id),
            sum(o.price),
            sum(o.distance),
            sum(o.duration)
        FROM
            oride_db.data_order o
            INNER JOIN
            oride_db.data_order_payment p on o.id=p.id and o.dt=p.dt
        WHERE
            o.dt = '{{ ds }}' and from_unixtime(o.create_time,'yyyy-MM-dd')='{{ ds }}'
        GROUP BY
            o.driver_id, o.status, p.mode, p.status
    """,
    schema='dashboard',
    dag=dag)

create_oride_user_overview  = HiveOperator(
    task_id='create_oride_user_overview',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_user_overview(
          user_id bigint,
          is_new boolean,
          order_status int,
          payment_mode int,
          payment_status int,
          order_num_total bigint,
          price_total decimal(10,2),
          distance_total bigint,
          duration_total bigint
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET

    """,
    schema='dashboard',
    dag=dag)

insert_oride_user_overview  = HiveOperator(
    task_id='insert_oride_user_overview',
    hql="""
        INSERT OVERWRITE TABLE oride_user_overview PARTITION (dt='{{ ds }}')
        SELECT
            u.user_id,
            u.is_new,
            od.order_status,
            od.payment_mode,
            od.payment_status,
            od.order_num_total,
            od.price_total,
            od.distance_total,
            od.duration_total
        FROM
            (
                SELECT
                    user_id,
                    max(is_new) as is_new
                FROM
                    oride_source.user_login
                WHERE
                    dt='{{ ds }}'
                GROUP BY
                    user_id
            ) u
            LEFT JOIN (
                SELECT
                    o.user_id as user_id,
                    o.status as order_status,
                    p.mode as payment_mode,
                    p.status as payment_status,
                    count(o.id) as order_num_total,
                    sum(o.price) as price_total,
                    sum(o.distance) as distance_total,
                    sum(o.duration) as duration_total
                FROM
                    oride_db.data_order o
                    INNER JOIN
                    oride_db.data_order_payment p on o.id=p.id and o.dt=p.dt
                WHERE
                    o.dt = '{{ ds }}' and from_unixtime(o.create_time,'yyyy-MM-dd')='{{ ds }}'
                GROUP BY
                    o.user_id, o.status, p.mode, p.status
            ) od ON u.user_id = od.user_id
    """,
    schema='dashboard',
    dag=dag)

# 给用户打标签
create_oride_user_label  = HiveOperator(
    task_id='create_oride_user_lable',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_user_label(
          user_id bigint,
          lab_new_user boolean,
          lab_login_without_orders boolean,
          lab_login_have_orders boolean,
          lab_cancel_ge_finish boolean,
          phone_number string
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET
    """,
    schema='dashboard',
    dag=dag)

# 1. 新用户, 2, 1周有多次登录但是没有打过车，3，一周多次登录 多次打车，4，取消大于完成
insert_oride_user_label  = HiveOperator(
    task_id='insert_oride_user_lable',
    hql="""
        ALTER TABLE oride_user_label DROP IF EXISTS PARTITION (dt='{{ ds }}');
        -- 一周登录情况 汇总登录次数
        WITH week_login as (
            SELECT
              user_id,
              count(distinct dt) as login_num
            FROM
              oride_source.user_login
            WHERE
              dt BETWEEN '{{ macros.ds_add(ds, -7) }}' AND '{{ ds }}'
            GROUP BY
              user_id
        ),
        -- 一周订单情况 统计完成取消
        week_order as (
            SELECT
              user_id,
              count(if(status=6, 1, null)) as cancel_num,
              count(if(status=5, 1, null)) as finished_num
            FROM
              oride_db.data_order
            WHERE
              dt = '{{ ds }}' AND from_unixtime(create_time,'yyyy-MM-dd') BETWEEN '{{ macros.ds_add(ds, -7) }}' AND '{{ ds }}'
            GROUP BY
              user_id
        )
        INSERT OVERWRITE TABLE oride_user_label PARTITION (dt='{{ ds }}')
        SELECT
          ue.id,
          if(datediff('{{ ds }}', from_unixtime(ue.register_time,'yyyy-MM-dd')) < 7, true, false),
          if(nvl(wl.login_num,0)>1 and nvl(wo.finished_num, 0) < 1, true, false),
          if(nvl(wl.login_num,0)>1 and nvl(wo.finished_num, 0) > 1, true, false),
          if(nvl(wo.cancel_num, 0) > nvl(wo.finished_num,0), true, false),
          du.phone_number
        FROM
          oride_db.data_user_extend ue
          INNER JOIN oride_db.data_user du ON du.id=ue.id AND du.dt=ue.dt
          LEFT JOIN week_login wl ON wl.user_id = ue.id
          LEFT JOIN week_order wo ON wo.user_id = ue.id
        WHERE
          ue.dt='{{ ds }}'
    """,
    schema='dashboard',
    dag=dag)

def user_label_to_redis(ds, **kwargs):
    label_list = {
        'lab_new_user' : 1,
        'lab_login_without_orders' : 2,
        'lab_login_have_orders' : 3,
        'lab_cancel_ge_finish' : 4
    }
    query = """
        SELECT
          user_id,
          lab_new_user,
          lab_login_without_orders,
          lab_login_have_orders,
          lab_cancel_ge_finish,
          phone_number
        FROM
          dashboard.oride_user_label
        WHERE
          dt='{dt}'
    """.format(dt=ds)
    cursor = get_hive_cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    redis_conn = RedisHook(redis_conn_id='redis_user_lab').get_conn()
    expire_time = 86400
    for user_id, lab_new_user, lab_login_without_orders,lab_login_have_orders,lab_cancel_ge_finish,phone_number in results:
        list = []
        if lab_new_user == True:
            list.append(label_list['lab_new_user'])
        if lab_login_without_orders == True:
            list.append(label_list['lab_login_without_orders'])
        if lab_login_have_orders == True:
            list.append(label_list['lab_login_have_orders'])
        if lab_cancel_ge_finish == True:
            list.append(label_list['lab_cancel_ge_finish'])
        if len(list):
            redis_key = 'user_tag_%s' % phone_number
            redis_conn.set(redis_key, json.dumps(list), ex=expire_time)
            logging.info('user_id:%s, lab_list:%s, key:%s, phone_number:%s' % (user_id, json.dumps(list), redis_key, phone_number))
    cursor.close()

user_label_export = PythonOperator(
    task_id='user_label_export',
    python_callable=user_label_to_redis,
    provide_context=True,
    dag=dag
)

def import_opay_install(ds, **kwargs):
    # download report
    api_url = "https://hq.appsflyer.com/export/team.opay.pay/installs_report/v5?api_token={api_token}&from={dt}&to={dt}&additional_fields=install_app_store,match_type,contributor1_match_type,contributor2_match_type,contributor3_match_type,device_category,gp_referrer,gp_click_time,gp_install_begin,amazon_aid,keyword_match_type".format(api_token=Variable.get("opay_appsflyer_api_token"), dt=ds)
    headers = {'Accept':'text/csv'}
    response = requests.get(
        api_url,
        headers=headers
    )
    logging.info('url:{} response_len:{}'.format(response.url, len(response.content)))
    tmp_path = '/tmp/'
    file_name = 'appsflyer_opay_install_log_'+ds
    tmp_file = tmp_path + file_name
    with open(tmp_file, 'wb') as f:
        f.write(response.content)
    # upload to ufile
    upload_cmd = '/root/filemgr/filemgr  --action mput --bucket opay-datalake --key oride/appsflyer/opay_install_log/dt={dt}/{file_name}  --file {tmp_file}'.format(dt=ds, file_name=file_name,tmp_file=tmp_file)

    os.system(upload_cmd)
    # clear tmp file
    clear_cmd = 'rm -f %s' % tmp_file
    os.system(clear_cmd)

import_opay_install_log = PythonOperator(
    task_id='import_opay_install_log',
    python_callable=import_opay_install,
    provide_context=True,
    dag=dag
)

create_opay_media_summary  = HiveOperator(
    task_id='create_opay_media_summary',
    hql="""
        CREATE TABLE IF NOT EXISTS opay_media_summary (
          media_source string,
          install_num int,
          register_num int
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET
    """,
    schema='dashboard',
    dag=dag)

insert_opay_media_summary  = HiveOperator(
    task_id='insert_opay_media_summary',
    hql="""
        ALTER TABLE oride_source.appsflyer_opay_install_log ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}');
        ALTER TABLE opay_media_summary DROP IF EXISTS PARTITION (dt='{{ macros.ds_add(ds, -1) }}');
        ALTER TABLE opay_media_summary DROP IF EXISTS PARTITION (dt='{{ ds }}');
        set hive.exec.dynamic.partition.mode=nonstrict;
        INSERT OVERWRITE TABLE opay_media_summary PARTITION (dt)
        SELECT
          a.media_source,
          count(distinct a.appsflyer_id) as install_num,
          count(distinct u.appsflyer_id) as register_num,
          a.dt
        FROM
          oride_source.appsflyer_opay_install_log a
          left join oride_source.user_login u on u.appsflyer_id=a.appsflyer_id and u.dt BETWEEN  '{{ macros.ds_add(ds, -1) }}' and '{{ds}}'
        WHERE
          a.dt BETWEEN '{{ macros.ds_add(ds, -1) }}' and '{{ds}}'
        GROUP BY
          a.dt,
          a.media_source
    """,
    schema='dashboard',
    dag=dag)

create_opay_media_summary  = HiveOperator(
    task_id='create_opay_media_summary',
    hql="""
        CREATE TABLE IF NOT EXISTS opay_media_summary (
          media_source string,
          install_num int,
          register_num int
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET
    """,
    schema='dashboard',
    dag=dag)

create_driver_online_time  = HiveOperator(
    task_id='create_driver_online_time',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_driver_online_time (
          driver_id int,
          online_time int
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET
    """,
    schema='dashboard',
    dag=dag)

def import_driver_online_time_to_hive(ds, ds_nodash, **kwargs):
    redis_conn = RedisHook(redis_conn_id='pika').get_conn()
    cursor=0
    count=100
    rows = []
    while True:
        result = redis_conn.scan(cursor, 'online_time:time:2:*:%s' % ds_nodash, count)
        cursor = result[0]
        if cursor == 0:
            break
        for k in result[1]:
            v = redis_conn.get(k)
            logging.info('k:%s, v:%s' % (k, v))
            tmp = str(k, 'utf-8').split(':')
            rows.append('('+ tmp[3] + ','+ str(v,'utf-8') +')')
    if rows:
        query = """
            INSERT OVERWRITE TABLE dashboard.oride_driver_online_time PARTITION (dt='{dt}')
            VALUES {value}
        """.format(dt=ds, value=','.join(rows))
        hive_hook = HiveCliHook()
        hive_hook.run_cli(query)

import_driver_online_time = PythonOperator(
    task_id='import_driver_online_time',
    python_callable=import_driver_online_time_to_hive,
    provide_context=True,
    dag=dag
)


create_coupon_summary = HiveOperator(
    task_id='create_coupon_summary',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_coupon_summary (
          template_id bigint,
          receive_users bigint,
          receive_times bigint,
          used_users bigint,
          used_times bigint
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET
    """,
    schema='dashboard',
    dag=dag)

insert_coupon_summary  = HiveOperator(
    task_id='insert_coupon_summary',
    hql="""
        ALTER TABLE oride_coupon_summary DROP IF EXISTS PARTITION (dt='{{ ds }}');
        INSERT OVERWRITE TABLE oride_coupon_summary PARTITION (dt='{{ ds }}')
        SELECT
            template_id,
            count(distinct if (from_unixtime(receive_time, 'yyyy-MM-dd')=dt, user_id, null)) as receive_users,
            sum(if (from_unixtime(receive_time, 'yyyy-MM-dd')=dt, 1, 0)) as receive_times,
            count(distinct if (from_unixtime(used_time, 'yyyy-MM-dd')=dt, user_id, null)) as used_users,
            sum(if (from_unixtime(used_time, 'yyyy-MM-dd')=dt, 1, 0)) as used_times
        FROM
            oride_db.data_coupon
        WHERE
            dt = '{{ ds }}'
        GROUP BY template_id
    """,
    schema='dashboard',
    dag=dag)

create_order_event_summary = HiveOperator(
    task_id='create_order_event_summary',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_order_event_summary (
          status int,
          order_num int,
          drivers int,
          users int,
          price_total int,
          reward_total int
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET
    """,
    schema='dashboard',
    dag=dag)

insert_order_event_summary  = HiveOperator(
    task_id='insert_order_event_summary',
    hql="""
        ALTER TABLE oride_order_event_summary DROP IF EXISTS PARTITION (dt='{{ ds }}');
        INSERT OVERWRITE TABLE oride_order_event_summary PARTITION (dt='{{ ds }}')
        SELECT
            status,
            count(distinct order_id) as order_num,
            count(distinct driver_id) as drivers,
            count(distinct user_id) as users,
            sum(price) as price_total,
            sum(reward) as reward_total
        FROM
            oride_source.user_order
        WHERE
          dt='{{ ds }}'
        GROUP BY status
    """,
    schema='dashboard',
    dag=dag)

create_oride_driver_daily_summary = HiveOperator(
    task_id='create_oride_driver_daily_summary',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_driver_daily_summary (
            driver_id bigint,
            phone_number string,
            real_name string,
            group_id int,
            group_name string,
            group_leader string,
            online_time bigint,
            comment_scores int,
            comment_times int,
            order_num int,
            order_finished_num int,
            order_cancel_num int,
            duration_total int,
            distance_total int,
            peak_time_order_num int
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET
    """,
    schema='dashboard',
    dag=dag)

insert_oride_driver_daily_summary  = HiveOperator(
    task_id='insert_oride_driver_daily_summary',
    hql="""
        ALTER TABLE oride_driver_daily_summary DROP IF EXISTS PARTITION (dt='{{ ds }}');
        with online_time as (
            select
                driver_id,
                online_time
            from dashboard.oride_driver_online_time where dt='{{ ds }}'
        ),
        driver_comment as (
            select
                driver_id,
                count(id) as comment_times,
                sum(score) as comment_scores
            from
                oride_db.data_driver_comment where dt='{{ ds }}' and from_unixtime(create_time,'yyyy-MM-dd')='{{ ds }}' group by driver_id
        ),
        order_data as (
            select
                driver_id,
                sum(if(status=5, 1, 0)) as order_finished_num,
                count(id) as order_num,
                sum(if(status=6 and cancel_role=2, 1, 0)) as order_cancel_num,
                sum(if(status=5, duration, 0)) as duration_total,
                sum(if(status=5, distance, 0)) as distance_total,
                sum(if(status=5 and cast(from_unixtime(create_time,'HH') as int)>=16 and cast(from_unixtime(create_time,'HH') as int)<20, 1, 0)) as peak_time_order_num
            from
                oride_db.data_order
            where
                dt='{{ ds }}' and from_unixtime(create_time,'yyyy-MM-dd')='{{ ds }}'
            group by driver_id
        )
        INSERT OVERWRITE TABLE oride_driver_daily_summary PARTITION (dt='{{ ds }}')
        SELECT
            dd.id,
            dd.phone_number,
            dd.real_name,
            dd.group_id,
            ddg.group_name,
            ddg.group_leader,
            nvl(ot.online_time,0),
            nvl(dc.comment_scores,0),
            nvl(dc.comment_times,0),
            nvl(od.order_num,0),
            nvl(od.order_finished_num,0),
            nvl(od.order_cancel_num,0),
            nvl(od.duration_total,0),
            nvl(od.distance_total,0),
            nvl(od.peak_time_order_num, 0)
        FROM
            oride_db.data_driver dd
            left join online_time ot on ot.driver_id=dd.id
            left join driver_comment dc on dc.driver_id=dd.id
            left join order_data od on od.driver_id=dd.id
            left join oride_db.data_driver_group ddg on ddg.id=dd.group_id AND ddg.dt=dd.dt
        WHERE
            dd.dt='{{ ds }}'
    """,
    schema='dashboard',
    dag=dag)

refresh_impala = ImpalaOperator(
    task_id = 'refresh_impala',
    hql="""\
        REFRESH dashboard.oride_driver_overview PARTITION (dt='{{ds}}');
        REFRESH dashboard.oride_user_overview PARTITION (dt='{{ds}}');
        REFRESH dashboard.oride_user_label PARTITION (dt='{{ds}}');
        REFRESH dashboard.opay_media_summary PARTITION (dt='{{macros.ds_add(ds, -1)}}');
        REFRESH dashboard.opay_media_summary PARTITION (dt='{{ds}}');
        REFRESH dashboard.oride_coupon_summary PARTITION (dt='{{ds}}');
        REFRESH dashboard.oride_driver_online_time PARTITION (dt='{{ds}}');
        REFRESH dashboard.oride_order_event_summary PARTITION (dt='{{ds}}');
        REFRESH dashboard.oride_driver_daily_summary PARTITION (dt='{{ds}}');
    """,
    schema='dashboard',
    priority_weight=50,
    dag=dag
)

create_oride_driver_overview >> insert_oride_driver_overview
create_oride_user_overview >> insert_oride_user_overview
insert_oride_driver_overview >> refresh_impala
insert_oride_user_overview >> refresh_impala
create_oride_user_label >> insert_oride_user_label
insert_oride_user_label >> refresh_impala
insert_oride_user_label >> user_label_export
create_opay_media_summary >> insert_opay_media_summary
import_opay_install_log >> insert_opay_media_summary
insert_opay_media_summary >> refresh_impala
create_coupon_summary >> insert_coupon_summary
insert_coupon_summary >> refresh_impala
create_driver_online_time >> import_driver_online_time
import_driver_online_time >> refresh_impala
create_order_event_summary >> insert_order_event_summary >> refresh_impala
import_driver_online_time >> insert_oride_driver_daily_summary
create_oride_driver_daily_summary >> insert_oride_driver_daily_summary >> refresh_impala