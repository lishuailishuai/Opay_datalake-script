# coding=utf-8

import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.utils.email import send_email
from airflow.models import Variable
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from utils.validate_metrics_utils import *
import codecs
import csv

args = {
    'owner': 'linan',
    'start_date': datetime(2019, 7, 11),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_fast_driver_info_daily_report',
    schedule_interval="30 03 * * *",
    default_args=args)

table_names = ['oride_db.data_order',
               'oride_db.data_driver_comment',
               'oride_db.data_driver_balance_extend',
               'oride_db.data_driver',
               'oride_db.data_driver_extend',
               'oride_bi.server_magic_push_detail',
               'oride_bi.oride_driver_timerange',
               'opay_spread.rider_signups',
               'opay_spread.driver_group',
               'opay_spread.driver_team'
               ]

headers = [
    'driver_id',
    'serv_type',
    'driver_name',
    'driver_number',
    'gender',
    'birth',
    'register_time',
    'city',
    'group_name',
    'team_name',
    'total_distance',
    'total_income',
    'driver_balance',
    'total_onlinetime(h)',
    'total_billtime(h)',
    'average_billtime(min)',
    'total_billtime(h)',
    'total_push_orders',
    'total_accept_orders',
    'total_finish_orders',
    'total_cancel_orders',
    'average_push_orders',
    'average_accept_orders',
    'average_finish_orders',
    'total_rate_score≤3',
    'total_average_rate_score',
    'today_online(Y or N)',
    'yesterday_online(Y or N)',
    'thedaybeforeyesterday_online(Y or N)',
    'today_onlinetime',
    'yesterday_onlinetime',
    'today_push_orders',
    'today_accept_orders',
    'yesterday_accept_orders',
    'today_finish_orders',
    'yesterday_finish_orders',
    'today_cancel_orders',
    'today_rate_score≤3',
    'today_rate_numbers',
    'taday_average_rate_score',

]

'''
校验分区代码
'''

validate_partition_data = PythonOperator(
    task_id='validate_partition_data',
    python_callable=validate_partition,
    provide_context=True,
    op_kwargs={
        # 验证table
        "table_names": table_names,
        # 任务名称
        "task_name": "快车司机档案数据"
    },
    dag=dag
)

data_order_validate_task = HivePartitionSensor(
    task_id="data_order_validate_task",
    table="data_order",
    partition="dt='{{ds}}'",
    schema="oride_db",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

data_driver_comment_validate_task = HivePartitionSensor(
    task_id="data_driver_comment_validate_task",
    table="data_driver_comment",
    partition="dt='{{ds}}'",
    schema="oride_db",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

data_driver_balance_extend_validate_task = HivePartitionSensor(
    task_id="data_driver_balance_extend_validate_task",
    table="data_driver_balance_extend",
    partition="dt='{{ds}}'",
    schema="oride_db",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

data_driver_validate_task = HivePartitionSensor(
    task_id="data_driver_validate_task",
    table="data_driver",
    partition="dt='{{ds}}'",
    schema="oride_db",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

data_driver_extend_validate_task = HivePartitionSensor(
    task_id="data_driver_extend_validate_task",
    table="data_driver_extend",
    partition="dt='{{ds}}'",
    schema="oride_db",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

server_magic_push_detail_validate_task = HivePartitionSensor(
    task_id="server_magic_push_detail_validate_task",
    table="server_magic_push_detail",
    partition="dt='{{ds}}'",
    schema="oride_bi",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

oride_driver_timerange_validate_task = HivePartitionSensor(
    task_id="oride_driver_timerange_validate_task",
    table="oride_driver_timerange",
    partition="dt='{{ds}}'",
    schema="oride_bi",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

rider_signups_validate_task = HivePartitionSensor(
    task_id="rider_signups_timerange_validate_task",
    table="rider_signups",
    partition="dt='{{ds}}'",
    schema="opay_spread",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

driver_group_validate_task = HivePartitionSensor(
    task_id="driver_group_timerange_validate_task",
    table="driver_group",
    partition="dt='{{ds}}'",
    schema="opay_spread",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

driver_team_validate_task = HivePartitionSensor(
    task_id="driver_team_timerange_validate_task",
    table="driver_team",
    partition="dt='{{ds}}'",
    schema="opay_spread",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


insert_data = HiveOperator(
    task_id='insert_data',
    hql='''
        with rider_data as (
            select 
            dd.id id,
            dde.serv_type serv_type,
            dd.real_name name,
            dd.gender gender,
            dd.phone_number mobile,
            dd.birthday birthday,
            from_unixtime(dde.register_time,'yyyy-MM-dd HH:mm:ss') create_time,
            datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),from_unixtime(dde.register_time,'yyyy-MM-dd')) regis_days
            
            from 
            (
                select 
                id ,
                real_name ,
                gender,
                phone_number ,
                birthday
                from 
                oride_db.data_driver 
                where dt = '{{ ds }}'
            ) dd 
            join oride_db.data_driver_extend dde on dd.id = dde.id and dde.dt = '{{ ds }}'
        ),
        
        order_data as (
            select 
            driver_id,
            sum(if(status = 4 or status = 5,distance,0)) distance_sum,
            sum(if(arrive_time > 0 and (status = 4 or status = 5),arrive_time - pickup_time,0)) duration_sum,
            count(if(driver_id <> 0 ,id,null)) accpet_num,
            count(if(status = 4 or status = 5,id,null)) on_ride_num,
            count(if(status = 6 and cancel_role = 2,id,null)) driver_cancel_num,
            count(if(from_unixtime(create_time,'yyyy-MM-dd') = '{{ ds }}' and driver_id <> 0 ,id,null)) accpet_num_today,
            count(if(from_unixtime(create_time,'yyyy-MM-dd') = '{{ yesterday_ds }}' and driver_id <> 0 ,id,null)) accpet_num_yesterday,
            count(if(from_unixtime(create_time,'yyyy-MM-dd') = '{{ ds }}' and (status = 4 or status = 5),id,null)) on_ride_num_today,
            count(if(from_unixtime(create_time,'yyyy-MM-dd') = '{{ yesterday_ds }}' and (status = 4 or status = 5),id,null)) on_ride_num_yesterday,
            count(if(from_unixtime(create_time,'yyyy-MM-dd') = '{{ ds }}' and (status = 6 and cancel_role = 2),id,null)) driver_cancel_num_today
            from oride_db.data_order 
            where dt = '{{ ds }}' 
            group by driver_id
        ),
        
        -- order_pay as (
        --     select 
        --     driver_id driver_id,
        --     sum(price) price_sum
        --     from oride_db.data_order_payment
        --     where dt = '{{ ds }}' and status = 1
        --     group by driver_id
        -- ),
        
        
        account_data as (
            select 
            driver_id,
            balance,
            total_income
            from oride_db.data_driver_balance_extend
            where dt = '{{ ds }}'
        ),
        
        push_data as (
            select 
            driver_id,
            count(distinct(order_id)) push_num,
            count(distinct(if(dt='{{ ds }}',order_id,null))) push_num_today
            from 
            oride_bi.server_magic_push_detail
            group by driver_id
        ),
        
        driver_his_online as (
            select 
            driver_id,
            sum(driver_onlinerange) driver_online_sum
            from oride_bi.oride_driver_timerange
            group by driver_id
        ),
        
        
        driver_online as (
            select 
            dt,
            driver_id,
            sum(driver_onlinerange) driver_online_sum
            from oride_bi.oride_driver_timerange
            where dt between '{{ macros.ds_add(ds, -2) }}' and '{{ ds }}'
            group by driver_id,dt
        ),
        
        driver_comment as (
            select 
            driver_id,
            count(1) score_num,
            count(if(score <= 3,id,null)) low_socre_num,
            round(sum(score)/count(1),2) score_avg,
            count(if(score <= 3 and from_unixtime(create_time,'yyyy-MM-dd') = '{{ ds }}',id,null)) low_socre_num_today,
            count(if(from_unixtime(create_time,'yyyy-MM-dd') = '{{ ds }}',id,null)) score_num_today,
            round(sum(if(from_unixtime(create_time,'yyyy-MM-dd') = '{{ ds }}',score,0))/count(if(from_unixtime(create_time,'yyyy-MM-dd') = '{{ ds }}',id,null)),2) score_avg_today
            from oride_db.data_driver_comment 
            where dt = '{{ ds }}'
            group by  driver_id  
        ),
        
        driver_info as (
            select 
            r.driver_id driver_id,
            g.city city,
            g.name  group_name, 
            t.name  admin_user
            from opay_spread.rider_signups r
            left join opay_spread.driver_group g on r.association_id = g.id and g.dt = '{{ ds }}'
            left join opay_spread.driver_team t on r.team_id = t.id and t.dt = '{{ ds }}'
            where 
            r.dt = '{{ ds }}'
            and r.status=2 
            and r.association_id >0 
            and r.team_id > 0
        )
        
        
        
        insert overwrite table oride_bi.oride_driver_record_report partition(dt='{{ ds }}')
        select 
        rd.id ,
        rd.serv_type ,
        rd.name ,
        rd.mobile ,
        rd.gender ,
        rd.birthday ,
        rd.create_time ,
        
        nvl(di.city,'') ,
        nvl(di.group_name,'') ,
        nvl(di.admin_user,'') ,
        
        nvl(round(od.distance_sum/1000,2),0),
        
        -- nvl(op.price_sum,0) as `total_income`,
        -- nvl(ad.total_income,0) as `total_income`,
        0 ,
        nvl(ad.balance,0) ,
        nvl(round(dho.driver_online_sum,0),0) ,
        nvl(round(od.duration_sum,1),0) ,
        
        nvl(rd.regis_days,0),
        nvl(pd.push_num,0) ,
        nvl(od.accpet_num,0) ,
        nvl(od.on_ride_num,0) ,
        nvl(od.driver_cancel_num,0) ,
        nvl(dc.score_num,0) ,
        nvl(dc.low_socre_num,0) ,
        nvl(dc.score_avg,0) ,
        
        if(do_today.driver_id is not null , 1,0) ,
        if(do_yesterday.driver_id is not null , 1,0) ,
        if(do_before.driver_id is not null , 1,0) ,
        if(do_today.driver_id is not null,nvl(round(do_today.driver_online_sum/3600,1),0),0) ,
        if(do_yesterday.driver_id is not null,nvl(round(do_yesterday.driver_online_sum/3600,1),0),0) ,
        
        nvl(pd.push_num_today,0) ,
        nvl(od.accpet_num_today,0) ,
        nvl(od.accpet_num_yesterday,0) ,
        nvl(od.on_ride_num_today,0) ,
        nvl(od.on_ride_num_yesterday,0) ,
        nvl(od.driver_cancel_num_today,0) ,
        
        nvl(dc.low_socre_num_today,0) ,
        nvl(dc.score_num_today,0) ,
        nvl(dc.score_avg_today,0) 
        
        from rider_data rd 
        left join order_data od on rd.id = od.driver_id
        -- left join order_pay op on rd.id = op.driver_id
        left join push_data pd on rd.id = pd.driver_id
        left join account_data ad on rd.id = ad.driver_id
        left join driver_his_online dho on rd.id = dho.driver_id
        left join driver_online do_today on rd.id = do_today.driver_id and  do_today.dt = '{{ ds }}'
        left join driver_online do_yesterday on rd.id = do_yesterday.driver_id and do_yesterday.dt = '{{ yesterday_ds }}'
        left join driver_online do_before on rd.id = do_before.driver_id and do_before.dt = '{{ macros.ds_add(ds, -2) }}'
        left join driver_comment dc on rd.id = dc.driver_id
        left join driver_info di on rd.id = di.driver_id
    
    
    ''',
    schema='oride_bi',
    dag=dag
)


def send_fast_csv_file(ds, **kwargs):
    sql = """
            select 
            driver_id,
            serv_type,
            driver_name,
            driver_number,
            gender,
            birth,
            register_time,
            city,
            group_name,
            team_name,
            total_distance,
            total_income,
            driver_balance,
            nvl(round(total_onlinetime/3600,0),0),
            nvl(round(total_billtime/3600,1),0),
            nvl(round(total_billtime/(60 * total_finish_orders),2),0),
            concat(cast(nvl(round((total_billtime * 100)/total_onlinetime,2),0) as string),'%'),
            total_push_orders,
            total_accept_orders,
            total_finish_orders,
            total_cancel_orders,
            nvl(round(total_push_orders/regis_days,0),0),
            nvl(round(total_accept_orders/regis_days,0),0), 
            nvl(round(total_finish_orders/regis_days,0),0),
            total_rate_numbers,
            total_rate_low_score_numbers,
            total_average_rate_score,
            if(today_online = 1,'Y','N'),
            if(yesterday_online = 1,'Y','N'),
            if(thedaybeforeyesterday_online = 1,'Y','N'),
            today_onlinetime,
            yesterday_onlinetime,
            today_push_orders,
            today_accept_orders,
            yesterday_accept_orders,
            today_finish_orders,
            yesterday_finish_orders,
            today_cancel_orders,
            today_rate_low_score_numbers,
            today_rate_numbers,
            taday_average_rate_score

            from 
            oride_bi.oride_driver_record_report 
            where dt = '{dt}' and serv_type = 2

        """.format(dt=ds)

    logging.info('Executing: %s', sql)
    cursor.execute(sql)
    rows = cursor.fetchall()

    file_name = 'fast_driver_{dt}.csv'.format(dt=ds)
    with codecs.open(file_name, 'w', 'utf_8_sig') as f:
        f_csv = csv.writer(f)
        f_csv.writerow(headers)
        f_csv.writerows(rows)

    # send mail
    email_to = Variable.get("oride_fast_driver_metrics_report_receivers").split()
    # email_to = ['nan.li@opay-inc.com']
    result = is_alert(ds, table_names)
    if result:
        email_to = ['bigdata@opay-inc.com']
        # email_to = ['nan.li@opay-inc.com']


    email_subject = 'oride快车司机明细附件_{dt}'.format(dt=ds)
    email_body = '快车司机档案数据，请查收。\n 附件中文乱码解决:使用记事本打开CSV文件，“文件”->“另存为”，编码方式选择ANSI，保存完毕后，用EXCEL打开，即可。'
    send_email(email_to, email_subject, email_body, [file_name], mime_charset='utf-8')


send_fast_driver_file_email = PythonOperator(
    task_id='send_fast_driver_file_email',
    python_callable=send_fast_csv_file,
    provide_context=True,
    dag=dag
)


def send_otirke_csv_file(ds, ds_nodash, **kwargs):
    sql = """
        select 
        driver_id,
        serv_type,
        driver_name,
        driver_number,
        gender,
        birth,
        register_time,
        city,
        group_name,
        team_name,
        total_distance,
        total_income,
        driver_balance,
        nvl(round(total_onlinetime/3600,0),0),
        nvl(round(total_billtime/3600,1),0),
        nvl(round(total_billtime/(60 * total_finish_orders),2),0),
        concat(cast(nvl(round((total_billtime * 100)/total_onlinetime,2),0) as string),'%'),
        total_push_orders,
        total_accept_orders,
        total_finish_orders,
        total_cancel_orders,
        nvl(round(total_push_orders/regis_days,0),0),
        nvl(round(total_accept_orders/regis_days,0),0), 
        nvl(round(total_finish_orders/regis_days,0),0),
        total_rate_numbers,
        total_rate_low_score_numbers,
        total_average_rate_score,
        if(today_online = 1,'Y','N'),
        if(yesterday_online = 1,'Y','N'),
        if(thedaybeforeyesterday_online = 1,'Y','N'),
        today_onlinetime,
        yesterday_onlinetime,
        today_push_orders,
        today_accept_orders,
        yesterday_accept_orders,
        today_finish_orders,
        yesterday_finish_orders,
        today_cancel_orders,
        today_rate_low_score_numbers,
        today_rate_numbers,
        taday_average_rate_score
        
        from 
        oride_bi.oride_driver_record_report 
        where dt = '{dt}' and serv_type = 3

    """.format(dt=ds)

    logging.info('Executing: %s', sql)
    cursor.execute(sql)
    rows = cursor.fetchall()

    file_name = 'otrike_driver_record_{dt}.csv'.format(dt=ds)
    with codecs.open(file_name, 'w', 'utf_8_sig') as f:
        f_csv = csv.writer(f)
        f_csv.writerow(headers)
        f_csv.writerows(rows)

    # send mail
    email_to = Variable.get("oride_otrike_driver_record_report_receivers").split()
    # email_to = ['nan.li@opay-inc.com']

    result = is_alert(ds, table_names)
    if result:
        email_to = ['bigdata@opay-inc.com']
        # email_to = ['nan.li@opay-inc.com']

    email_subject = 'otrike司机明细附件_{dt}'.format(dt=ds)
    email_body = 'otrike司机档案数据，请查收。\n 附件中文乱码解决:使用记事本打开CSV文件，“文件”->“另存为”，编码方式选择ANSI，保存完毕后，用EXCEL打开，即可。'
    send_email(email_to, email_subject, email_body, [file_name], mime_charset='utf-8')


send_otrike_file_email = PythonOperator(
    task_id='send_otrike_file_email',
    python_callable=send_otirke_csv_file,
    provide_context=True,
    dag=dag
)

validate_partition_data >> data_order_validate_task >> insert_data
validate_partition_data >> data_driver_balance_extend_validate_task >> insert_data
validate_partition_data >> data_driver_comment_validate_task >> insert_data
validate_partition_data >> data_driver_extend_validate_task >> insert_data
validate_partition_data >> data_driver_validate_task >> insert_data
validate_partition_data >> server_magic_push_detail_validate_task >> insert_data
validate_partition_data >> oride_driver_timerange_validate_task >> insert_data
validate_partition_data >> rider_signups_validate_task >> insert_data
validate_partition_data >> driver_group_validate_task >> insert_data
validate_partition_data >> driver_team_validate_task >> insert_data

insert_data >> send_fast_driver_file_email
insert_data >> send_otrike_file_email
