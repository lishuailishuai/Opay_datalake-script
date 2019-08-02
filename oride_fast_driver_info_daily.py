# coding=utf-8

import airflow
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from airflow.models import Variable
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from utils.validate_metrics_utils import *

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


'''
校验分区代码
'''

validate_partition_data = PythonOperator(
    task_id='validate_partition_data',
    python_callable=validate_partition,
    provide_context=True,
    op_kwargs={
        # 验证table
        "table_names":
            ['oride_db.data_order',
             'oride_db.data_driver_comment',
             'oride_db.data_driver_balance_extend',
             'oride_db.data_driver',
             'oride_db.data_driver_extend',
             'oride_bi.server_magic_push_detail',
             'oride_bi.oride_driver_timerange',
             'opay_spread.rider_signups',
             'opay_spread.driver_group',
             'opay_spread.driver_team'
             ],
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


create_csv_file = BashOperator(
    task_id='create_csv_file',
    bash_command='''
    
        export LANG=zh_CN.UTF-8
        log_path="/data/app_log"
        dt="{{ ds }}"
        yesterday="{{ yesterday_ds }}"
        before_yesterday="{{ macros.ds_add(ds, -2) }}"

        # 导出数据到文件
        # 周五报表
        fast_driver_sql="
            with rider_data as (
            select 
            dd.id id,
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
            where dt = '${dt}'
            ) dd 
            join oride_db.data_driver_extend dde on dd.id = dde.id and dde.dt = '${dt}'
            and dde.serv_type = 2
            
            ),
            
            order_data as (
            select 
            driver_id,
            sum(if(status = 4 or status = 5,distance,0)) distance_sum,
            sum(if(status = 4 or status = 5,duration,0)) duration_sum,
            count(if(driver_id <> 0 ,id,null)) accpet_num,
            count(if(status = 4 or status = 5,id,null)) on_ride_num,
            count(if(status = 6 and cancel_role = 2,id,null)) driver_cancel_num,
            count(if(from_unixtime(create_time,'yyyy-MM-dd') = '${dt}' and driver_id <> 0 ,id,null)) accpet_num_today,
            count(if(from_unixtime(create_time,'yyyy-MM-dd') = '${yesterday}' and driver_id <> 0 ,id,null)) accpet_num_yesterday,
            count(if(from_unixtime(create_time,'yyyy-MM-dd') = '${dt}' and (status = 4 or status = 5),id,null)) on_ride_num_today,
            count(if(from_unixtime(create_time,'yyyy-MM-dd') = '${yesterday}' and (status = 4 or status = 5),id,null)) on_ride_num_yesterday,
            count(if(from_unixtime(create_time,'yyyy-MM-dd') = '${dt}' and (status = 6 and cancel_role = 2),id,null)) driver_cancel_num_today
            from oride_db.data_order 
            where dt = '${dt}' 
            group by driver_id
            ),
            
            -- order_pay as (
            --     select 
            --     driver_id driver_id,
            --     sum(price) price_sum
            --     from oride_db.data_order_payment
            --     where dt = '${dt}' and status = 1
            --     group by driver_id
            -- ),
            
            
            account_data as (
            select 
            driver_id,
            balance,
            total_income
            from oride_db.data_driver_balance_extend
            where dt = '${dt}'
            ),
            
            push_data as (
            select 
            driver_id,
            count(distinct(order_id)) push_num,
            count(distinct(if(dt='${dt}',order_id,null))) push_num_today
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
            where dt between '${before_yesterday}' and '${dt}'
            group by driver_id,dt
            ),
            
            driver_comment as (
            select 
            driver_id,
            count(1) score_num,
            count(if(score <= 3,id,null)) low_socre_num,
            round(sum(score)/count(1),2) score_avg,
            count(if(score <= 3 and from_unixtime(create_time,'yyyy-MM-dd') = '${dt}',id,null)) low_socre_num_today,
            count(if(from_unixtime(create_time,'yyyy-MM-dd') = '${dt}',id,null)) score_num_today,
            round(sum(if(from_unixtime(create_time,'yyyy-MM-dd') = '${dt}',score,0))/count(if(from_unixtime(create_time,'yyyy-MM-dd') = '${dt}',id,null)),2) score_avg_today
            from oride_db.data_driver_comment 
            where dt = '${dt}'
            group by  driver_id  
            ),
            
            driver_info as (
            select 
            r.driver_id driver_id,
            g.city city,
            g.name  group_name, 
            t.name  admin_user
            from opay_spread.rider_signups r
            left join opay_spread.driver_group g on r.association_id = g.id and g.dt = '${dt}'
            left join opay_spread.driver_team t on r.team_id = t.id and t.dt = '${dt}'
            where 
            r.dt = '${dt}'
            and r.driver_type=2 
            and r.status=2 
            and r.association_id >0 
            and r.team_id > 0
            )
            
            
            select 
            rd.id as \`driver_id\`,
            rd.name as \`driver_name\`,
            rd.mobile as \`driver_number\`,
            rd.gender as \`gender\`,
            rd.birthday as \`birth\`,
            rd.create_time as \`register_time \`,
            
            nvl(di.city,'') as \`city\`,
            nvl(di.group_name,'') as \`group_name\`,
            nvl(di.admin_user,'') as \`team_name\`,
            
            nvl(round(od.distance_sum/1000,2),0) as \`total_distance\`,
            
            -- nvl(op.price_sum,0) as \`total_income\`,
            -- nvl(ad.total_income,0) as \`total_income\`,
            0 as \`total_income\`,
            nvl(ad.balance,0) as \`driver_balance\`,
            nvl(round(dho.driver_online_sum/3600,0),0) as \`total_onlinetime(h)\`,
            nvl(round(od.duration_sum/3600,1),0) as \`total_billtime(h)\`,
            nvl(round(od.duration_sum/(60*od.on_ride_num),2),0) as \`average_billtime(min)\`,
            concat(cast(nvl(round((od.duration_sum * 100)/dho.driver_online_sum,2),0) as string),'%') as \`billtime_ratio\`,
            nvl(pd.push_num,0) as \`total_push_orders\`,
            nvl(od.accpet_num,0) as \`total_accept_orders\`,
            nvl(od.on_ride_num,0) as \`total_finish_orders\`,
            nvl(od.driver_cancel_num,0) as \`total_cancel_orders\`,
            nvl(round(pd.push_num/rd.regis_days,0),0) as \`average_push_orders\`,
            nvl(round(od.accpet_num/rd.regis_days,0),0) as \`average_accept_orders\`,
            nvl(round(od.on_ride_num/rd.regis_days,0),0) as \`average_finish_orders\`,
            nvl(dc.score_num,0) as \`total_rate_numbers\`,
            nvl(dc.low_socre_num,0) as \`total_rate_score≤3\`,
            nvl(dc.score_avg,0) as \`total_average_rate_score\`,
            
            if(do_today.driver_id is not null , 'Y','N') as \`today_online(Y or N)\`,
            if(do_yesterday.driver_id is not null , 'Y','N') as \`yesterday_online(Y or N)\`,
            if(do_before.driver_id is not null , 'Y','N') as \`thedaybeforeyesterday_online(Y or N)\`,
            if(do_today.driver_id is not null,round(do_today.driver_online_sum/3600,1),0) as \`today_onlinetime\`,
            if(do_yesterday.driver_id is not null,round(do_yesterday.driver_online_sum/3600,1),0) as \`yesterday_onlinetime\`,
            
            nvl(pd.push_num_today,0) as \`today_push_orders\`,
            nvl(od.accpet_num_today,0) as \`today_accept_orders\`,
            nvl(od.accpet_num_yesterday,0) as \`yesterday_accept_orders\`,
            nvl(od.on_ride_num_today,0) as \`today_finish_orders\`,
            nvl(od.on_ride_num_yesterday,0) as \`yesterday_finish_orders\`,
            nvl(od.driver_cancel_num_today,0) as \`today_cancel_orders\`,
            
            nvl(dc.low_socre_num_today,0) as \`today_rate_score≤3\`,
            nvl(dc.score_num_today,0) as \`today_rate_numbers\`,
            nvl(dc.score_avg_today,0) as \`taday_average_rate_score\`
            
            from rider_data rd 
            left join order_data od on rd.id = od.driver_id
            -- left join order_pay op on rd.id = op.driver_id
            left join push_data pd on rd.id = pd.driver_id
            left join account_data ad on rd.id = ad.driver_id
            left join driver_his_online dho on rd.id = dho.driver_id
            left join driver_online do_today on rd.id = do_today.driver_id and  do_today.dt = '${dt}'
            left join driver_online do_yesterday on rd.id = do_yesterday.driver_id and do_yesterday.dt = '${yesterday}'
            left join driver_online do_before on rd.id = do_before.driver_id and do_before.dt = '${before_yesterday}'
            left join driver_comment dc on rd.id = dc.driver_id
            left join driver_info di on rd.id = di.driver_id
            ;
"
        echo ${fast_driver_sql}
        
        hive -e "set hive.cli.print.header=true; ${fast_driver_sql}"  | sed 's/[\t]/,/g'  > ${log_path}/tmp/fast_driver_${dt}.csv
        
    ''',
    dag=dag,
)


def send_csv_file(ds, **kwargs):
    name_list = [
        'fast_driver'
    ]
    file_list = []
    for name in name_list:
        file_list.append("/data/app_log/tmp/%s_%s.csv" % (name, ds))

    # send mail
    email_to = Variable.get("oride_fast_driver_metrics_report_receivers").split()
    # email_to = ['nan.li@opay-inc.com']
    email_subject = 'oride快车司机明细附件_{dt}'.format(dt=ds)
    send_email(email_to, email_subject,
               '快车司机档案数据，请查收。\n 附件中文乱码解决:使用记事本打开CSV文件，“文件”->“另存为”，编码方式选择ANSI，保存完毕后，用EXCEL打开，即可。',
               file_list,
               mime_charset='utf-8')


send_file_email = PythonOperator(
    task_id='send_file_email',
    python_callable=send_csv_file,
    provide_context=True,
    dag=dag
)

validate_partition_data >> data_order_validate_task >> create_csv_file
validate_partition_data >> data_driver_balance_extend_validate_task >> create_csv_file
validate_partition_data >> data_driver_comment_validate_task >> create_csv_file
validate_partition_data >> data_driver_extend_validate_task >> create_csv_file
validate_partition_data >> data_driver_validate_task >> create_csv_file
validate_partition_data >> server_magic_push_detail_validate_task >> create_csv_file
validate_partition_data >> oride_driver_timerange_validate_task >> create_csv_file
validate_partition_data >> rider_signups_validate_task >> create_csv_file
validate_partition_data >> driver_group_validate_task >> create_csv_file
validate_partition_data >> driver_team_validate_task >> create_csv_file

create_csv_file >> send_file_email
