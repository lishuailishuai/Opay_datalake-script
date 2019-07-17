# coding=utf-8

import airflow
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from airflow.models import Variable

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
            dd.driver_id driver_id,
            cc.name city,
            dg.name group_name,
            au.name admin_user
            from 
            (
            select 
            driver_id,group_id
            from
            opay_spread.driver_data
            where dt = '${dt}'
            ) dd 
            left join opay_spread.driver_group dg on dd.group_id=dg.id and dg.dt = '${dt}'
            left join oride_db.data_city_conf cc on dg.city = cc.id and cc.dt = '${dt}'
            left join opay_spread.admin_users au on au.id = dg.manager_id and au.dt = '${dt}'
            )
            
            
            select 
            rd.id as \`司机id\`,
            rd.name as \`司机姓名\`,
            rd.mobile as \`司机电话\`,
            rd.gender as \`性别\`,
            rd.birthday as \`生日\`,
            rd.create_time as \`注册时间\`,
            
            nvl(di.city,'') as \`所属区域\`,
            nvl(di.group_name,'') as \`所属协会\`,
            nvl(di.admin_user,'') as \`所属司管姓名\`,
            
            nvl(round(od.distance_sum/1000,2),0) as \`总里程数（km）\`,
            
            -- nvl(op.price_sum,0) as \`总收入\`,
            -- nvl(ad.total_income,0) as \`总收入1\`,
            0 as \`总收入\`,
            nvl(ad.balance,0) as \`司机账户余额\`,
            nvl(round(dho.driver_online_sum/3600,0),0) as \`总在线时长（时）\`,
            nvl(round(od.duration_sum/3600,1),0) as \`总计费时长（时）\`,
            nvl(round(od.duration_sum/(60*od.on_ride_num),2),0) as \`平均计费时长（分）\`,
            concat(cast(nvl(round((od.duration_sum * 100)/dho.driver_online_sum,2),0) as string),'%') as \`计费时长占比\`,
            nvl(pd.push_num,0) as \`总推送订单数\`,
            nvl(od.accpet_num,0) as \`总接单数\`,
            nvl(od.on_ride_num,0) as \`总完单数\`,
            nvl(od.driver_cancel_num,0) as \`总取消订单数\`,
            nvl(round(pd.push_num/rd.regis_days,0),0) as \`平均日推送订单数\`,
            nvl(round(od.accpet_num/rd.regis_days,0),0) as \`平均日接单数\`,
            nvl(round(od.on_ride_num/rd.regis_days,0),0) as \`平均日完单数\`,
            nvl(dc.score_num,0) as \`总评价次数\`,
            nvl(dc.low_socre_num,0) as \`3分以下评价次数\`,
            nvl(dc.score_avg,0) as \`平均评分\`,
            
            if(do_today.driver_id is not null , 'Y','N') as \`今日是否在线\`,
            if(do_yesterday.driver_id is not null , 'Y','N') as \`昨日是否在线\`,
            if(do_before.driver_id is not null , 'Y','N') as \`前日是否在线\`,
            if(do_today.driver_id is not null,round(do_today.driver_online_sum/3600,1),0) as \`今日在线时长（时）\`,
            if(do_yesterday.driver_id is not null,round(do_yesterday.driver_online_sum/3600,1),0) as \`昨日在线时长（时）\`,
            
            nvl(pd.push_num_today,0) as \`今日推送订单数\`,
            nvl(od.accpet_num_today,0) as \`今日接单数\`,
            nvl(od.accpet_num_yesterday,0) as \`昨日接单数\`,
            nvl(od.on_ride_num_today,0) as \`今日完单数\`,
            nvl(od.on_ride_num_yesterday,0) as \`昨日完单数\`,
            nvl(od.driver_cancel_num_today,0) as \`今日取消订单数\`,
            
            nvl(dc.low_socre_num_today,0) as \`今日差评数(评分≤3分）\`,
            nvl(dc.score_num_today,0) as \`今日总评价次数\`,
            nvl(dc.score_avg_today,0) as \`今日平均评分\`
            
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
    email_subject = 'oride快车司机明细附件（测试版）_{dt}'.format(dt=ds)
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

create_csv_file >> send_file_email
