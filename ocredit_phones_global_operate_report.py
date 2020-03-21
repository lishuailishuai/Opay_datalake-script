# coding=utf-8

import airflow
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.utils.email import send_email
import logging
from airflow.models import Variable
from utils.connection_helper import get_hive_cursor
from plugins.comwx import ComwxApi
from utils.validate_metrics_utils import *
from constant.metrics_constant import *
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.sensors import UFileSensor
from airflow.sensors import OssSensor

comwx = ComwxApi('wwd26d45f97ea74ad2', 'BLE_v25zCmnZaFUgum93j3zVBDK-DjtRkLisI_Wns4g', '1000011')

args = {
    'owner': 'lishuai',
    'start_date': datetime(2020, 2, 25),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    'ocredit_phones_global_operate_report',
    schedule_interval="30 01 * * *",
    default_args=args)



##----------------------------------------- 依赖 ---------------------------------------##
app_ocredit_phones_order_base_cube_d_task = OssSensor(
    task_id='app_ocredit_phones_order_base_cube_d_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones/ocredit_phones_dw/app_ocredit_phones_order_base_cube_d/country_code=nal",

        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

app_ocredit_phones_order_base_cube_m_task = OssSensor(
    task_id='app_ocredit_phones_order_base_cube_m_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones/ocredit_phones_dw/app_ocredit_phones_order_base_cube_m/country_code=nal",

        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

app_ocredit_phones_order_base_cube_w_task = OssSensor(
    task_id='app_ocredit_phones_order_base_cube_w_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones/ocredit_phones_dw/app_ocredit_phones_order_base_cube_w/country_code=nal",

        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


def get_show_date(ds):
    tr_fmt = '''
            <tr>
            各位同事：<br/>
            附件为截止到昨日{date_of_entry}的手机分期业务日报情况，主要内容如下：<br/>
            1、当日情况：进件{entry_cnt}笔，实际放款{loan_cnt}笔，转化率{business_rate}，贷款金额{loan_amount_usd}美元，单笔件均{average_usd}美元；期限结构方面，3-6-4-8期放款结构占比分别为{rate_3}，{rate_6}，{rate_4}和{rate_8}
            </tr>
            '''

    sql = '''
          select date_of_entry,entry_cnt,loan_cnt,business_rate,loan_amount_usd,average_usd
          ,concat(round(loan_cnt_3/loan_cnt*100,2),'%'),
          concat(round(loan_cnt_6/loan_cnt*100,2),'%'),
          concat(round(loan_cnt_4/loan_cnt*100,2),'%'),
          concat(round(loan_cnt_8/loan_cnt*100,2),'%')
          
          from(
           select date_of_entry,
          sum(entry_cnt) as entry_cnt,
          sum(loan_cnt) as loan_cnt,
          format_number(sum(loan_amount_usd),0) as loan_amount_usd,
          format_number(sum(average_usd),0) as average_usd,
          concat(sum(business_rate),'%') as business_rate,
          
          sum(entry_cnt_3) as entry_cnt_3,
          sum(loan_cnt_3) as loan_cnt_3,
          format_number(sum(loan_amount_usd_3),0) as loan_amount_usd_3,
          format_number(sum(average_usd_3),0) as average_usd_3,
          concat(sum(business_rate_3),'%') as business_rate_3,
          
          sum(entry_cnt_6) as entry_cnt_6,
          sum(loan_cnt_6) as loan_cnt_6,
          format_number(sum(loan_amount_usd_6),0) as loan_amount_usd_6,
          format_number(sum(average_usd_6),0) as average_usd_6,
          concat(sum(business_rate_6),'%') as business_rate_6,
          
          
          sum(entry_cnt_4) as entry_cnt_4,
          sum(loan_cnt_4) as loan_cnt_4,
          format_number(sum(loan_amount_usd_4),0) as loan_amount_usd_4,
          format_number(sum(average_usd_4),0) as average_usd_4,
          concat(sum(business_rate_4),'%') as business_rate_4,
          
          sum(entry_cnt_8) as entry_cnt_8,
          sum(loan_cnt_8) as loan_cnt_8,
          format_number(sum(loan_amount_usd_8),0) as loan_amount_usd_8,
          format_number(sum(average_usd_8),0) as average_usd_8,
          concat(sum(business_rate_8),'%') as business_rate_8
          
          from(
          select date_of_entry,
          entry_cnt,
          loan_cnt,
          round(loan_amount_usd,2) loan_amount_usd,
          round(loan_amount_usd/loan_cnt,2) average_usd,
          round(loan_cnt/entry_cnt*100,2) business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8


          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_d
          where dt='{dt}'
          and terms=-10000
          and substr(date_of_entry,1,7) = substr('{dt}',1,7)
          
          union all
          
          select date_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          entry_cnt as entry_cnt_3,
          loan_cnt as loan_cnt_3,
          round(loan_amount_usd,2) loan_amount_usd_3,
          round(loan_amount_usd/loan_cnt,2) average_usd_3,
          round(loan_cnt/entry_cnt*100,2) business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_d
          where dt='{dt}'
          and terms=3
          and substr(date_of_entry,1,7) = substr('{dt}',1,7)
          
          
          union all
          select date_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          entry_cnt as entry_cnt_6,
          loan_cnt as loan_cnt_6,
          round(loan_amount_usd,2) loan_amount_usd_6,
          round(loan_amount_usd/loan_cnt,2) average_usd_6,
          round(loan_cnt/entry_cnt*100,2) business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8

          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_d
          where dt='{dt}'
          and terms=6
          and substr(date_of_entry,1,7) = substr('{dt}',1,7)
          
          
          union all
          select date_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          entry_cnt as entry_cnt_4,
          loan_cnt as loan_cnt_4,
          round(loan_amount_usd,2) loan_amount_usd_4,
          round(loan_amount_usd/loan_cnt,2) average_usd_4,
          round(loan_cnt/entry_cnt*100,2) business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_d
          where dt='{dt}'
          and terms=4
          and substr(date_of_entry,1,7) = substr('{dt}',1,7)
          
          
          
          union all
          select date_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          entry_cnt as entry_cnt_8,
          loan_cnt as loan_cnt_8,
          round(loan_amount_usd,2) loan_amount_usd_8,
          round(loan_amount_usd/loan_cnt,2) average_usd_8,
          round(loan_cnt/entry_cnt*100,2) business_rate_8
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_d
          where dt='{dt}'
          and terms=8
          and substr(date_of_entry,1,7) = substr('{dt}',1,7)
          
          ) a
          group by date_of_entry
          order by date_of_entry desc
          
          ) a where date_of_entry='{dt}'
               
               
             
                 '''.format(dt=ds,
                            start_date=airflow.macros.ds_add(ds, -14))
    cursor = get_hive_cursor()
    logging.info('Executing: %s', sql)
    cursor.execute(sql)
    data_list = cursor.fetchall()
    cursor.close()
    row_html = ''
    if len(data_list) > 0:
        for data in data_list:
            date_of_entry=data[0]
            entry_cnt=data[1]
            loan_cnt=data[2]
            business_rate=data[3]

            loan_amount_usd = data[4]
            average_usd = data[5]

            rate_3=data[6]
            rate_6=data[7]
            rate_4=data[8]
            rate_8=data[9]

            row_html += tr_fmt.format(

            date_of_entry=date_of_entry,
            entry_cnt = entry_cnt,
            loan_cnt = loan_cnt,
            business_rate = business_rate,
            loan_amount_usd = loan_amount_usd,
            average_usd = average_usd,
            rate_3 = rate_3,
            rate_6 = rate_6,
            rate_4=rate_4,
            rate_8=rate_8

            )

    return row_html





def get_show_month(ds):
    tr_fmt = '''
               <tr>
            2、本月情况：进件{entry_cnt}笔，实际放款{loan_cnt}笔，转化率{business_rate}，贷款金额{loan_amount_usd}美元，单笔件均{average_usd}美元；期限结构方面，3-6-4-8期放款结构占比分别为{rate_3}，{rate_6}，{rate_4}和{rate_8}
            </tr>
            '''

    sql = '''
          
          select entry_cnt,loan_cnt,business_rate,loan_amount_usd,average_usd
          ,concat(round(loan_cnt_3/loan_cnt*100,2),'%'),
          concat(round(loan_cnt_6/loan_cnt*100,2),'%'),
          concat(round(loan_cnt_4/loan_cnt*100,2),'%'),
          concat(round(loan_cnt_8/loan_cnt*100,2),'%')
          
          
          from(
          
          select month_of_entry,
          sum(entry_cnt) as entry_cnt,
          sum(loan_cnt) as loan_cnt,
          format_number(sum(loan_amount_usd),0) as loan_amount_usd,
          format_number(sum(average_usd),0) as average_usd,
          concat(sum(business_rate),'%') as business_rate,
          
          sum(entry_cnt_3) as entry_cnt_3,
          sum(loan_cnt_3) as loan_cnt_3,
          format_number(sum(loan_amount_usd_3),0) as loan_amount_usd_3,
          format_number(sum(average_usd_3),0) as average_usd_3,
          concat(sum(business_rate_3),'%') as business_rate_3,
          
          sum(entry_cnt_6) as entry_cnt_6,
          sum(loan_cnt_6) as loan_cnt_6,
          format_number(sum(loan_amount_usd_6),0) as loan_amount_usd_6,
          format_number(sum(average_usd_6),0) as average_usd_6,
          concat(sum(business_rate_6),'%') as business_rate_6,
          
          
          sum(entry_cnt_4) as entry_cnt_4,
          sum(loan_cnt_4) as loan_cnt_4,
          format_number(sum(loan_amount_usd_4),0) as loan_amount_usd_4,
          format_number(sum(average_usd_4),0) as average_usd_4,
          concat(sum(business_rate_4),'%') as business_rate_4,
          
          sum(entry_cnt_8) as entry_cnt_8,
          sum(loan_cnt_8) as loan_cnt_8,
          format_number(sum(loan_amount_usd_8),0) as loan_amount_usd_8,
          format_number(sum(average_usd_8),0) as average_usd_8,
          concat(sum(business_rate_8),'%') as business_rate_8
          
          from(
          select month_of_entry,
          entry_cnt,
          loan_cnt,
          round(loan_amount_usd,2) loan_amount_usd,
          round(loan_amount_usd/loan_cnt,2) average_usd,
          round(loan_cnt/entry_cnt*100,2) business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8
          
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_m
          where dt='{dt}'
          and terms=-10000
          
          union all

          select month_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          entry_cnt as entry_cnt_3,
          loan_cnt as loan_cnt_3,
          round(loan_amount_usd,2) loan_amount_usd_3,
          round(loan_amount_usd/loan_cnt,2) average_usd_3,
          round(loan_cnt/entry_cnt*100,2) business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8
          
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_m
          where dt='{dt}'
          and terms=3
          
          
          union all
          select month_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          entry_cnt as entry_cnt_6,
          loan_cnt as loan_cnt_6,
          round(loan_amount_usd,2) loan_amount_usd_6,
          round(loan_amount_usd/loan_cnt,2) average_usd_6,
          round(loan_cnt/entry_cnt*100,2) business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_m
          where dt='{dt}'
          and terms=6



            union all
          select month_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          entry_cnt as entry_cnt_4,
          loan_cnt as loan_cnt_4,
          round(loan_amount_usd,2) loan_amount_usd_4,
          round(loan_amount_usd/loan_cnt,2) average_usd_4,
          round(loan_cnt/entry_cnt*100,2) business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_m
          where dt='{dt}'
          and terms=4
          
          
           union all
          select month_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          entry_cnt as entry_cnt_8,
          loan_cnt as loan_cnt_8,
          round(loan_amount_usd,2) loan_amount_usd_8,
          round(loan_amount_usd/loan_cnt,2) average_usd_8,
          round(loan_cnt/entry_cnt*100,2) business_rate_8
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_m
          where dt='{dt}'
          and terms=8



          ) a
          group by month_of_entry
          order by month_of_entry desc
          
          ) a where month_of_entry=substr('{dt}',1,7)



                 '''.format(dt=ds,
                            start_date=airflow.macros.ds_add(ds, -14))
    cursor = get_hive_cursor()
    logging.info('Executing: %s', sql)
    cursor.execute(sql)
    data_list = cursor.fetchall()
    cursor.close()
    row_html = ''
    if len(data_list) > 0:
        for data in data_list:
            entry_cnt = data[0]
            loan_cnt = data[1]
            business_rate = data[2]
            loan_amount_usd = data[3]
            average_usd = data[4]
            rate_3 = data[5]
            rate_6 = data[6]
            rate_4 = data[7]
            rate_8 = data[8]

            row_html += tr_fmt.format(

                dt=ds,
                entry_cnt=entry_cnt,
                loan_cnt=loan_cnt,
                business_rate=business_rate,
                loan_amount_usd=loan_amount_usd,
                average_usd=average_usd,
                rate_3=rate_3,
                rate_6=rate_6,
                rate_4=rate_4,
                rate_8=rate_8

            )

    return row_html


def get_show_all(ds):
    tr_fmt = '''
               <tr>
            3、累计情况：进件{entry_cnt}笔，实际放款{loan_cnt}笔，转化率{business_rate}，贷款金额{loan_amount_usd}美元，件均{average_usd}美元；期限结构方面，3-6-4-8期放款结构占比分别为{rate_3}，{rate_6}，{rate_4}和{rate_8}<br/>
            备注：业务转化率=放款量/进件量<br/>
            以上敬请参考，如有疑问，随时沟通！
            </tr>
            '''

    sql = '''

          select sum(entry_cnt),sum(loan_cnt),concat(round(sum(loan_cnt)/sum(entry_cnt) * 100, 2),'%') as business_rate,
          format_number(sum(loan_amount_usd),0),format_number(sum(loan_amount_usd)/sum(loan_cnt),0) as  average_usd,
          
          concat(round(sum(loan_cnt_3)/sum(loan_cnt)*100,2),'%'),
          concat(round(sum(loan_cnt_6)/sum(loan_cnt)*100,2),'%'),
          concat(round(sum(loan_cnt_4)/sum(loan_cnt)*100,2),'%'),
          concat(round(sum(loan_cnt_8)/sum(loan_cnt)*100,2),'%')

          from(

          select month_of_entry,
          sum(entry_cnt) as entry_cnt,
          sum(loan_cnt) as loan_cnt,
          sum(loan_amount_usd) as loan_amount_usd,
          sum(average_usd) as average_usd,
          concat(sum(business_rate),'%') as business_rate,

          sum(entry_cnt_3) as entry_cnt_3,
          sum(loan_cnt_3) as loan_cnt_3,
          sum(loan_amount_usd_3) as loan_amount_usd_3,
          sum(average_usd_3) as average_usd_3,
          concat(sum(business_rate_3),'%') as business_rate_3,

          sum(entry_cnt_6) as entry_cnt_6,
          sum(loan_cnt_6) as loan_cnt_6,
          sum(loan_amount_usd_6) as loan_amount_usd_6,
          sum(average_usd_6) as average_usd_6,
          concat(sum(business_rate_6),'%') as business_rate_6,


          sum(entry_cnt_4) as entry_cnt_4,
          sum(loan_cnt_4) as loan_cnt_4,
          sum(loan_amount_usd_4) as loan_amount_usd_4,
          sum(average_usd_4) as average_usd_4,
          concat(sum(business_rate_4),'%') as business_rate_4,

          sum(entry_cnt_8) as entry_cnt_8,
          sum(loan_cnt_8) as loan_cnt_8,
          sum(loan_amount_usd_8) as loan_amount_usd_8,
          sum(average_usd_8) as average_usd_8,
          concat(sum(business_rate_8),'%') as business_rate_8

          from(
          select month_of_entry,
          entry_cnt,
          loan_cnt,
          round(loan_amount_usd,2) loan_amount_usd,
          round(loan_amount_usd/loan_cnt,2) average_usd,
          round(loan_cnt/entry_cnt*100,2) business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8


          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_m
          where dt='{dt}'
          and terms=-10000

          union all

          select month_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          entry_cnt as entry_cnt_3,
          loan_cnt as loan_cnt_3,
          round(loan_amount_usd,2) loan_amount_usd_3,
          round(loan_amount_usd/loan_cnt,2) average_usd_3,
          round(loan_cnt/entry_cnt*100,2) business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8


          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_m
          where dt='{dt}'
          and terms=3


          union all
          select month_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          entry_cnt as entry_cnt_6,
          loan_cnt as loan_cnt_6,
          round(loan_amount_usd,2) loan_amount_usd_6,
          round(loan_amount_usd/loan_cnt,2) average_usd_6,
          round(loan_cnt/entry_cnt*100,2) business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8

          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_m
          where dt='{dt}'
          and terms=6



            union all
          select month_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          entry_cnt as entry_cnt_4,
          loan_cnt as loan_cnt_4,
          round(loan_amount_usd,2) loan_amount_usd_4,
          round(loan_amount_usd/loan_cnt,2) average_usd_4,
          round(loan_cnt/entry_cnt*100,2) business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8

          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_m
          where dt='{dt}'
          and terms=4


           union all
          select month_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          entry_cnt as entry_cnt_8,
          loan_cnt as loan_cnt_8,
          round(loan_amount_usd,2) loan_amount_usd_8,
          round(loan_amount_usd/loan_cnt,2) average_usd_8,
          round(loan_cnt/entry_cnt*100,2) business_rate_8

          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_m
          where dt='{dt}'
          and terms=8



          ) a
          group by month_of_entry
          order by month_of_entry desc

          ) a 



                 '''.format(dt=ds,
                            start_date=airflow.macros.ds_add(ds, -14))
    cursor = get_hive_cursor()
    logging.info('Executing: %s', sql)
    cursor.execute(sql)
    data_list = cursor.fetchall()
    cursor.close()
    row_html = ''
    if len(data_list) > 0:
        for data in data_list:
            entry_cnt = data[0]
            loan_cnt = data[1]
            business_rate = data[2]
            loan_amount_usd = data[3]
            average_usd = data[4]
            rate_3 = data[5]
            rate_6 = data[6]
            rate_4 = data[7]
            rate_8 = data[8]

            row_html += tr_fmt.format(

                dt=ds,
                entry_cnt=entry_cnt,
                loan_cnt=loan_cnt,
                business_rate=business_rate,
                loan_amount_usd=loan_amount_usd,
                average_usd=average_usd,
                rate_3=rate_3,
                rate_6=rate_6,
                rate_4=rate_4,
                rate_8=rate_8

            )

    return row_html


def get_rows(ds):
    tr_fmt = '''
               <tr>{row}</tr>
            '''
    weekend_tr_fmt = '''
                <tr style="background:#fff2cc">{row}</tr>
            '''

    row_fmt = '''
                            <!--分类-->
                            <td>{}</td>
                            <!--整体情况-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <!--3期产品-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <!--6期产品-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>  
                            <!--4期产品-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>   
                            <!--8期产品-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>                           
                    '''
    sql = '''
               
               select date_of_entry,
          sum(entry_cnt) as entry_cnt,
          sum(loan_cnt) as loan_cnt,
          format_number(sum(loan_amount_usd),0) as loan_amount_usd,
          format_number(sum(average_usd),0) as average_usd,
          concat(sum(business_rate),'%') as business_rate,
          
          sum(entry_cnt_3) as entry_cnt_3,
          sum(loan_cnt_3) as loan_cnt_3,
          format_number(sum(loan_amount_usd_3),0) as loan_amount_usd_3,
          format_number(sum(average_usd_3),0) as average_usd_3,
          concat(sum(business_rate_3),'%') as business_rate_3,
          
          sum(entry_cnt_6) as entry_cnt_6,
          sum(loan_cnt_6) as loan_cnt_6,
          format_number(sum(loan_amount_usd_6),0) as loan_amount_usd_6,
          format_number(sum(average_usd_6),0) as average_usd_6,
          concat(sum(business_rate_6),'%') as business_rate_6,
          
          
          sum(entry_cnt_4) as entry_cnt_4,
          sum(loan_cnt_4) as loan_cnt_4,
          format_number(sum(loan_amount_usd_4),0) as loan_amount_usd_4,
          format_number(sum(average_usd_4),0) as average_usd_4,
          concat(sum(business_rate_4),'%') as business_rate_4,
          
          sum(entry_cnt_8) as entry_cnt_8,
          sum(loan_cnt_8) as loan_cnt_8,
          format_number(sum(loan_amount_usd_8),0) as loan_amount_usd_8,
          format_number(sum(average_usd_8),0) as average_usd_8,
          concat(sum(business_rate_8),'%') as business_rate_8
          
          from(
          select date_of_entry,
          entry_cnt,
          loan_cnt,
          round(loan_amount_usd,2) loan_amount_usd,
          round(loan_amount_usd/loan_cnt,2) average_usd,
          round(loan_cnt/entry_cnt*100,2) business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8


          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_d
          where dt='{dt}'
          and terms=-10000
          and substr(date_of_entry,1,7) = substr('{dt}',1,7)
          
          union all
          
          select date_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          entry_cnt as entry_cnt_3,
          loan_cnt as loan_cnt_3,
          round(loan_amount_usd,2) loan_amount_usd_3,
          round(loan_amount_usd/loan_cnt,2) average_usd_3,
          round(loan_cnt/entry_cnt*100,2) business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_d
          where dt='{dt}'
          and terms=3
          and substr(date_of_entry,1,7) = substr('{dt}',1,7)
          
          
          union all
          select date_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          entry_cnt as entry_cnt_6,
          loan_cnt as loan_cnt_6,
          round(loan_amount_usd,2) loan_amount_usd_6,
          round(loan_amount_usd/loan_cnt,2) average_usd_6,
          round(loan_cnt/entry_cnt*100,2) business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8

          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_d
          where dt='{dt}'
          and terms=6
          and substr(date_of_entry,1,7) = substr('{dt}',1,7)
          
          
          union all
          select date_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          entry_cnt as entry_cnt_4,
          loan_cnt as loan_cnt_4,
          round(loan_amount_usd,2) loan_amount_usd_4,
          round(loan_amount_usd/loan_cnt,2) average_usd_4,
          round(loan_cnt/entry_cnt*100,2) business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_d
          where dt='{dt}'
          and terms=4
          and substr(date_of_entry,1,7) = substr('{dt}',1,7)
          
          
          
          union all
          select date_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          entry_cnt as entry_cnt_8,
          loan_cnt as loan_cnt_8,
          round(loan_amount_usd,2) loan_amount_usd_8,
          round(loan_amount_usd/loan_cnt,2) average_usd_8,
          round(loan_cnt/entry_cnt*100,2) business_rate_8
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_d
          where dt='{dt}'
          and terms=8
          and substr(date_of_entry,1,7) = substr('{dt}',1,7)
          
          ) a
          group by date_of_entry
          order by date_of_entry desc
               


                

            '''.format(dt=ds,
                       start_date=airflow.macros.ds_add(ds, -14))
    cursor = get_hive_cursor()
    logging.info('Executing: %s', sql)
    cursor.execute(sql)
    data_list = cursor.fetchall()
    cursor.close()
    row_html = ''
    if len(data_list) > 0:
        for data in data_list:
            row = row_fmt.format(*list(data))
            week = data[1]
            print (week)
            if week == '6' or week == '7':
                row_html += weekend_tr_fmt.format(row=row)
            else:
                row_html += tr_fmt.format(row=row)

    return row_html




def get_month_rows(ds):
    tr_fmt = '''
               <tr>{row}</tr>
            '''
    row_fmt = '''
                           <!--分类-->
                            <td>{}</td>
                            <!--整体情况-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <!--3期产品-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <!--6期产品-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>  
                            <!--4期产品-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <!--8期产品-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>                    
                    '''
    sql = '''
                
                
                select month_of_entry,
          sum(entry_cnt) as entry_cnt,
          sum(loan_cnt) as loan_cnt,
          format_number(sum(loan_amount_usd),0) as loan_amount_usd,
          format_number(sum(average_usd),0) as average_usd,
          concat(sum(business_rate),'%') as business_rate,
          
          sum(entry_cnt_3) as entry_cnt_3,
          sum(loan_cnt_3) as loan_cnt_3,
          format_number(sum(loan_amount_usd_3),0) as loan_amount_usd_3,
          format_number(sum(average_usd_3),0) as average_usd_3,
          concat(sum(business_rate_3),'%') as business_rate_3,
          
          sum(entry_cnt_6) as entry_cnt_6,
          sum(loan_cnt_6) as loan_cnt_6,
          format_number(sum(loan_amount_usd_6),0) as loan_amount_usd_6,
          format_number(sum(average_usd_6),0) as average_usd_6,
          concat(sum(business_rate_6),'%') as business_rate_6,
          
          
          nvl(sum(entry_cnt_4),'') as entry_cnt_4,
          nvl(sum(loan_cnt_4),'') as loan_cnt_4,
          nvl(format_number(sum(loan_amount_usd_4),0),'') as loan_amount_usd_4,
          nvl(format_number(sum(average_usd_4),0),'') as average_usd_4,
          nvl(concat(sum(business_rate_4),'%'),'') as business_rate_4,
          
          nvl(sum(entry_cnt_8),'') as entry_cnt_8,
          nvl(sum(loan_cnt_8),'') as loan_cnt_8,
          nvl(format_number(sum(loan_amount_usd_8),0),'') as loan_amount_usd_8,
          nvl(format_number(sum(average_usd_8),0),'') as average_usd_8,
          nvl(concat(sum(business_rate_8),'%'),'') as business_rate_8
          
          from(
          select month_of_entry,
          entry_cnt,
          loan_cnt,
          round(loan_amount_usd,2) loan_amount_usd,
          round(loan_amount_usd/loan_cnt,2) average_usd,
          round(loan_cnt/entry_cnt*100,2) business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8
          
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_m
          where dt='{dt}'
          and terms=-10000
          
          union all
          select month_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          entry_cnt as entry_cnt_3,
          loan_cnt as loan_cnt_3,
          round(loan_amount_usd,2) loan_amount_usd_3,
          round(loan_amount_usd/loan_cnt,2) average_usd_3,
          round(loan_cnt/entry_cnt*100,2) business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8
          
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_m
          where dt='{dt}'
          and terms=3
          
          
          union all
          select month_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          entry_cnt as entry_cnt_6,
          loan_cnt as loan_cnt_6,
          round(loan_amount_usd,2) loan_amount_usd_6,
          round(loan_amount_usd/loan_cnt,2) average_usd_6,
          round(loan_cnt/entry_cnt*100,2) business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_m
          where dt='{dt}'
          and terms=6


          union all
          select month_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          entry_cnt as entry_cnt_4,
          loan_cnt as loan_cnt_4,
          round(loan_amount_usd,2) loan_amount_usd_4,
          round(loan_amount_usd/loan_cnt,2) average_usd_4,
          round(loan_cnt/entry_cnt*100,2) business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_m
          where dt='{dt}'
          and terms=4
          
          
        union all
          select month_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          entry_cnt as entry_cnt_8,
          loan_cnt as loan_cnt_8,
          round(loan_amount_usd,2) loan_amount_usd_8,
          round(loan_amount_usd/loan_cnt,2) average_usd_8,
          round(loan_cnt/entry_cnt*100,2) business_rate_8
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_m
          where dt='{dt}'
          and terms=8
          ) a
          group by month_of_entry
          order by month_of_entry desc


                 '''.format(dt=ds,
                            start_date=airflow.macros.ds_add(ds, -14))
    cursor = get_hive_cursor()
    logging.info('Executing: %s', sql)
    cursor.execute(sql)
    data_list = cursor.fetchall()
    cursor.close()
    row_html = ''
    if len(data_list) > 0:
        for data in data_list:
            row = row_fmt.format(*list(data))
            row_html += tr_fmt.format(row=row)

    return row_html


def get_week_rows(ds):
    tr_fmt = '''
               <tr>{row}</tr>
            '''
    row_fmt = '''
                           <!--分类-->
                            <td>{}</td>
                            <!--整体情况-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <!--3期产品-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <!--6期产品-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>         
                            <!--4期产品-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <!--8期产品-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>             
                    '''
    sql = '''
       
       
              select dateweek_of_entry,
          sum(entry_cnt) as entry_cnt,
          sum(loan_cnt) as loan_cnt,
          format_number(sum(loan_amount_usd),0) as loan_amount_usd,
          format_number(sum(average_usd),0) as average_usd,
          concat(sum(business_rate),'%') as business_rate,
          
          sum(entry_cnt_3) as entry_cnt_3,
          sum(loan_cnt_3) as loan_cnt_3,
          format_number(sum(loan_amount_usd_3),0) as loan_amount_usd_3,
          format_number(sum(average_usd_3),0) as average_usd_3,
          concat(sum(business_rate_3),'%') as business_rate_3,
          
          sum(entry_cnt_6) as entry_cnt_6,
          sum(loan_cnt_6) as loan_cnt_6,
          format_number(sum(loan_amount_usd_6),0) as loan_amount_usd_6,
          format_number(sum(average_usd_6),0) as average_usd_6,
          concat(sum(business_rate_6),'%') as business_rate_6,
          
          nvl(sum(entry_cnt_4),'') as entry_cnt_4,
          nvl(sum(loan_cnt_4),'') as loan_cnt_4,
          nvl(format_number(sum(loan_amount_usd_4),0),'') as loan_amount_usd_4,
          nvl(format_number(sum(average_usd_4),0),'') as average_usd_4,
          nvl(concat(sum(business_rate_4),'%'),'') as business_rate_4,
          
          nvl(sum(entry_cnt_8),'') as entry_cnt_8,
          nvl(sum(loan_cnt_8),'') as loan_cnt_8,
          nvl(format_number(sum(loan_amount_usd_8),0),'') as loan_amount_usd_8,
          nvl(format_number(sum(average_usd_8),0),'') as average_usd_8,
          nvl(concat(sum(business_rate_8),'%'),'') as business_rate_8
          
          
          
          from(
          
          select 
          dateweek_of_entry,
          entry_cnt,
          loan_cnt,
          round(loan_amount_usd,2) loan_amount_usd,
          round(loan_amount_usd/loan_cnt,2) average_usd,
          round(loan_cnt/entry_cnt*100,2) business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_w
          where dt='{dt}'
          and terms=-10000
          

          union all
          
          select
          dateweek_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          entry_cnt as entry_cnt_3,
          loan_cnt as loan_cnt_3,
          round(loan_amount_usd,2) loan_amount_usd_3,
          round(loan_amount_usd/loan_cnt,2) average_usd_3,
          round(loan_cnt/entry_cnt*100,2) business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_w
          where dt='{dt}'
          and terms=3
          
          
          union all
          select 
          dateweek_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          entry_cnt as entry_cnt_6,
          loan_cnt as loan_cnt_6,
          round(loan_amount_usd,2) loan_amount_usd_6,
          round(loan_amount_usd/loan_cnt,2) average_usd_6,
          round(loan_cnt/entry_cnt*100,2) business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8

          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_w
          where dt='{dt}'
          and terms=6
          
          
          union all
          select 
          dateweek_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          entry_cnt as entry_cnt_4,
          loan_cnt as loan_cnt_4,
          round(loan_amount_usd,2) loan_amount_usd_4,
          round(loan_amount_usd/loan_cnt,2) average_usd_4,
          round(loan_cnt/entry_cnt*100,2) business_rate_4,
          null as entry_cnt_8,
          null as loan_cnt_8,
          null as loan_amount_usd_8,
          null as average_usd_8,
          null as business_rate_8

          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_w
          where dt='{dt}'
          and terms=4
          
          
          
          union all
          select 
          dateweek_of_entry,
          null as entry_cnt,
          null as loan_cnt,
          null as loan_amount_usd,
          null as average_usd,
          null as business_rate,
          null as entry_cnt_3,
          null as loan_cnt_3,
          null as loan_amount_usd_3,
          null as average_usd_3,
          null as business_rate_3,
          null as entry_cnt_6,
          null as loan_cnt_6,
          null as loan_amount_usd_6,
          null as average_usd_6,
          null as business_rate_6,
          null as entry_cnt_4,
          null as loan_cnt_4,
          null as loan_amount_usd_4,
          null as average_usd_4,
          null as business_rate_4,
          entry_cnt as entry_cnt_8,
          loan_cnt as loan_cnt_8,
          round(loan_amount_usd,2) loan_amount_usd_8,
          round(loan_amount_usd/loan_cnt,2) average_usd_8,
          round(loan_cnt/entry_cnt*100,2) business_rate_8

          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_w
          where dt='{dt}'
          and terms=8
          
          
          ) a
          group by dateweek_of_entry
          order by dateweek_of_entry desc




                 '''.format(dt=ds,
                            start_date=airflow.macros.ds_add(ds, -14))
    cursor = get_hive_cursor()
    logging.info('Executing: %s', sql)
    cursor.execute(sql)
    data_list = cursor.fetchall()
    cursor.close()
    row_html = ''
    if len(data_list) > 0:
        for data in data_list:
            row = row_fmt.format(*list(data))
            row_html += tr_fmt.format(row=row)

    return row_html


def send_report_email(ds, **kwargs):
    # logging.info("receivers:%s" % Variable.get("oride_global_operate_report_receivers"))

    month_fmt = '''
                            <tr>
                                <th colspan="1" style="text-align: center;">分类</th>
                                <th colspan="5" style="text-align: center;">总体</th>
                                <th colspan="5" style="text-align: center;">3期</th>
                                <th colspan="5" style="text-align: center;">6期</th>
                                <th colspan="5" style="text-align: center;">4期</th>
                                <th colspan="5" style="text-align: center;">8期</th>
                            </tr>
                            <tr>
                            <!--分类-->
                            <th>进件月份</th>
                            <!--整体情况-->
                            <th>进件量</th>
                            <th>放款量</th>
                            <th>贷款额_usd</th>
                            <th>件均_usd</th>
                            <th>业务转化率</th>
                            <!--3期产品-->
                            <th>进件量</th>
                            <th>放款量</th>
                            <th>贷款额_usd</th>
                            <th>件均_usd</th>
                            <th>业务转化率</th>
                            <!--6期产品-->
                            <th>进件量</th>
                            <th>放款量</th>
                            <th>贷款额_usd</th>
                            <th>件均_usd</th>
                            <th>业务转化率</th>
                            <!--4期产品-->
                            <th>进件量</th>
                            <th>放款量</th>
                            <th>贷款额_usd</th>
                            <th>件均_usd</th>
                            <th>业务转化率</th>
                            <!--8期产品-->
                            <th>进件量</th>
                            <th>放款量</th>
                            <th>贷款额_usd</th>
                            <th>件均_usd</th>
                            <th>业务转化率</th>
                        </tr>
    '''

    week_fmt = '''
                                <tr>
                                    <th colspan="1" style="text-align: center;">分类</th>
                                    <th colspan="5" style="text-align: center;">总体</th>
                                    <th colspan="5" style="text-align: center;">3期产品</th>
                                    <th colspan="5" style="text-align: center;">6期产品</th>
                                    <th colspan="5" style="text-align: center;">4期产品</th>
                                    <th colspan="5" style="text-align: center;">8期产品</th>
                                </tr>
                                <tr>
                                <!--分类-->
                                <th>进件周</th>
                                <!--整体情况-->
                                <th>进件量</th>
                                <th>放款量</th>
                                <th>贷款额_usd</th>
                                <th>件均_usd</th>
                                <th>业务转化率</th>
                                <!--3期产品-->
                                <th>进件量</th>
                                <th>放款量</th>
                                <th>贷款额_usd</th>
                                <th>件均_usd</th>
                                <th>业务转化率</th>
                                <!--6期产品-->
                                <th>进件量</th>
                                <th>放款量</th>
                                <th>贷款额_usd</th>
                                <th>件均_usd</th>
                                <th>业务转化率</th>
                                <!--4期产品-->
                                <th>进件量</th>
                                <th>放款量</th>
                                <th>贷款额_usd</th>
                                <th>件均_usd</th>
                                <th>业务转化率</th>
                                <!--8期产品-->
                                <th>进件量</th>
                                <th>放款量</th>
                                <th>贷款额_usd</th>
                                <th>件均_usd</th>
                                <th>业务转化率</th>
                            </tr>
        '''

    html_fmt = '''
            <html>
            <head>
            <title></title>
            <style type="text/css">
                table
                {{
                    font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
                    border-collapse: collapse;
                    margin: 0 auto;
                    text-align: left;
                    align:left;
                }}
                table td, table th
                {{
                    border: 1px solid #000000;
                    color: #000000;
                    height: 30px;
                    padding: 5px 10px 5px 5px;
                }}
                table thead th
                {{
                    background-color: #F39C12;
                    //color: white;
                    width: 100px;
                    color: #000000;
                }}
            </style>
            </head>
            <body>
            
            {show_date}
            {show_month}
            {show_all}
                <table width="100%" class="table">
                    <caption>
                        <h3>当月每日进件-放款转化情况</h3>
                    </caption>
                </table>
                <table width="100%" class="table">
                    <thead>
                        <tr>
                            <th colspan="1" style="text-align: center;">分类</th>
                            <th colspan="5" style="text-align: center;">总体</th>
                            <th colspan="5" style="text-align: center;">3期</th>
                            <th colspan="5" style="text-align: center;">6期</th>
                            <th colspan="5" style="text-align: center;">4期</th>
                            <th colspan="5" style="text-align: center;">8期</th>
                        </tr>
                        <tr>
                            <!--分类-->
                            <th>进件日期</th>
                            <!--整体情况-->
                            <th>进件量</th>
                            <th>放款量</th>
                            <th>贷款额_usd</th>
                            <th>件均_usd</th>
                            <th>业务转化率</th>
                            <!--3期产品-->
                            <th>进件量</th>
                            <th>放款量</th>
                            <th>贷款额_usd</th>
                            <th>件均_usd</th>
                            <th>业务转化率</th>
                            <!--6期产品-->
                            <th>进件量</th>
                            <th>放款量</th>
                            <th>贷款额_usd</th>
                            <th>件均_usd</th>
                            <th>业务转化率</th>
                            <!--4期产品-->
                            <th>进件量</th>
                            <th>放款量</th>
                            <th>贷款额_usd</th>
                            <th>件均_usd</th>
                            <th>业务转化率</th>
                            <!--8期产品-->
                            <th>进件量</th>
                            <th>放款量</th>
                            <th>贷款额_usd</th>
                            <th>件均_usd</th>
                            <th>业务转化率</th>
                            
                        </tr>
                    </thead>
                    <tbody>
                    {rows}
                    </tbody>
                </table>
                <table width="100%" class="table">
                <caption>
                    <h3>历史各月进件-放款转化情况</h3>
                </caption>
                </table>
                <table width="100%" class="table">
                    <thead>
                    {month_fmt}
                    </thead>
                    <tbody>
                    {month_rows}
                    </tbody>
                </table>
                
                <table width="100%" class="table">
                <caption>
                    <h3>历史各周进件-放款转化情况</h3>
                </caption>
                </table>
                <table width="100%" class="table">
                    <thead>
                    {week_fmt}
                    </thead>
                    <tbody>
                    {week_rows}
                    </tbody>
                </table>
                
        </body>
        </html>
        '''

    # print (result)
    # print (len(result))
    html = html_fmt.format(rows=get_rows(ds),
                           month_rows=get_month_rows(ds),
                           week_rows=get_week_rows(ds),
                           month_fmt=month_fmt,
                           week_fmt=week_fmt,
                           show_date=get_show_date(ds),
                           show_month=get_show_month(ds),
                           show_all=get_show_all(ds)
                           )  #

    logging.info("==============")
    # logging.info(html)

    #email_to = Variable.get("oride_global_operate_report_receivers").split()
    email_to = Variable.get("ocredit_phones_global_operate_report_receivers").split()

    #email_to = ['bigdata@opay-inc.com','lili.chen@opay-inc.com']

    #email_to = ['lili.chen@opay-inc.com']
    #email_to = ['shuai01.li@opay-inc.com','lili.chen@opay-inc.com','jiaying.kang@opay-inc.com']
    #email_to = ['jiaying.kang@opay-inc.com']
    # send mail

    email_subject = 'ocredit_phones业务转化日报_{}'.format(ds)
    send_email(
        email_to
        , email_subject, html, mime_charset='utf-8')
    return


send_report = PythonOperator(
    task_id='send_report',
    python_callable=send_report_email,
    provide_context=True,
    dag=dag
)

app_ocredit_phones_order_base_cube_d_task >> send_report
app_ocredit_phones_order_base_cube_m_task >> send_report
app_ocredit_phones_order_base_cube_w_task >> send_report


