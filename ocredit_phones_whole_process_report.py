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
    'start_date': datetime(2020, 2, 26),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    'ocredit_phones_whole_process_report',
    schedule_interval="40 01 * * *",
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
            附件为截止到昨天的手机分期全流程日报情况，主要内容如下：<br/>
            1、当日情况：昨日{date_of_entry}进件{entry_cnt}笔，初审通过{pre_amount}笔，复审通过{review_amount}笔，风控通过率为{all_rate}，其中3期风控通过率为{all_rate_3}，6期风控通过率为{all_rate_6}；
            </tr>
            '''

    sql = '''
          select date_of_entry,entry_cnt,
          pre_amount,review_amount,all_rate,
          all_rate_3,all_rate_6
          from(
          select
          date_of_entry,
          sum(entry_cnt) as entry_cnt,
          sum(pre_amount) as pre_amount,
          sum(review_amount) as review_amount,
          sum(loan_cnt) as loan_cnt,
          concat(sum(pre_rate),'%') as pre_rate,
          concat(sum(review_rate),'%') as review_rate,
          concat(sum(all_rate),'%') as all_rate,
          
          sum(entry_cnt_3) as entry_cnt_3,
          sum(pre_amount_3) as pre_amount_3,
          sum(review_amount_3) as review_amount_3,
          sum(loan_cnt_3) as loan_cnt_3,
          concat(sum(pre_rate_3),'%') as pre_rate_3,
          concat(sum(review_rate_3),'%') as review_rate_3,
          concat(sum(all_rate_3),'%') as all_rate_3,
          
          sum(entry_cnt_6) as entry_cnt_6,
          sum(pre_amount_6) as pre_amount_6,
          sum(review_amount_6) as review_amount_6,
          sum(loan_cnt_6) as loan_cnt_6,
          concat(sum(pre_rate_6),'%') as pre_rate_6,
          concat(sum(review_rate_6),'%') as review_rate_6,
          concat(sum(all_rate_6),'%') as all_rate_6
          
          from(
          select
          date_of_entry,
          entry_cnt,
          pre_amount,
          review_amount,
          loan_cnt,
          round(pre_amount/entry_cnt*100,2) pre_rate,
          round(review_amount/pre_amount*100,2) review_rate,
          round(review_amount/entry_cnt*100,2) all_rate,
          null as entry_cnt_3,
          null as pre_amount_3,
          null as review_amount_3,
          null as loan_cnt_3,
          null as pre_rate_3,
          null as review_rate_3,
          null as all_rate_3,
          null as entry_cnt_6,
          null as pre_amount_6,
          null as review_amount_6,
          null as loan_cnt_6,
          null as pre_rate_6,
          null as review_rate_6,
          null as all_rate_6
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_d
          where dt='{dt}'
          and terms=-10000
          and substr(date_of_entry,1,7) = substr('{dt}',1,7)
          
          union all
          
          select
          date_of_entry,
          null as entry_cnt,
          null as pre_amount,
          null as review_amount,
          null as loan_cnt,
          null as pre_rate,
          null as review_rate,
          null as all_rate,
          entry_cnt as entry_cnt_3,
          pre_amount as pre_amount_3,
          review_amount as review_amount_3,
          loan_cnt as loan_cnt_3,
          round(pre_amount/entry_cnt*100,2) pre_rate_3,
          round(review_amount/pre_amount*100,2) review_rate_3,
          round(review_amount/entry_cnt*100,2) all_rate_3,
          null as entry_cnt_6,
          null as pre_amount_6,
          null as review_amount_6,
          null as loan_cnt_6,
          null as pre_rate_6,
          null as review_rate_6,
          null as all_rate_6
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_d
          where dt='{dt}'
          and terms=3
          and substr(date_of_entry,1,7) = substr('{dt}',1,7)
          
          
          union all


          select
          date_of_entry,
          null as entry_cnt,
          null as pre_amount,
          null as review_amount,
          null as loan_cnt,
          null as pre_rate,
          null as review_rate,
          null as all_rate,
          null as entry_cnt_3,
          null as pre_amount_3,
          null as review_amount_3,
          null as loan_cnt_3,
          null as pre_rate_3,
          null as review_rate_3,
          null as all_rate_3,
          entry_cnt as entry_cnt_6,
          pre_amount as pre_amount_6,
          review_amount as review_amount_6,
          loan_cnt as loan_cnt_6,
          round(pre_amount/entry_cnt*100,2) pre_rate_6,
          round(review_amount/pre_amount*100,2) review_rate_6,
          round(review_amount/entry_cnt*100,2) all_rate_6
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_d
          where dt='{dt}'
          and terms=6
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
            date_of_entry = data[0]
            entry_cnt = data[1]
            pre_amount = data[2]
            review_amount = data[3]
            all_rate = data[4]
            all_rate_3 = data[5]
            all_rate_6 = data[6]

            row_html += tr_fmt.format(

                date_of_entry=date_of_entry,
                entry_cnt=entry_cnt,
                pre_amount=pre_amount,
                review_amount = review_amount,
                all_rate = all_rate,
                all_rate_3 = all_rate_3,
                all_rate_6 = all_rate_6

            )

    return row_html





def get_show_month(ds):
    tr_fmt = '''
            <tr>
            2、本月情况：截止到昨日{dt}进件{entry_cnt}笔，初审通过{pre_amount}笔，复审通过{review_amount}笔，风控通过率为{all_rate}，其中3期风控通过率为{all_rate_3}，6期风控通过率为{all_rate_6}；<br/>
            备注1：初审通过率=初审通过的/进件量<br/>
            备注2：复审通过率=复审通过的/初审通过的<br/>
            备注3：风控整体通过率=复审通过的/进件量<br/>
            以上敬请参考，如有疑问，随时沟通！
            </tr>
            '''

    sql = '''
          select 
          entry_cnt,pre_amount,review_amount,all_rate,
          all_rate_3,all_rate_6
          
          from(
          select
          month_of_entry,
          sum(entry_cnt) as entry_cnt,
          sum(pre_amount) as pre_amount,
          sum(review_amount) as review_amount,
          sum(loan_cnt) as loan_cnt,
          concat(sum(pre_rate),'%') as pre_rate,
          concat(sum(review_rate),'%') as review_rate,
          concat(sum(all_rate),'%') as all_rate,
          
          sum(entry_cnt_3) as entry_cnt_3,
          sum(pre_amount_3) as pre_amount_3,
          sum(review_amount_3) as review_amount_3,
          sum(loan_cnt_3) as loan_cnt_3,
          concat(sum(pre_rate_3),'%') as pre_rate_3,
          concat(sum(review_rate_3),'%') as review_rate_3,
          concat(sum(all_rate_3),'%') as all_rate_3,
          
          sum(entry_cnt_6) as entry_cnt_6,
          sum(pre_amount_6) as pre_amount_6,
          sum(review_amount_6) as review_amount_6,
          sum(loan_cnt_6) as loan_cnt_6,
          concat(sum(pre_rate_6),'%') as pre_rate_6,
          concat(sum(review_rate_6),'%') as review_rate_6,
          concat(sum(all_rate_6),'%') as all_rate_6
          
          from(
          select
          month_of_entry,
          entry_cnt,
          pre_amount,
          review_amount,
          loan_cnt,
          round(pre_amount/entry_cnt*100,2) pre_rate,
          round(review_amount/pre_amount*100,2) review_rate,
          round(review_amount/entry_cnt*100,2) all_rate,
          null as entry_cnt_3,
          null as pre_amount_3,
          null as review_amount_3,
          null as loan_cnt_3,
          null as pre_rate_3,
          null as review_rate_3,
          null as all_rate_3,
          null as entry_cnt_6,
          null as pre_amount_6,
          null as review_amount_6,
          null as loan_cnt_6,
          null as pre_rate_6,
          null as review_rate_6,
          null as all_rate_6

          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_m
          where dt='{dt}'
          and terms=-10000
          
          
          union all
          
          select
          month_of_entry,
          null as entry_cnt,
          null as pre_amount,
          null as review_amount,
          null as loan_cnt,
          null as pre_rate,
          null as review_rate,
          null as all_rate,
          entry_cnt as entry_cnt_3,
          pre_amount as pre_amount_3,
          review_amount as review_amount_3,
          loan_cnt as loan_cnt_3,
          round(pre_amount/entry_cnt*100,2) pre_rate_3,
          round(review_amount/pre_amount*100,2) review_rate_3,
          round(review_amount/entry_cnt*100,2) all_rate_3,
          null as entry_cnt_6,
          null as pre_amount_6,
          null as review_amount_6,
          null as loan_cnt_6,
          null as pre_rate_6,
          null as review_rate_6,
          null as all_rate_6
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_m
          where dt='{dt}'
          and terms=3
          
          
          
          union all


          select
          month_of_entry,
          null as entry_cnt,
          null as pre_amount,
          null as review_amount,
          null as loan_cnt,
          null as pre_rate,
          null as review_rate,
          null as all_rate,
          null as entry_cnt_3,
          null as pre_amount_3,
          null as review_amount_3,
          null as loan_cnt_3,
          null as pre_rate_3,
          null as review_rate_3,
          null as all_rate_3,
          entry_cnt as entry_cnt_6,
          pre_amount as pre_amount_6,
          review_amount as review_amount_6,
          loan_cnt as loan_cnt_6,
          round(pre_amount/entry_cnt*100,2) pre_rate_6,
          round(review_amount/pre_amount*100,2) review_rate_6,
          round(review_amount/entry_cnt*100,2) all_rate_6
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_m
          where dt='{dt}'
          and terms=6
          
          ) a
          group by month_of_entry
          order by month_of_entry 
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
            pre_amount = data[1]
            review_amount = data[2]
            all_rate = data[3]
            all_rate_3 = data[4]
            all_rate_6 = data[5]

            row_html += tr_fmt.format(

                dt=ds,
                entry_cnt=entry_cnt,
                pre_amount=pre_amount,
                review_amount = review_amount,
                all_rate = all_rate,
                all_rate_3 = all_rate_3,
                all_rate_6 = all_rate_6
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
                            <td>{}</td>
                            <td>{}</td>
                            <!--3期产品-->
                            <td>{}</td>
                            <td>{}</td>
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
                            <td>{}</td>
                            <td>{}</td>                       
                    '''
    sql = '''

              
              select
          date_of_entry,
          sum(entry_cnt) as entry_cnt,
          sum(pre_amount) as pre_amount,
          sum(review_amount) as review_amount,
          sum(loan_cnt) as loan_cnt,
          concat(sum(pre_rate),'%') as pre_rate,
          concat(sum(review_rate),'%') as review_rate,
          concat(sum(all_rate),'%') as all_rate,
          
          sum(entry_cnt_3) as entry_cnt_3,
          sum(pre_amount_3) as pre_amount_3,
          sum(review_amount_3) as review_amount_3,
          sum(loan_cnt_3) as loan_cnt_3,
          concat(sum(pre_rate_3),'%') as pre_rate_3,
          concat(sum(review_rate_3),'%') as review_rate_3,
          concat(sum(all_rate_3),'%') as all_rate_3,
          
          sum(entry_cnt_6) as entry_cnt_6,
          sum(pre_amount_6) as pre_amount_6,
          sum(review_amount_6) as review_amount_6,
          sum(loan_cnt_6) as loan_cnt_6,
          concat(sum(pre_rate_6),'%') as pre_rate_6,
          concat(sum(review_rate_6),'%') as review_rate_6,
          concat(sum(all_rate_6),'%') as all_rate_6
          
          from(
          select
          date_of_entry,
          entry_cnt,
          pre_amount,
          review_amount,
          loan_cnt,
          round(pre_amount/entry_cnt*100,2) pre_rate,
          round(review_amount/pre_amount*100,2) review_rate,
          round(review_amount/entry_cnt*100,2) all_rate,
          null as entry_cnt_3,
          null as pre_amount_3,
          null as review_amount_3,
          null as loan_cnt_3,
          null as pre_rate_3,
          null as review_rate_3,
          null as all_rate_3,
          null as entry_cnt_6,
          null as pre_amount_6,
          null as review_amount_6,
          null as loan_cnt_6,
          null as pre_rate_6,
          null as review_rate_6,
          null as all_rate_6
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_d
          where dt='{dt}'
          and terms=-10000
          and substr(date_of_entry,1,7) = substr('{dt}',1,7)
          
          union all
          
          select
          date_of_entry,
          null as entry_cnt,
          null as pre_amount,
          null as review_amount,
          null as loan_cnt,
          null as pre_rate,
          null as review_rate,
          null as all_rate,
          entry_cnt as entry_cnt_3,
          pre_amount as pre_amount_3,
          review_amount as review_amount_3,
          loan_cnt as loan_cnt_3,
          round(pre_amount/entry_cnt*100,2) pre_rate_3,
          round(review_amount/pre_amount*100,2) review_rate_3,
          round(review_amount/entry_cnt*100,2) all_rate_3,
          null as entry_cnt_6,
          null as pre_amount_6,
          null as review_amount_6,
          null as loan_cnt_6,
          null as pre_rate_6,
          null as review_rate_6,
          null as all_rate_6
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_d
          where dt='{dt}'
          and terms=3
          and substr(date_of_entry,1,7) = substr('{dt}',1,7)
          
          
          union all


          select
          date_of_entry,
          null as entry_cnt,
          null as pre_amount,
          null as review_amount,
          null as loan_cnt,
          null as pre_rate,
          null as review_rate,
          null as all_rate,
          null as entry_cnt_3,
          null as pre_amount_3,
          null as review_amount_3,
          null as loan_cnt_3,
          null as pre_rate_3,
          null as review_rate_3,
          null as all_rate_3,
          entry_cnt as entry_cnt_6,
          pre_amount as pre_amount_6,
          review_amount as review_amount_6,
          loan_cnt as loan_cnt_6,
          round(pre_amount/entry_cnt*100,2) pre_rate_6,
          round(review_amount/pre_amount*100,2) review_rate_6,
          round(review_amount/entry_cnt*100,2) all_rate_6
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_d
          where dt='{dt}'
          and terms=6
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
                            <td>{}</td>
                            <td>{}</td>
                            <!--3期产品-->
                            <td>{}</td>
                            <td>{}</td>
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
                            <td>{}</td>
                            <td>{}</td>           
                    '''
    sql = '''


                 select
          month_of_entry,
          sum(entry_cnt) as entry_cnt,
          sum(pre_amount) as pre_amount,
          sum(review_amount) as review_amount,
          sum(loan_cnt) as loan_cnt,
          concat(sum(pre_rate),'%') as pre_rate,
          concat(sum(review_rate),'%') as review_rate,
          concat(sum(all_rate),'%') as all_rate,
          
          sum(entry_cnt_3) as entry_cnt_3,
          sum(pre_amount_3) as pre_amount_3,
          sum(review_amount_3) as review_amount_3,
          sum(loan_cnt_3) as loan_cnt_3,
          concat(sum(pre_rate_3),'%') as pre_rate_3,
          concat(sum(review_rate_3),'%') as review_rate_3,
          concat(sum(all_rate_3),'%') as all_rate_3,
          
          sum(entry_cnt_6) as entry_cnt_6,
          sum(pre_amount_6) as pre_amount_6,
          sum(review_amount_6) as review_amount_6,
          sum(loan_cnt_6) as loan_cnt_6,
          concat(sum(pre_rate_6),'%') as pre_rate_6,
          concat(sum(review_rate_6),'%') as review_rate_6,
          concat(sum(all_rate_6),'%') as all_rate_6
          
          from(
          select
          month_of_entry,
          entry_cnt,
          pre_amount,
          review_amount,
          loan_cnt,
          round(pre_amount/entry_cnt*100,2) pre_rate,
          round(review_amount/pre_amount*100,2) review_rate,
          round(review_amount/entry_cnt*100,2) all_rate,
          null as entry_cnt_3,
          null as pre_amount_3,
          null as review_amount_3,
          null as loan_cnt_3,
          null as pre_rate_3,
          null as review_rate_3,
          null as all_rate_3,
          null as entry_cnt_6,
          null as pre_amount_6,
          null as review_amount_6,
          null as loan_cnt_6,
          null as pre_rate_6,
          null as review_rate_6,
          null as all_rate_6

          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_m
          where dt='{dt}'
          and terms=-10000
          
          
          union all
          
          select
          month_of_entry,
          null as entry_cnt,
          null as pre_amount,
          null as review_amount,
          null as loan_cnt,
          null as pre_rate,
          null as review_rate,
          null as all_rate,
          entry_cnt as entry_cnt_3,
          pre_amount as pre_amount_3,
          review_amount as review_amount_3,
          loan_cnt as loan_cnt_3,
          round(pre_amount/entry_cnt*100,2) pre_rate_3,
          round(review_amount/pre_amount*100,2) review_rate_3,
          round(review_amount/entry_cnt*100,2) all_rate_3,
          null as entry_cnt_6,
          null as pre_amount_6,
          null as review_amount_6,
          null as loan_cnt_6,
          null as pre_rate_6,
          null as review_rate_6,
          null as all_rate_6
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_m
          where dt='{dt}'
          and terms=3
          
          
          
          union all


          select
          month_of_entry,
          null as entry_cnt,
          null as pre_amount,
          null as review_amount,
          null as loan_cnt,
          null as pre_rate,
          null as review_rate,
          null as all_rate,
          null as entry_cnt_3,
          null as pre_amount_3,
          null as review_amount_3,
          null as loan_cnt_3,
          null as pre_rate_3,
          null as review_rate_3,
          null as all_rate_3,
          entry_cnt as entry_cnt_6,
          pre_amount as pre_amount_6,
          review_amount as review_amount_6,
          loan_cnt as loan_cnt_6,
          round(pre_amount/entry_cnt*100,2) pre_rate_6,
          round(review_amount/pre_amount*100,2) review_rate_6,
          round(review_amount/entry_cnt*100,2) all_rate_6
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_m
          where dt='{dt}'
          and terms=6
          
          ) a
          group by month_of_entry
          order by month_of_entry 



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
                            <td>{}</td>
                            <td>{}</td>
                            <!--3期产品-->
                            <td>{}</td>
                            <td>{}</td>
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
                            <td>{}</td>
                            <td>{}</td>            
                    '''
    sql = '''


               select
          dateweek_of_entry,
          sum(entry_cnt) as entry_cnt,
          sum(pre_amount) as pre_amount,
          sum(review_amount) as review_amount,
          sum(loan_cnt) as loan_cnt,
          concat(sum(pre_rate),'%') as pre_rate,
          concat(sum(review_rate),'%') as review_rate,
          concat(sum(all_rate),'%') as all_rate,
          
          sum(entry_cnt_3) as entry_cnt_3,
          sum(pre_amount_3) as pre_amount_3,
          sum(review_amount_3) as review_amount_3,
          sum(loan_cnt_3) as loan_cnt_3,
          concat(sum(pre_rate_3),'%') as pre_rate_3,
          concat(sum(review_rate_3),'%') as review_rate_3,
          concat(sum(all_rate_3),'%') as all_rate_3,
          
          sum(entry_cnt_6) as entry_cnt_6,
          sum(pre_amount_6) as pre_amount_6,
          sum(review_amount_6) as review_amount_6,
          sum(loan_cnt_6) as loan_cnt_6,
          concat(sum(pre_rate_6),'%') as pre_rate_6,
          concat(sum(review_rate_6),'%') as review_rate_6,
          concat(sum(all_rate_6),'%') as all_rate_6
          
          from(
          select
          dateweek_of_entry,
          entry_cnt,
          pre_amount,
          review_amount,
          loan_cnt,
          round(pre_amount/entry_cnt*100,2) pre_rate,
          round(review_amount/pre_amount*100,2) review_rate,
          round(review_amount/entry_cnt*100,2) all_rate,
          null as entry_cnt_3,
          null as pre_amount_3,
          null as review_amount_3,
          null as loan_cnt_3,
          null as pre_rate_3,
          null as review_rate_3,
          null as all_rate_3,
          null as entry_cnt_6,
          null as pre_amount_6,
          null as review_amount_6,
          null as loan_cnt_6,
          null as pre_rate_6,
          null as review_rate_6,
          null as all_rate_6
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_w
          where dt='{dt}'
          and terms=-10000
          
          
          union all
          
          select
          dateweek_of_entry,
          null as entry_cnt,
          null as pre_amount,
          null as review_amount,
          null as loan_cnt,
          null as pre_rate,
          null as review_rate,
          null as all_rate,
          entry_cnt as entry_cnt_3,
          pre_amount as pre_amount_3,
          review_amount as review_amount_3,
          loan_cnt as loan_cnt_3,
          round(pre_amount/entry_cnt*100,2) pre_rate_3,
          round(review_amount/pre_amount*100,2) review_rate_3,
          round(review_amount/entry_cnt*100,2) all_rate_3,
          null as entry_cnt_6,
          null as pre_amount_6,
          null as review_amount_6,
          null as loan_cnt_6,
          null as pre_rate_6,
          null as review_rate_6,
          null as all_rate_6
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_w
          where dt='{dt}'
          and terms=3
          
          
          
          union all


          select
          dateweek_of_entry,
          null as entry_cnt,
          null as pre_amount,
          null as review_amount,
          null as loan_cnt,
          null as pre_rate,
          null as review_rate,
          null as all_rate,
          null as entry_cnt_3,
          null as pre_amount_3,
          null as review_amount_3,
          null as loan_cnt_3,
          null as pre_rate_3,
          null as review_rate_3,
          null as all_rate_3,
          entry_cnt as entry_cnt_6,
          pre_amount as pre_amount_6,
          review_amount as review_amount_6,
          loan_cnt as loan_cnt_6,
          round(pre_amount/entry_cnt*100,2) pre_rate_6,
          round(review_amount/pre_amount*100,2) review_rate_6,
          round(review_amount/entry_cnt*100,2) all_rate_6
          
          from ocredit_phones_dw.app_ocredit_phones_order_base_cube_w
          where dt='{dt}'
          and terms=6
          
          ) a
          group by dateweek_of_entry
          order by dateweek_of_entry 





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
                                <th colspan="7" style="text-align: center;">整体情况</th>
                                <th colspan="7" style="text-align: center;">3期产品</th>
                                <th colspan="7" style="text-align: center;">6期产品</th>
                            </tr>
                            <tr>
                            <!--分类-->
                            <th>进件月份</th>
                            <!--整体情况-->
                            <th>进件量</th>
                            <th>初审通过量</th>
                            <th>终审通过量</th>
                            <th>放款量</th>
                            <th>初审通过率</th>
                            <th>复审通过率</th>
                            <th>风控整体通过率</th>
                            <!--3期产品-->
                            <th>进件量</th>
                            <th>初审通过量</th>
                            <th>终审通过量</th>
                            <th>放款量</th>
                            <th>初审通过率</th>
                            <th>复审通过率</th>
                            <th>风控整体通过率</th>
                            <!--6期产品-->
                            <th>进件量</th>
                            <th>初审通过量</th>
                            <th>终审通过量</th>
                            <th>放款量</th>
                            <th>初审通过率</th>
                            <th>复审通过率</th>
                            <th>风控整体通过率</th>
                        </tr>
    '''

    week_fmt = '''
                               <tr>
                                <th colspan="1" style="text-align: center;">分类</th>
                                <th colspan="7" style="text-align: center;">整体情况</th>
                                <th colspan="7" style="text-align: center;">3期产品</th>
                                <th colspan="7" style="text-align: center;">6期产品</th>
                            </tr>
                            <tr>
                            <!--分类-->
                            <th>进件周</th>
                            <!--整体情况-->
                            <th>进件量</th>
                            <th>初审通过量</th>
                            <th>终审通过量</th>
                            <th>放款量</th>
                            <th>初审通过率</th>
                            <th>复审通过率</th>
                            <th>风控整体通过率</th>
                            <!--3期产品-->
                            <th>进件量</th>
                            <th>初审通过量</th>
                            <th>终审通过量</th>
                            <th>放款量</th>
                            <th>初审通过率</th>
                            <th>复审通过率</th>
                            <th>风控整体通过率</th>
                            <!--6期产品-->
                            <th>进件量</th>
                            <th>初审通过量</th>
                            <th>终审通过量</th>
                            <th>放款量</th>
                            <th>初审通过率</th>
                            <th>复审通过率</th>
                            <th>风控整体通过率</th>
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
                <table width="100%" class="table">
                    <caption>
                        <h3>当月风控全流程通过情况</h3>
                    </caption>
                </table>
                <table width="100%" class="table">
                    <thead>
                       <tr>
                                <th colspan="1" style="text-align: center;">分类</th>
                                <th colspan="7" style="text-align: center;">整体情况</th>
                                <th colspan="7" style="text-align: center;">3期产品</th>
                                <th colspan="7" style="text-align: center;">6期产品</th>
                            </tr>
                            <tr>
                            <!--分类-->
                            <th>进件日期</th>
                            <!--整体情况-->
                            <th>进件量</th>
                            <th>初审通过量</th>
                            <th>终审通过量</th>
                            <th>放款量</th>
                            <th>初审通过率</th>
                            <th>复审通过率</th>
                            <th>风控整体通过率</th>
                            <!--3期产品-->
                            <th>进件量</th>
                            <th>初审通过量</th>
                            <th>终审通过量</th>
                            <th>放款量</th>
                            <th>初审通过率</th>
                            <th>复审通过率</th>
                            <th>风控整体通过率</th>
                            <!--6期产品-->
                            <th>进件量</th>
                            <th>初审通过量</th>
                            <th>终审通过量</th>
                            <th>放款量</th>
                            <th>初审通过率</th>
                            <th>复审通过率</th>
                            <th>风控整体通过率</th>
                        </tr>
                    </thead>
                    <tbody>
                    {rows}
                    </tbody>
                </table>
                <table width="100%" class="table">
                <caption>
                    <h3>历史各月风控全流程通过情况</h3>
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
                    <h3>近4周风控全流程通过情况</h3>
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
                           show_month=get_show_month(ds)
                           )  #

    logging.info("==============")
    # logging.info(html)

    # email_to = Variable.get("oride_global_operate_report_receivers").split()
    #email_to = Variable.get("ocredit_phones_global_operate_report_receivers").split()
    email_to = Variable.get("ocredit_phones_whole_process_report_receivers").split()
    # email_to = ['bigdata@opay-inc.com','lili.chen@opay-inc.com']

    # email_to = ['lili.chen@opay-inc.com']
    #email_to = ['shuai01.li@opay-inc.com','jiaying.kang@opay-inc.com']
    # email_to = ['jiaying.kang@opay-inc.com']
    # send mail

    email_subject = 'ocredit_phones全流程日报_{}'.format(ds)
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


