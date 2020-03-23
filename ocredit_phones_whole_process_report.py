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
app_ocredit_phones_risk_control_cube_d_task = OssSensor(
    task_id='app_ocredit_phones_risk_control_cube_d_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones/ocredit_phones_dw/app_ocredit_phones_risk_control_cube_d/country_code=nal",

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
            附件为截止到昨日{dt}手机分期全流程日报情况，主要内容如下：<br/>
            1、当日情况：进件{entry_opay_cnt}笔，初审通过率为{one_rate}，复审通过率为{two_rate}，风控通过率为{three_rate}；进件到放款总时效为{total_use_average_hour}时；
            </tr>
            '''

    sql = '''
          
          select entry_opay_cnt,concat(round(pre_amount/entry_opay_cnt*100,2),'%'),
          concat(round(review_amount/pre_amount*100,2),'%'),
          concat(round(review_amount/entry_opay_cnt*100,2),'%'),
          round(total_use_average_hour,2)
          from
          ocredit_phones_dw.app_ocredit_phones_risk_control_cube_d 
          where date_of_entry is not null
          and dt='{dt}'
          and date_of_entry='{dt}'
          
          
          
          
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
            entry_opay_cnt = data[0]
            one_rate = data[1]
            two_rate = data[2]
            three_rate = data[3]
            total_use_average_hour = data[4]


            row_html += tr_fmt.format(
                dt=ds,
                entry_opay_cnt=entry_opay_cnt,
                one_rate=one_rate,
                two_rate=two_rate,
                three_rate = three_rate,
                total_use_average_hour = total_use_average_hour

            )

    return row_html





def get_show_month(ds):
    tr_fmt = '''
            
            <tr>
            2、本月情况：进件{entry_opay_cnt}笔，初审通过率为{one_rate}，复审通过率为{two_rate}，风控通过率为{three_rate}；进件到放款总时效为{total_use_average_hour}时；
            </tr>
            '''

    sql = '''
         
          select entry_opay_cnt,concat(round(pre_amount/entry_opay_cnt*100,2),'%'),
          concat(round(review_amount/pre_amount*100,2),'%'),
          concat(round(review_amount/entry_opay_cnt*100,2),'%'),
          round(total_use_average_hour,2)
          from
          ocredit_phones_dw.app_ocredit_phones_risk_control_cube_d 
          where month_of_entry is not null
          and dt='{dt}'
          and month_of_entry=substr('{dt}',1,7)

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
            entry_opay_cnt = data[0]
            one_rate = data[1]
            two_rate = data[2]
            three_rate = data[3]
            total_use_average_hour = data[4]

            row_html += tr_fmt.format(

                dt=ds,
                entry_opay_cnt=entry_opay_cnt,
                one_rate=one_rate,
                two_rate=two_rate,
                three_rate=three_rate,
                total_use_average_hour=total_use_average_hour
            )

    return row_html


def get_show_all(ds):
    tr_fmt = '''
            <tr>
            3、累计情况：进件{entry_opay_cnt}笔，初审通过率为{one_rate}，复审通过率为{two_rate}，风控通过率为{three_rate}；进件到放款总时效为{total_use_average_hour}时；<br/>
            备注1：初审通过率=初审通过的/进件量，复审通过率=复审通过的/初审通过的，风控整体通过率=复审通过的/进件量<br/>
            以上敬请参考，如有疑问，随时沟通！
            </tr>
            '''

    sql = '''
                
          select sum(entry_opay_cnt),concat(round(sum(pre_amount)/sum(entry_opay_cnt)*100,2),'%'),
          concat(round(sum(review_amount)/sum(pre_amount)*100,2),'%'),
          concat(round(sum(review_amount)/sum(entry_opay_cnt)*100,2),'%'),
          round(sum(total_use_average_hour),2)
          from
          ocredit_phones_dw.app_ocredit_phones_risk_control_cube_d 
          where month_of_entry is not null
          and dt='{dt}'
          


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
            entry_opay_cnt = data[0]
            one_rate = data[1]
            two_rate = data[2]
            three_rate = data[3]
            total_use_average_hour = data[4]

            row_html += tr_fmt.format(

                dt=ds,
                entry_opay_cnt=entry_opay_cnt,
                one_rate=one_rate,
                two_rate=two_rate,
                three_rate=three_rate,
                total_use_average_hour=total_use_average_hour
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
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                                                 
                    '''
    sql = '''

          select
          date_of_entry,
          entry_order_cnt,
          entry_opay_cnt,
          concat(round(pre_amount/entry_opay_cnt*100,2),'%'),
          concat(round(review_amount/pre_amount*100,2),'%'),
          
          concat(round(loan_cnt/review_amount*100,2),'%'),
          concat(round(review_amount/entry_opay_cnt*100,2),'%'),
          concat(round(loan_cnt/entry_opay_cnt*100,2),'%'),
          
          round(pre_actual_average_minute,2),
          round(review_actual_average_minute,2),
          round(pay_actual_average_minute,2),
          round(contract_review_average_minute,2),
          round(total_use_average_hour,2)
          
          from
          ocredit_phones_dw.app_ocredit_phones_risk_control_cube_d 
          where date_of_entry is not null
          and dt='{dt}'
          and substr(date_of_entry,1,7) = substr('{dt}',1,7)
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
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                    '''
    sql = '''


              select
          month_of_entry,
          entry_order_cnt,
          entry_opay_cnt,
          concat(round(pre_amount/entry_opay_cnt*100,2),'%'),
          concat(round(review_amount/pre_amount*100,2),'%'),
          
          concat(round(loan_cnt/review_amount*100,2),'%'),
          concat(round(review_amount/entry_opay_cnt*100,2),'%'),
          concat(round(loan_cnt/entry_opay_cnt*100,2),'%'),
          
          round(pre_actual_average_minute,2),
          round(review_actual_average_minute,2),
          round(pay_actual_average_minute,2),
          round(contract_review_average_minute,2),
          round(total_use_average_hour,2)
          
          from
          ocredit_phones_dw.app_ocredit_phones_risk_control_cube_d 
          where month_of_entry is not null
          and dt='{dt}'
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
          week_of_entry,
          entry_order_cnt,
          entry_opay_cnt,
          concat(round(pre_amount/entry_opay_cnt*100,2),'%'),
          concat(round(review_amount/pre_amount*100,2),'%'),
          
          concat(round(loan_cnt/review_amount*100,2),'%'),
          concat(round(review_amount/entry_opay_cnt*100,2),'%'),
          concat(round(loan_cnt/entry_opay_cnt*100,2),'%'),
          
          round(pre_actual_average_minute,2),
          round(review_actual_average_minute,2),
          round(pay_actual_average_minute,2),
          round(contract_review_average_minute,2),
          round(total_use_average_hour,2)
          
          from
          ocredit_phones_dw.app_ocredit_phones_risk_control_cube_d 
          where week_of_entry is not null
          and dt='{dt}'
          order by week_of_entry desc
          

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
                            <!--分类-->
                            <th>进件日期</th>
                            <!--整体情况-->
                            <th>进件订单数</th>
                            <th>进件用户数</th>
                            <th>初审通过率</th>
                            <th>复审通过率</th>
                            <th>复审到放款转化率</th>
                            <th>进件到风控通过转化率</th>
                            <th>进件到放款转化率</th>
                            <th>初审实际用时（分）</th>
                            <th>复审实际用时（分）</th>
                            <th>支付实际用时（分）</th>
                            <th>合同审核用时（分）</th>
                            <th>总用时（时）</th>
                        </tr>
    '''

    week_fmt = '''
                     <tr>
                            <!--分类-->
                            <th>进件日期</th>
                            <!--整体情况-->
                            <th>进件订单数</th>
                            <th>进件用户数</th>
                            <th>初审通过率</th>
                            <th>复审通过率</th>
                            <th>复审到放款转化率</th>
                            <th>进件到风控通过转化率</th>
                            <th>进件到放款转化率</th>
                            <th>初审实际用时（分）</th>
                            <th>复审实际用时（分）</th>
                            <th>支付实际用时（分）</th>
                            <th>合同审核用时（分）</th>
                            <th>总用时（时）</th>
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
                        <h3>当月每日风控全流程通过情况</h3>
                    </caption>
                </table>
                <table width="100%" class="table">
                    <thead>
                       <tr>
                            <!--分类-->
                            <th>进件日期</th>
                            <!--整体情况-->
                            <th>进件订单数</th>
                            <th>进件用户数</th>
                            <th>初审通过率</th>
                            <th>复审通过率</th>
                            <th>复审到放款转化率</th>
                            <th>进件到风控通过转化率</th>
                            <th>进件到放款转化率</th>
                            <th>初审实际用时（分）</th>
                            <th>复审实际用时（分）</th>
                            <th>支付实际用时（分）</th>
                            <th>合同审核用时（分）</th>
                            <th>总用时（时）</th>
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
                    <h3>历史每周风控全流程通过情况</h3>
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

    # email_to = Variable.get("oride_global_operate_report_receivers").split()
    #email_to = Variable.get("ocredit_phones_global_operate_report_receivers").split()
    email_to = Variable.get("ocredit_phones_whole_process_report_receivers").split()
    # email_to = ['bigdata@opay-inc.com','lili.chen@opay-inc.com']

    # email_to = ['lili.chen@opay-inc.com']
    #email_to = ['shuai01.li@opay-inc.com','jiaying.kang@opay-inc.com']
    #email_to = ['shuai01.li@opay-inc.com']
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

app_ocredit_phones_risk_control_cube_d_task >> send_report

