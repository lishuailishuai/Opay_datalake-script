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
    'start_date': datetime(2020, 3, 10),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    'ocredit_phones_overdue_report',
    schedule_interval="50 01 * * *",
    default_args=args)

##----------------------------------------- 依赖 ---------------------------------------##

app_ocredit_phones_overdue_cube_d_task = OssSensor(
    task_id='app_ocredit_phones_overdue_cube_d_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones/ocredit_phones_dw/app_ocredit_phones_overdue_cube_d/country_code=nal",

        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


def get_show_existing(ds):
    tr_fmt = '''
            <tr>
            各位同事：<br/>
            附件为截止到昨天的手机分期逾期日报情况，主要内容如下：<br/>
            1、现存逾期情况：截止当前累计应收放款量{loan_cnt_expire}笔，现存逾期合同{existing_overdue_cnt}笔，CPD0+%为{CPD0}，CPD7+%为{CPD7}，CPD15+%为{CPD15};
            </tr>
            '''

    sql = '''
          
          select
          sum(loan_cnt_expire),
          sum(existing_overdue_cnt),
          concat(round(sum(existing_overdue_cnt)/sum(existing_expire0_cnt)*100,2),'%'),
          concat(round(sum(existing_overdue7_cnt)/sum(existing_expire7_cnt)*100,2),'%'),
          concat(round(sum(existing_overdue15_cnt)/sum(existing_expire15_cnt)*100,2),'%')
          from 
          ocredit_phones_dw.app_ocredit_phones_overdue_cube_d
          where dt ='{dt}'
          and loan_time is not null
          --and loan_time<=date_add(trunc('{dt}','MM'),-1)
          and loan_time<='{dt}'
 
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
            loan_cnt_expire = data[0]
            existing_overdue_cnt = data[1]
            CPD0 = data[2]
            CPD7 = data[3]
            CPD15 = data[4]
            row_html += tr_fmt.format(
                dt=ds,
                loan_cnt_expire=loan_cnt_expire,
                existing_overdue_cnt=existing_overdue_cnt,
                CPD0=CPD0,
                CPD7=CPD7,
                CPD15=CPD15

            )

    return row_html


def get_show_downpay(ds):
    tr_fmt = '''
               <tr>
            2、首期逾期情况：截止当前累计应收放款量{loan_cnt_expire}笔，首期逾期合同{downpay_overdue_cnt}笔，FPD0+%为{FPD0}，FPD7+%为{FPD7}，FPD15+%为{FPD15}；<br/>
            以上敬请参考，如有疑问，随时沟通！<br/>
            备注：CPD是指现存逾期，CPDx+%=现存逾期x+合同/放款到期x+合同；FPD是指首期逾期，FPDx+%=首期逾期x+合同/放款到期x+合同
            </tr>
            '''

    sql = '''
          select
          sum(loan_cnt_expire),
          sum(downpay_overdue_cnt),
          concat(round(sum(downpay_overdue_cnt)/sum(downpay_expire0_cnt)*100,2),'%'),
          concat(round(sum(downpay_overdue7_cnt)/sum(downpay_expire7_cnt)*100,2),'%'),
          concat(round(sum(downpay_overdue15_cnt)/sum(downpay_expire15_cnt)*100,2),'%')
          from 
          ocredit_phones_dw.app_ocredit_phones_overdue_cube_d
          where dt ='{dt}'
          and loan_time is not null
          --and loan_time<=date_add(trunc('{dt}','MM'),-1)
          and loan_time<='{dt}'

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
            loan_cnt_expire = data[0]
            downpay_overdue_cnt = data[1]
            FPD0 = data[2]
            FPD7 = data[3]
            FPD15 = data[4]


            row_html += tr_fmt.format(

                dt=ds,
                loan_cnt_expire=loan_cnt_expire,
                downpay_overdue_cnt=downpay_overdue_cnt,
                FPD0=FPD0,
                FPD7=FPD7,
                FPD15=FPD15

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
                            <!--放款规模-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <!--现存逾期率（CPDX+%）-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <!--首期逾期率(FPDX+%)-->
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
          loan_time,--放款日期
          format_number(loan_amount_usd_expire,0),--贷款金额USD
          loan_cnt_expire,--放款件数
          existing_overdue_cnt,--CPD0+件数
          nvl(concat(round(existing_overdue_cnt/existing_expire0_cnt*100,2),'%'),''),--CPD0+%
          nvl(concat(round(existing_overdue3_cnt/existing_expire3_cnt*100,2),'%'),''),--CPD3+%
          nvl(concat(round(existing_overdue5_cnt/existing_expire5_cnt*100,2),'%'),''),--CPD5+%
          nvl(concat(round(existing_overdue7_cnt/existing_expire7_cnt*100,2),'%'),''),--CPD7+%
          nvl(concat(round(existing_overdue15_cnt/existing_expire15_cnt*100,2),'%'),''),--CPD15+%
          nvl(concat(round(existing_overdue30_cnt/existing_expire30_cnt*100,2),'%'),''),--CPD30+%
          downpay_overdue_cnt,--FPD0+件数
          nvl(concat(round(downpay_overdue_cnt/downpay_expire0_cnt*100,2),'%'),''),--FPD0+%
          nvl(concat(round(downpay_overdue3_cnt/downpay_expire3_cnt*100,2),'%'),''),--FPD0+%
          nvl(concat(round(downpay_overdue5_cnt/downpay_expire5_cnt*100,2),'%'),''),--FPD0+%
          nvl(concat(round(downpay_overdue7_cnt/downpay_expire7_cnt*100,2),'%'),''),--FPD0+%
          nvl(concat(round(downpay_overdue15_cnt/downpay_expire15_cnt*100,2),'%'),''),--FPD0+%
          nvl(concat(round(downpay_overdue30_cnt/downpay_expire30_cnt*100,2),'%'),'')--FPD0+%
          from
          ocredit_phones_dw.app_ocredit_phones_overdue_cube_d
          where dt='{dt}'
          and loan_time is not null
          and 
          substr(loan_time,1,7) = substr(ADD_MONTHS('{dt}',-1),1,7)
          order by loan_time desc

         
           



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
                            <!--放款规模-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <!--现存逾期率（CPDX+%）-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <!--首期逾期率(FPDX+%)-->
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
          loan_month,--放款月
          format_number(loan_amount_usd_expire,0),--贷款金额USD
          loan_cnt_expire,--放款件数
          existing_overdue_cnt,--CPD0+件数
          nvl(concat(round(existing_overdue_cnt/existing_expire0_cnt*100,2),'%'),''),--CPD0+%
          nvl(concat(round(existing_overdue3_cnt/existing_expire3_cnt*100,2),'%'),''),--CPD3+%
          nvl(concat(round(existing_overdue5_cnt/existing_expire5_cnt*100,2),'%'),''),--CPD5+%
          nvl(concat(round(existing_overdue7_cnt/existing_expire7_cnt*100,2),'%'),''),--CPD7+%
          nvl(concat(round(existing_overdue15_cnt/existing_expire15_cnt*100,2),'%'),''),--CPD15+%
          nvl(concat(round(existing_overdue30_cnt/existing_expire30_cnt*100,2),'%'),''),--CPD30+%
          downpay_overdue_cnt,--FPD0+件数
          nvl(concat(round(downpay_overdue_cnt/downpay_expire0_cnt*100,2),'%'),''),--FPD0+%
          nvl(concat(round(downpay_overdue3_cnt/downpay_expire3_cnt*100,2),'%'),''),--FPD0+%
          nvl(concat(round(downpay_overdue5_cnt/downpay_expire5_cnt*100,2),'%'),''),--FPD0+%
          nvl(concat(round(downpay_overdue7_cnt/downpay_expire7_cnt*100,2),'%'),''),--FPD0+%
          nvl(concat(round(downpay_overdue15_cnt/downpay_expire15_cnt*100,2),'%'),''),--FPD0+%
          nvl(concat(round(downpay_overdue30_cnt/downpay_expire30_cnt*100,2),'%'),'')--FPD0+%
          from
          ocredit_phones_dw.app_ocredit_phones_overdue_cube_d
          where dt='{dt}'
          and loan_month is not null
          order by loan_month desc

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
                           <!--放款规模-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <!--现存逾期率（CPDX+%）-->
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <!--首期逾期率(FPDX+%)-->
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
          --concat_ws('-',regexp_replace(substr(loan_week,6,5),'-',''),regexp_replace(substr(loan_week,17,5),'-','')) aa,--放款周
          loan_week,
          format_number(loan_amount_usd_expire,0),--贷款金额USD
          loan_cnt_expire,--放款件数
          existing_overdue_cnt,--CPD0+件数
          nvl(concat(round(existing_overdue_cnt/existing_expire0_cnt*100,2),'%'),''),--CPD0+%
          nvl(concat(round(existing_overdue3_cnt/existing_expire3_cnt*100,2),'%'),''),--CPD3+%
          nvl(concat(round(existing_overdue5_cnt/existing_expire5_cnt*100,2),'%'),''),--CPD5+%
          nvl(concat(round(existing_overdue7_cnt/existing_expire7_cnt*100,2),'%'),''),--CPD7+%
          nvl(concat(round(existing_overdue15_cnt/existing_expire15_cnt*100,2),'%'),''),--CPD15+%
          nvl(concat(round(existing_overdue30_cnt/existing_expire30_cnt*100,2),'%'),''),--CPD30+%
          downpay_overdue_cnt,--FPD0+件数
          nvl(concat(round(downpay_overdue_cnt/downpay_expire0_cnt*100,2),'%'),''),--FPD0+%
          nvl(concat(round(downpay_overdue3_cnt/downpay_expire3_cnt*100,2),'%'),''),--FPD0+%
          nvl(concat(round(downpay_overdue5_cnt/downpay_expire5_cnt*100,2),'%'),''),--FPD0+%
          nvl(concat(round(downpay_overdue7_cnt/downpay_expire7_cnt*100,2),'%'),''),--FPD0+%
          nvl(concat(round(downpay_overdue15_cnt/downpay_expire15_cnt*100,2),'%'),''),--FPD0+%
          nvl(concat(round(downpay_overdue30_cnt/downpay_expire30_cnt*100,2),'%'),'')--FPD0+%
          from
          ocredit_phones_dw.app_ocredit_phones_overdue_cube_d
          where dt=date_add(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),-1)
          and loan_week is not null
          order by loan_week desc

              

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
                                <th colspan="3" style="text-align: center;">放款规模</th>
                                <th colspan="7" style="text-align: center;">现存逾期率（CPDX+%）</th>
                                <th colspan="7" style="text-align: center;">首期逾期率(FPDX+%)</th>
                            </tr>
                            <tr>
                            <!--放款规模-->
                            <th>放款日期</th>
                            <th>贷款美金</th>
                            <th>放款笔数</th>
                            <!--现存逾期率（CPDX+%）-->
                            <th>CPD0+件数</th>
                            <th>CPD0+%</th>
                            <th>CPD3+%</th>
                            <th>CPD5+%</th>
                            <th>CPD7+%</th>
                            <th>CPD15+%</th>
                            <th>CPD30+%</th>
                            <!--首期逾期率(FPDX+%)-->
                           <th>FPD0+件数</th>
                           <th>FPD0+%</th>
                           <th>FPD3+%</th>
                           <th>FPD5+%</th>
                           <th>FPD7+%</th>
                           <th>FPD15+%</th>
                           <th>FPD30+%</th>
                           
                            
                        </tr>
    '''

    week_fmt = '''
                                <tr>
                                <th colspan="3" style="text-align: center;">放款规模</th>
                                <th colspan="7" style="text-align: center;">现存逾期率（CPDX+%）</th>
                                <th colspan="7" style="text-align: center;">首期逾期率(FPDX+%)</th>
                            </tr>
                            <tr>
                            <!--放款规模-->
                            <th>放款日期</th>
                            <th>贷款美金</th>
                            <th>放款笔数</th>
                            <!--现存逾期率（CPDX+%）-->
                            <th>CPD0+件数</th>
                            <th>CPD0+%</th>
                            <th>CPD3+%</th>
                            <th>CPD5+%</th>
                            <th>CPD7+%</th>
                            <th>CPD15+%</th>
                            <th>CPD30+%</th>
                            <!--首期逾期率(FPDX+%)-->
                           <th>FPD0+件数</th>
                           <th>FPD0+%</th>
                           <th>FPD3+%</th>
                           <th>FPD5+%</th>
                           <th>FPD7+%</th>
                           <th>FPD15+%</th>
                           <th>FPD30+%</th>
                           
                            
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

            {show_existing}
            {show_downpay}
                <table width="100%" class="table">
                    <caption>
                        <h3>当月每日到期逾期情况</h3>
                    </caption>
                </table>
                <table width="100%" class="table">
                    <thead>
                        <tr>
                                <th colspan="3" style="text-align: center;">放款规模</th>
                                <th colspan="7" style="text-align: center;">现存逾期率（CPDX+%）</th>
                                <th colspan="7" style="text-align: center;">首期逾期率(FPDX+%)</th>
                            </tr>
                            <tr>
                            <!--放款规模-->
                            <th>放款日期</th>
                            <th>贷款美金</th>
                            <th>放款笔数</th>
                            <!--现存逾期率（CPDX+%）-->
                            <th>CPD0+件数</th>
                            <th>CPD0+%</th>
                            <th>CPD3+%</th>
                            <th>CPD5+%</th>
                            <th>CPD7+%</th>
                            <th>CPD15+%</th>
                            <th>CPD30+%</th>
                            <!--首期逾期率(FPDX+%)-->
                           <th>FPD0+件数</th>
                           <th>FPD0+%</th>
                           <th>FPD3+%</th>
                           <th>FPD5+%</th>
                           <th>FPD7+%</th>
                           <th>FPD15+%</th>
                           <th>FPD30+%</th>
                           
                            
                        </tr>
                    </thead>
                    <tbody>
                    {rows}
                    </tbody>
                </table>
                <table width="100%" class="table">
                <caption>
                    <h3>历史各月到期逾期情况</h3>
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
                    <h3>历史各周到期逾期情况</h3>
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
                           show_existing=get_show_existing(ds),
                           show_downpay=get_show_downpay(ds)
                           )  #

    logging.info("==============")
    # logging.info(html)

    # email_to = Variable.get("oride_global_operate_report_receivers").split()
    #email_to = Variable.get("ocredit_phones_global_operate_report_receivers").split()

    # email_to = ['bigdata@opay-inc.com','lili.chen@opay-inc.com']

    # email_to = ['lili.chen@opay-inc.com']
    #email_to = ['shuai01.li@opay-inc.com','jiaying.kang@opay-inc.com']
    email_to = Variable.get("ocredit_phones_overdue_report_receivers").split()
    # email_to = ['jiaying.kang@opay-inc.com']
    # send mail

    email_subject = 'ocredit_phones逾期日报_{}'.format(ds)
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

app_ocredit_phones_overdue_cube_d_task >> send_report


