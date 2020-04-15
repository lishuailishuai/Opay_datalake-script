# coding: utf-8
import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from utils.connection_helper import get_hive_cursor
from airflow.utils.email import send_email
import xlwt
import logging
import codecs
from airflow.models import Variable
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from utils.validate_metrics_utils import *
from airflow.sensors import OssSensor

args = {
    'owner': 'dong.xie',
    'start_date': datetime(2020, 4, 14),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'otrade_b2c_merchant_goods_23h_email',
    schedule_interval="40 21 * * *",
    default_args=args)

table_names = ['otrade_dw.dwm_otrade_b2c_order_goods_collect_hi']

'''
校验分区代码
'''
dwm_otrade_b2c_order_goods_collect_hi_check_task = OssSensor(
    task_id='dwm_otrade_b2c_order_goods_collect_hi_check_task',
    bucket_key="{hdfs_path_str}/dt={pt}/hour=22/_SUCCESS".format(
        hdfs_path_str="otrade/otrade_dw/dwm_otrade_b2c_order_goods_collect_hi/country_code=NG",
        pt="{{macros.ds_add(ds, +1)}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

def send_b2c_merchant_goods_email(ds, **kwargs):
    cursor = get_hive_cursor()
    sql = '''
        with dwm_otrade_b2c_order_goods_collect_22h_data as (
            select 
                merchant_id,
                merchant_name,
                country,
                country_name,
                city,
                city_name,
                one_level_id,
                one_level_name,
                two_level_id,
                two_level_name,
                three_level_id,
                three_level_name,
                goods_id,
                goods_name,
                product_id,
                product_name,
                retail_price,
                number,
                add_time,
                opayid,
                first_order,
                pay_time,
                row_number() over(partition by id order by utc_date_hour desc) rn
            from otrade_dw.dwm_otrade_b2c_order_goods_collect_hi
            where country_code='NG' and concat(dt,' ',hour) between '{pt} 00' and '{pt} 22'   
        
        )
        select
            merchant_id,
            merchant_name,
            country,
            country_name,
            city,
            city_name,
            one_level_id,
            one_level_name,
            two_level_id,
            two_level_name,
            three_level_id,
            three_level_name,
            goods_id,
            goods_name,
            product_id,
            product_name,
              
            --下单分析
            sum(if(substr(add_time,0,10)='{pt}',nvl(retail_price*number,0),0)) as order_amt,
            sum(if(substr(add_time,0,10)='{pt}',nvl(number,0),0)) as order_goods_amt,
            count(if(substr(add_time,0,10)='{pt}',1,null)) as order_cnt,
            count(distinct(if(substr(add_time,0,10)='{pt}',opayid,null))) as order_people,
            sum(if(first_order=1 and substr(add_time,0,10)='{pt}',nvl(retail_price*number,0),0)) as first_order_amt,
            count(if(first_order=1 and substr(add_time,0,10)='{pt}',1,null)) as first_order_cnt,
            count(distinct(if(first_order=1 and substr(add_time,0,10)='{pt}',opayid,null))) as first_order_people,
              
            --销售分析
            sum(if(substr(pay_time,0,10)='{pt}',nvl(retail_price*number,0),0)) as pay_amt,
            count(if(substr(pay_time,0,10)='{pt}',1,null)) as pay_suc_cnt,
            sum(if(substr(pay_time,0,10)='{pt}',nvl(number,0),0)) as pay_goods_cnt,
              
            --用户分析
            count(distinct(if(substr(pay_time,0,10)='{pt}',opayid,null))) as pay_user_cnt,
            'NG' as country_code
        from dwm_otrade_b2c_order_goods_collect_22h_data 
        where rn = 1
        group by
            merchant_id,
            merchant_name,
            country,
            country_name,
            city,
            city_name,
            one_level_id,
            one_level_name,
            two_level_id,
            two_level_name,
            three_level_id,
            three_level_name,
            goods_id,
            goods_name,
            product_id,
            product_name

        '''.format(pt=airflow.macros.ds_add(ds, +1))

    logging.info('Executing: %s', sql)
    cursor.execute(sql)
    rows1 = cursor.fetchall()


    file_name1 = '/tmp/b2c_merchant_goods_{dt}.xls'.format(dt=airflow.macros.ds_add(ds, +1))

    # 生成excel文件
    book1 = xlwt.Workbook()
    merchant_goods = book1.add_sheet('merchant_goods', cell_overwrite_ok=True)


    # 表头标题
    merchant_goods.write(0, 0, '店铺id'),
    merchant_goods.write(0, 1, '店铺名称'),
    merchant_goods.write(0, 2, '国家'),
    merchant_goods.write(0, 3, '国家名称'),
    merchant_goods.write(0, 4, '城市'),
    merchant_goods.write(0, 5, '城市名称'),
    merchant_goods.write(0, 6, '一级品类id'),
    merchant_goods.write(0, 7, '一级品类名称'),
    merchant_goods.write(0, 8, '二级品类id'),
    merchant_goods.write(0, 9, '二级品类名称'),
    merchant_goods.write(0, 10, '三级品类id'),
    merchant_goods.write(0, 11, '三级品类名称'),
    merchant_goods.write(0, 12, '商品id(spu)'),
    merchant_goods.write(0, 13, '商品名称(spu名称)'),
    merchant_goods.write(0, 14, '产品Id(sku)'),
    merchant_goods.write(0, 15, 'sku商品名称'),
    merchant_goods.write(0, 16, '下单金额(应付金额)'),
    merchant_goods.write(0, 17, '下单数量(商品数量)'),
    merchant_goods.write(0, 18, '下单订单量'),
    merchant_goods.write(0, 19, '下单用户数'),
    merchant_goods.write(0, 20, '首单下单金额(应付金额)'),
    merchant_goods.write(0, 21, '首单下单数量'),
    merchant_goods.write(0, 22, '首单下单用户数'),
    merchant_goods.write(0, 23, '销售金额'),
    merchant_goods.write(0, 24, '销售单量(支付成功订单数量)'),
    merchant_goods.write(0, 25, '销售数量(成功支付订单的各订单内商品数量之和)'),
    merchant_goods.write(0, 26, '支付用户数'),
    merchant_goods.write(0, 27, '国家编码')

    # 每一列写入excel文件，不然数据会全在一个单元格中
    for i in range(len(rows1)):
        for j in range(28):
            # print (rows[i][j])-
            # print ("--------")
            merchant_goods.write(i + 1, j, rows1[i][j])

    book1.save(file_name1)
    
    cursor.close()

    # send mail
    # email_to = Variable.get("otrade_b2c_merchant_goods_23h_email_list").split()
    email_to = ['dong.xie@opay-inc.com', 'meiyuan.zhang@opay-inc.com']
    result = is_alert(airflow.macros.ds_add(ds, +1), table_names)
    # if result:
    #     email_to = ['bigdata@opay-inc.com']

    email_subject = 'B2C业务数据（商品维度数据）邮件报表 Lagos.{dt} 00点 至 22点'.format(dt=airflow.macros.ds_add(ds, +1))
    email_body = 'Lagos.{dt} 00点 至 22点 B2C业务数据（商品维度数据）邮件报表 见附件, 请查收'.format(dt=airflow.macros.ds_add(ds, +1))
    send_email(email_to, email_subject, email_body, [file_name1], mime_charset='utf-8')


otrade_b2c_merchant_goods_23h_email = PythonOperator(
    task_id='otrade_b2c_merchant_goods_23h_email',
    python_callable=send_b2c_merchant_goods_email,
    provide_context=True,
    dag=dag
)

dwm_otrade_b2c_order_goods_collect_hi_check_task >> otrade_b2c_merchant_goods_23h_email