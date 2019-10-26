from datetime import datetime, timedelta
import airflow
import logging
from utils.connection_helper import get_hive_cursor
from airflow.utils.email import send_email
from plugins.comwx import ComwxApi

comwx = ComwxApi('wwd26d45f97ea74ad2', 'BLE_v25zCmnZaFUgum93j3zVBDK-DjtRkLisI_Wns4g', '1000011')



now = datetime.today()

# 预警阀值
ods_data_alert_limit = 0.1

# 停止阀值
ods_data_stop_limit = 0.45

"""
    校验全局数据量函数
"""


def validata_data(db, table_name, table_format, table_core_list, table_not_core_list, ds, **kwargs):
    cursor = get_hive_cursor()
    day = ds
    day_before_1 = airflow.macros.ds_add(ds, -1)
    day_before_7 = airflow.macros.ds_add(ds, -7)

    flag = True
    time_column = ''

    for db_name, validate_table_name, conn_id, prefix_name, column_name in table_core_list:
        if table_name == table_format % (prefix_name, validate_table_name):
            time_column = column_name
            flag = False
            break

    for db_name, validate_table_name, conn_id, prefix_name, column_name in table_not_core_list:
        if table_name == table_format % (prefix_name, validate_table_name):
            time_column = column_name
            flag = False
            break

    if flag:
        write_meta_data(table_name, day, 1, '此表不在校验列表中，导入成功')
        return

    # 第一次验证
    sql = '''
        select 
        from_unixtime({time_column},'yyyy-MM-dd') order_day,
        count(1)
        from 
        {db}.{table_name}
        where dt = '{day}'
        and (from_unixtime({time_column},'yyyy-MM-dd') = '{day}' or from_unixtime({time_column},'yyyy-MM-dd') = '{day_before_7}')
        group by from_unixtime({time_column},'yyyy-MM-dd')
        order by order_day desc
    '''.format(
        db=db,
        table_name=table_name,
        time_column=time_column,
        day=day,
        day_before_1=day_before_1,
        day_before_7=day_before_7
    )

    t = validate(cursor, table_name, sql, day, day_before_1, day_before_7)

    if not t[0]:
        write_meta_data(table_name, day, t[0], t[1])

        comwx.postAppMessage(t[1], '271')
        # send error mail
        email_subject = '数据导入预警_{table_name}_{day}'.format(
            table_name=table_name,
            day=day)
        send_email(
            'bigdata_dw@opay-inc.com'
            , email_subject, t[1], mime_charset='utf-8')
        return

    sql = '''
        select 
        t.order_day,
        t.counts
        from 
        (
        select 
                from_unixtime({time_column},'yyyy-MM-dd') order_day,
                count(1) counts
                from 
                {db}.{table_name}
                where dt = '{day}' and from_unixtime({time_column},'yyyy-MM-dd') = '{day}'
                group by from_unixtime({time_column},'yyyy-MM-dd')
                union all 
                select 
                from_unixtime({time_column},'yyyy-MM-dd') order_day,
                count(1) counts
                from 
                {db}.{table_name}
                where dt = '{day_before_1}' and from_unixtime({time_column},'yyyy-MM-dd') = '{day_before_7}'
                group by from_unixtime({time_column},'yyyy-MM-dd')
        ) t
        order by t.order_day desc
    '''.format(
        db=db,
        table_name=table_name,
        time_column=time_column,
        day=day,
        day_before_1=day_before_1,
        day_before_7=day_before_7
    )

    t = validate(cursor, table_name, sql, day, day_before_1, day_before_7)

    if not t[0]:
        write_meta_data(table_name, day, t[0], t[1])

        comwx.postAppMessage(t[1], '271')
        # send error mail
        email_subject = '数据导入预警_{table_name}_{day}'.format(
            table_name=table_name,
            day=day)
        send_email(
            'bigdata_dw@opay-inc.com'
            , email_subject, t[1], mime_charset='utf-8')
        return

    write_meta_data(table_name, day, t[0], '验证完成，成功导入')


def validate(cursor, table_name, sql, day, day_before_1, day_before_7):
    error_message = ''
    is_import = 1

    cursor.execute(sql)
    res = cursor.fetchall()
    if res is None or len(res) < 2:
        is_import = 0
        error_message = '''
            {table_name}  未找到 {day} 日期数据
        '''.format(
            table_name=table_name,
            day=day)
    else:
        day_num = float(res[0][1])
        day_before_7_num = float(res[1][1])

        print('day_num : {} , day_before_7_num : {} '.format(day_num, day_before_7_num))

        if day_num == 0:
            is_import = 0
            error_message = '''
                {table_name}  {day}  日期数据数据量为 0
            '''.format(table_name=table_name, day=day)
        elif (day_num - day_before_7_num) < 0 and abs((day_num - day_before_7_num) / day_num) > ods_data_stop_limit:
            is_import = 0
            error_message = '''
                监控规则：数据量下降幅度超过10%进行预警，数据量下降幅度超过45%发送BI，进行审核。
                异常说明：表： {table_name} ，{day} 数据量为 {day_num}，{day_before_7}  数据量为 {day_before_7_num}，已达到预警标准。
                请BI 核查指标数据是否异常，如存在策略调整，请通知相关使用方。
            '''.format(
                table_name=table_name,
                day=day,
                day_num=day_num,
                day_before_7=day_before_7,
                day_before_7_num=day_before_7_num)
        elif (day_num - day_before_7_num) < 0 and abs((day_num - day_before_7_num) / day_num) > ods_data_alert_limit:
            error_message = '''
                监控规则：数据量下降幅度超过10%进行预警，数据量下降幅度超过45%发送BI，进行审核。
                异常说明：表： {table_name} ，{day} 数据量为 {day_num}，{day_before_7}  数据量为 {day_before_7_num}，需要BI审核。
                请BI 核查指标数据是否异常，如存在策略调整，请通知相关使用方。
            '''.format(
                table_name=table_name,
                day=day,
                day_num=day_num,
                day_before_7=day_before_7,
                day_before_7_num=day_before_7_num)

    return (is_import, error_message)


def write_meta_data(table_name, day, result, msg):
    cursor = get_hive_cursor()
    # if not result:
    #     sql = '''
    #         ALTER TABLE oride_db.{table_name} DROP IF EXISTS PARTITION(dt='{day}')
    #     '''.format(
    #         table_name=table_name,
    #         day=day)
    #
    #     cursor.execute(sql)

    sql = '''
        insert into table oride_bi.oride_meta_import_data 
        partition (dt='{day}',table_name='{table_name}')
        select {result},'{msg}','{timestamp}' from default.dual
    '''.format(
        table_name=table_name,
        day=day,
        result=result,
        msg=msg,
        timestamp=now.strftime('%Y-%m-%d %H:%M:%S')
    )

    cursor.execute(sql)


def validate_partition(*op_args, **op_kwargs):
    cursor = get_hive_cursor()
    dt = op_kwargs['ds']
    table_names = op_kwargs['table_names']
    task_name = op_kwargs['task_name']
    for table_name in table_names:
        sql = '''
            show partitions {table_name}
        '''.format(
            table_name=table_name
        )

        cursor.execute(sql)
        res = cursor.fetchall()

        flag = False
        for partition in res:
            if str(partition[0]).find(dt) > -1:
                flag = True
                break

        if not flag:
            comwx.postAppMessage('{table_name} : {dt} 分区不存在 , {task_name} 任务终止执行'.format(
                table_name=table_name,
                dt=dt,
                task_name=task_name
            ), '271')


def is_alert(dt, table_names):
    cursor = get_hive_cursor()
    template = "'{table_name}',"
    table_list = ''

    for table_name in table_names:
        if str(table_name).find('.') > -1:
            table_name = str(table_name).split('.')[1]
        table_list += template.format(table_name=table_name)

    table_list = table_list[0:len(table_list) - 1]

    sql = '''
        select 
        count(1)
        from 
        oride_bi.oride_meta_import_data
        where dt = '{dt}'
        and table_name in ({table_list})
        and is_import = 0
    '''.format(dt=dt,
               table_list=table_list)

    logging.info(sql)

    cursor.execute(sql)
    res = cursor.fetchall()
    result = int(res[0][0])

    return result


# 校验指标正确性
def validate_metrics(dt, source_name, data_map, metric_name_map):
    cursor = get_hive_cursor()
    sql = '''
        select 
        o.dt,o.metric_name,o.metric_compare_type,o.metric_deviation_limit
        from oride_bi.oride_metric_rule_info o
        join 
        (
        select max(dt) dt
        from oride_bi.oride_metric_rule_info
        where  metric_source_name = '{source_name}'
        ) t on t.dt = o.dt
        where o.metric_source_name = '{source_name}'

    '''.format(source_name=source_name)

    cursor.execute(sql)
    res = cursor.fetchall()

    # 没有校验规则
    if res is None or len(res) == 0:
        logging.info(' {} 没有校验规则'.format(source_name))
        return

    # 整合校验规则
    rule_map = dict()
    for data in res:
        key = data[1]
        value = (data[2], data[3])
        rule_map[key] = value

    error_metric_map = dict()

    for data_key, data_value in data_map.items():
        # 如果指标在校验规则指标内，进行校验
        if data_key in rule_map.keys():
            rule = rule_map.get(data_key)
            if rule[0] == 'number':
                if data_value[0] - data_value[1] < 0 and abs(data_value[0] - data_value[1]) > rule[1]:
                    error_metric_map[data_key] = [data_value[0], data_value[1], rule[0], rule[1]]
            elif rule[0] == 'rate':
                if data_value[0] - data_value[1] < 0 and abs((data_value[0] - data_value[1]) / data_value[0]) > rule[1]:
                    error_metric_map[data_key] = [data_value[0], data_value[1], rule[0], rule[1]]
            else:
                continue

    if len(error_metric_map) == 0:
        logging.info("{} 校验成功，指标校验没有异常 ".format(source_name))
        return

    sql = '''
        insert into table  oride_bi.oride_metric_validate_record partition (dt='{dt}',metric_source_name='{source_name}')
    '''.format(dt=dt, source_name=source_name)

    err_message = '指标计算异常，终止计算 \n'

    i = 0
    for key, value in error_metric_map.items():

        sql += '''
            select '{metric_name}',{metric_now_value},{metric_compare_value},'{metric_compare_type}',{metric_deviation_limit},'{timestamp}' from default.dual
        '''.format(
            metric_name=key,
            metric_now_value=value[0],
            metric_compare_value=value[1],
            metric_compare_type=value[2],
            metric_deviation_limit=value[3],
            timestamp=now.strftime('%Y-%m-%d %H:%M:%S')
        )
        if i != len(error_metric_map) - 1:
            sql += ' union all '
        i += 1

        err_message += '''
            报表: {source_name} \n
            指标名称:{metric_name} \n
            当日值: {metric_now_value} \n
            7日前值 : {metric_compare_value} \n
            比较类型 : {metric_compare_type} \n
            预警阀值: {metric_deviation_limit} \n
            \n
        '''.format(
            source_name=source_name,
            metric_name=metric_name_map[key],
            metric_now_value=value[0],
            metric_compare_value=value[1],
            metric_compare_type=value[2],
            metric_deviation_limit=value[3]
        )

    logging.info(err_message)

    cursor.execute(sql)

    # send mail
    email_subject = '调度算法效果监控指标预警邮件_{}'.format(dt)
    send_email(
        'bigdata_dw@opay-inc.com'
        , email_subject, err_message, mime_charset='utf-8')

    comwx.postAppMessage(err_message, '271')

    raise Exception('指标异常，终止计算')


'''
data_now：需要验证的数据
data_before_7 ： 7日前数据
metric_order_and_name_map ： 指标的顺序map
'''


def create_validate_data(data_now, data_before_7, metric_order_and_name_map):
    # 进行数据验证，拼接数据
    data_map = dict()

    j = 1
    while j < len(data_now):

        metric = str(data_now[j])
        if metric.find('%') > -1:
            metric = metric.replace('%', '')
        metric = float(metric)
        list_tmp = list()
        list_tmp.append(metric)
        data_map[metric_order_and_name_map[j]] = list_tmp
        j += 1

    j = 1
    while j < len(data_before_7):

        metric = str(data_before_7[j])
        if metric.find('%') > -1:
            metric = metric.replace('%', '')
        metric = float(metric)

        data_map[metric_order_and_name_map[j]].append(metric)
        j += 1

    return data_map

"""
数据量监控
"""
def data_volume_monitoring(ds, db_name, table_name, **op_kwargs):
    cursor = get_hive_cursor()
    sql = """
        SELECT count(1) FROM {db_name}.{table_name} WHERE dt='{dt}'
    """.format(
        db_name=db_name,
        table_name=table_name,
        dt=ds
    )
    logging.info("execute sql:%s", sql)
    cursor.execute(sql)
    res = cursor.fetchone()
    print(res)
    row_num = int(res[0])
    logging.info("import data {db}.{table}, row_num:{row_num}".format(db=db_name, table=table_name, row_num=row_num))
    if row_num <= 0:
        comwx.postAppMessage("{db}.{table}数据导入异常".format(db=db_name, table=table_name), '271')
        raise Exception('sqoop导入数据异常')


