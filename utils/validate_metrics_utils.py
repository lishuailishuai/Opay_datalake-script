from datetime import datetime, timedelta
import logging
from utils.connection_helper import get_hive_cursor
from airflow.utils.email import send_email
from plugins.comwx import ComwxApi

comwx = ComwxApi('wwd26d45f97ea74ad2', 'BLE_v25zCmnZaFUgum93j3zVBDK-DjtRkLisI_Wns4g', '1000011')

cursor = get_hive_cursor()
now = datetime.today()


def validate_partition(*op_args, **op_kwargs):
    print (' op_kwargs = ' + str(op_kwargs))
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

            raise Exception('{table_name} : {dt} 分区不存在 , {task_name} 任务终止执行'.format(
                table_name=table_name,
                dt=dt,
                task_name=task_name
            ))


# 校验指标正确性
def validate_metrics(dt, source_name, data_map, metric_name_map):
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
        print(' {} 没有校验规则'.format(source_name))
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

    print ('error_metric_map = ' + str(error_metric_map))

    if len(error_metric_map) == 0:
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

    print(sql)
    print(err_message)

    cursor.execute(sql)

    # send mail
    email_subject = '调度算法效果监控指标预警邮件_{}'.format(dt)
    send_email(
        'nan.li@opay-inc.com'
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

    print ('data_now  = ' + str(data_now))
    print ('data_before_7  = ' + str(data_before_7))
    print ('metric_order_and_name_map  = ' + str(metric_order_and_name_map))

    j = 1
    while j < len(data_now):
        print(j)
        print(type(metric_order_and_name_map[j]))
        print(type(data_now[j]))

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
