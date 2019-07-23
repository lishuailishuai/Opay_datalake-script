'''
    调度算法报表，播单数据
    校验字段指标的名称和sql 查询的顺序对应关系map
'''
capacity_dispatch_order_metric_order_map = {
    1: 'request_num',
    2: 'request_rate',
    3: 'on_ride_num',
    4: 'on_ride_rate',
    5: 'on_ride_driver_num',
    6: 'on_ride_avg',
    7: 'pick_up_time_avg',
    8: 'take_time_avg',
    9: 'sys_cancel_rate',
    10: 'passanger_before_cancel_rate',
    11: 'passanger_after_cancel_rate',
    12: 'validity_ride_num',
    13: 'validity_on_ride_rate',
    14: 'cannel_pick_avg',
    15: 'wait_time_avg',
    16: 'billing_time_avg',
    17: 'pay_time_avg',
}

'''
    调度算法报表，播单数据
    指标对应中文指标含义
'''
capacity_dispatch_order_metric_name_map = {
    'ride_num': '下单量',
    'request_num': '接单量',
    'request_rate': '接单率',
    'on_ride_num': '完单量',
    'on_ride_rate': '完单率',
    'on_ride_driver_num': '完单骑手数',
    'on_ride_avg': '人均完单量',
    'pick_up_time_avg': '单均接驾时长（分钟）',
    'take_time_avg': '单均应答时长（分钟）',
    'sys_cancel_rate': '系统取消率',
    'passanger_before_cancel_rate': '乘客应答前取消率',
    'passanger_after_cancel_rate': '乘客应答后取消率',
    'validity_ride_num': '有效下单量',
    'validity_on_ride_rate': '完单率(有效订单数)',
    'cannel_pick_avg': '平均取消接驾时长(分钟)',
    'wait_time_avg': '平均等待上车时长(分钟)',
    'billing_time_avg': '平均计费时长(分钟)',
    'pay_time_avg': '平均支付时长(分钟)',
}

'''
    调度算法报表，订单数据
    校验字段指标的名称和sql 查询的顺序对应关系map

'''
capacity_dispatch_report_metric_order_map = {
    1: 'report_times',
    2: 'not_found_driver_rate',
    3: 'filter_driver_rate',
    4: 'push_driver_rate',
    5: 'accept_driver_time_rate',
    6: 'not_idle_rate',
    7: 'assigned_another_job_rate',
    8: 'assigned_this_order_rate',
    9: 'not_in_service_mode_rate',
    10: 'push_avg',
    11: 'push_order_avg',
    12: 'order_push_driver_avg',
    13: 'accept_driver_time_avg',
    14: 'obey_rate'
}

'''
    调度算法报表，订单数据
    指标对应中文指标含义
'''
capacity_dispatch_report_metric_name_map = {
    'report_times': '播报轮数',
    'not_found_driver_rate': '圈选不到司机',
    'filter_driver_rate': '圈选后司机都被过滤',
    'push_driver_rate': '订单指派给司机',
    'accept_driver_time_rate': '司机成功接单',
    'not_idle_rate': '正在干活',
    'assigned_another_job_rate': '被其他订单锁住',
    'assigned_this_order_rate': '被指派过',
    'not_in_service_mode_rate': '不在接单状态',
    'push_avg': '骑手平均被推送次数',
    'push_order_avg': '骑手平均被推送订单',
    'order_push_driver_avg': '订单平均推送骑手数',
    'accept_driver_time_avg': '骑手平均应答次数',
    'obey_rate': '服从率'
}
