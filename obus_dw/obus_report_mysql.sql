create database if not exists obus_dw;

#1.obus分城市汇总
create table if not exists app_obus_report_collect_d (
    dt timestamp not null default '1970-01-02' comment '日期',
    city_id int unsigned not null default '0' comment '城市ID 0为所有城市汇总',
    city varchar(32) not null default '' comment '城市',
    total_lines_double int unsigned not null default '0' comment '总线路数量(双)',
    total_drivers int unsigned not null default '0' comment '线路总司机数量',
    serv_drivers int unsigned not null default '0' comment '线路上班司机数量',
    no_serv_drivers int unsigned not null default '0' comment '线路下班司机数量',
    lines_orders_double int unsigned not null default '0' comment '线路订单数(双)',
    lines_finished_orders_double int unsigned not null default '0' comment '线路完单数(双)',
    total_stations int unsigned not null default '0' comment '总站点数',
    line_gmv_double decimal(15,2) unsigned not null default '0.00' comment '线路收益(双)',
    new_users int unsigned not null default '0' comment '新用户数量',
    obusapp_new_users int unsigned not null default '0' comment 'ObusAPP新用户数量',
    ticket_new_users int unsigned not null default '0' comment '首次使用公交卡新用户数量',
    recharge_users int unsigned not null default '0' comment '用户钱包充值人数',
    online_uv int unsigned not null default '0' comment '用户钱包总数量=线上uv',
    money_ballet_users int unsigned not null default '0' comment '今日钱包使用人数',
    tied_cards int unsigned not null default '0' comment '绑卡数',
    money_ballet_recharge_users int unsigned not null default '0' comment '今日钱包新充值人数',
    primary key (dt, city_id)
)engine=innodb default charset=utf8;

#2.obus线路明细
create table if not exists app_obus_report_path_detail_d (
    dt timestamp not null default '1970-01-02' comment '日期',
    city_id int unsigned not null default '0' comment '城市ID',
    city varchar(32) not null default '' comment '城市',
    line_id bigint unsigned not null default '0' comment '线路ID',
    line_name varchar(64) not null default '' comment '线路名称',
    station_id bigint unsigned not null default '0' comment '站点ID',
    station_name varchar(64) not null default '' comment '站点名称',
    total_drivers int unsigned not null default '0' comment '司机总人数',
    serv_drivers int unsigned not null default '0' comment '上班司机数',
    serv_on_the_road_drivers int unsigned not null default '0' comment '上班行驶司机数',
    serv_idle_drivers int unsigned not null default '0' comment '上班未行驶司机数',
    no_serv_drivers int unsigned not null default '0' comment '下班司机数',
    lines_orders int unsigned not null default '0' comment '线路总订单数',
    station_orders int unsigned not null default '0' comment '分站点订单数',
    lines_finished_orders int unsigned not null default '0' comment '线路总完单数',
    station_finished_orders int unsigned not null default '0' comment '分站点完单数',
    new_users int unsigned not null default '0' comment '新用户数量',
    get_on_users int unsigned not null default '0' comment '上车乘客数',
    get_off_users int unsigned not null default '0' comment '下车乘客数',
    line_gmv_single decimal(15,2) unsigned not null default '0.00' comment '线路收益(单)',
    primary key (dt, city_id, line_id, station_id)
)engine=innodb default charset=utf8;

#3.obus司机排行榜
create table if not exists app_obus_report_driver_rank_d (
    dt timestamp not null default '1970-01-02' comment '日期',
    city_id int unsigned not null default '0' comment '城市ID',
    city varchar(32) not null default '' comment '城市',
    driver_id bigint unsigned not null default '0' comment '司机ID',
    driver_name varchar(32) not null default '' comment '司机名字',
    serv_time decimal(5,2) unsigned not null default '0.00' comment '司机今日在线时长(小时)',
    cycle_cnt int unsigned not null default '0' comment '司机跑圈次数',
    avg_time decimal(5,2) unsigned not null default '0.00' comment '司机跑一圈的均时(小时)',
    driver_amount decimal(15,2) unsigned not null default '0.00' comment '司机收入',
    obus_pay_driver_amount decimal(15,2) unsigned not null default '0.00' comment 'Obus支付司机收入',
    tickets_pay_driver_amount decimal(15,2) unsigned not null default '0.00' comment '公交卡支付司机收入',
    num int unsigned not null default '0' comment '司机在路线中所有司机收入排名',
    primary key (dt, city_id, driver_id)
)engine=innodb default charset=utf8;

#4.obus司机明细
create table if not exists app_obus_report_driver_detail_d (
    dt timestamp not null default '1970-01-02' comment '日期',
    num int unsigned not null default '0' comment '序号',
    driver_id bigint unsigned not null default '0' comment '司机ID',
    driver_name varchar(32) not null default '' comment '司机姓名',
    driver_phone varchar(20) not null default '' comment '司机手机号',
    driver_bus_number varchar(30) not null default '' comment '车辆编号',
    cycle_id bigint unsigned not null default '0' comment '环线代号',
    cycle_name varchar(60) not null default '' comment '所属线路',
    number_of_seats tinyint unsigned not null default '0' comment '座位数',
    mtd_serv_time_today decimal(6,2) unsigned not null default '0.00' comment '本日累计上班时长(分)',
    finished_orders_today int unsigned not null default '0' comment '本日已经完成的订单数',
    mtd_gmv_today decimal(15,2) unsigned not null default '0.00' comment '本日累计交易额',
    primary key (dt, driver_id)
)engine=innodb default charset=utf8;
