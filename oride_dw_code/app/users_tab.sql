# 每周日计算之前所有的用户标签
# 1. 有账号从未下过单的用户
# 2. 下过单从未完单的用户
# 3. 有完单,计算时前7日没下单的用户
# 4. 有完单,计算时前14日没下单的用户

CREATE TABLE IF NOT EXISTS app_oride_users_tag_w(
    city_id bigint unsigned not null default 0 comment '城市ID',
    user_id bigint unsigned not null default 0 comment '用户ID',
    user_type tinyint unsigned not null default 0 comment '用户类型0有账号未下单,1下单未完单,2完单近7日未下单,3完单近14日未下单',
    phone_number varchar(20) not null default '' comment '用户手机号',
    create_at int unsigned not null default 0 comment '计算时间',
    PRIMARY KEY (city_id, user_id, user_type),
    KEY ct (city_id, user_type)
) engine=InnoDB default charset=utf8mb4 comment='oride用户标签表';