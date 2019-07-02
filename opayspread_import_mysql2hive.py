'''
add by duo.wu 中台业务数据库导入hive
@2019-07-01
'''

import airflow
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from utils.connection_helper import get_db_conf
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator

opaySpreadTable = {
    'promoter_user': '''
        CREATE EXTERNAL TABLE IF NOT EXISTS promoter_user (
            id bigint,
            user_name string,
            name string,
            pass string,
            code string,
            team_id int,
            token string,
            create_time string
        ) 
        PARTITIONED BY (dt string)
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        LOCATION 'ufile://opay-datalake/opay-spread/promoter_user'
    ''',

    'rider_signups': '''
        CREATE EXTERNAL TABLE IF NOT EXISTS rider_signups(
            id int,
            name string,
            mobile string,
            gender int,
            birthday string,
            country string,
            state string,
            city string,
            address string,
            address_photo string,
            address_status string,
            address_status_note string,
            adress_status_time int,
            address_status_admin_id int,
            address_collecting_time int,
            avator string,
            dirver_experience int,
            license_number string,
            holding_license_time int,
            gmail_account string,
            opay_account string,
            drivers_test int,
            drivers_test_note string,
            drivers_test_time int,
            drivers_test_admin_id int,
            way_know int,
            base_finished_time int,
            bvn_number string,
            bnv_status int,
            bvn_status_note string,
            bvn_time int,
            bvn_admin_id int,
            veri_time int,
            status int,
            note string,
            admin_id int,
            reg_code string,
            create_time int,
            update_time int,
            rider_experience int,
            exp_cert_images string,
            exp_plate_number string,
            know_orider int,
            agent_opay_account string,
            field_sales_number string,
            telesales_number string,
            riders_number string,
            road_show_number string,
            hr_agent_company int,
            emergencies_name string,
            emergencies_mobile string,
            traing_test int,
            is_reward_amount int,
            reward_amount int,
            marital_status int,
            religion int,
            religion_other string,
            id_number string,
            online_test int,
            online_test_note string,
            online_test_time int,
            online_test_admin_id int,
            driver_type int,
            own_vehicle_brand string,
            own_vehicle_brand_other string,
            own_vehicle_model string,
            own_plate_number string,
            own_chassis_number string,
            own_engine_number string,
            own_engine_capacity int,
            own_bike_photos string,
            local_government string,
            vehicle_status int,
            vehicle_status_note string,
            vehicle_status_time int,
            vehicle_status_admin_id int,
            record_by string   
        )
        PARTITIONED BY (`dt` string)
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        LOCATION
            'ufile://opay-datalake/opay-spread/rider_signups';
    ''',

    'rider_signups_guarantors': '''
        CREATE EXTERNAL TABLE IF NOT EXISTS rider_signups_guarantors (
            id int,
            rider_id int,
            name string,
            gender int,
            country string,
            state string,
            city string,
            address string,
            address_photo string,
            address_status int,
            address_status_note string,
            mobile string,
            n_passport string,
            y_passport string,
            passport_status int,
            passport_status_note string,
            address_admin_id int,
            address_admin_time int,
            passport_admin_id int,
            passport_admin_time int,
            note string,
            update_time int,
            create_time int
        )
        PARTITIONED BY (`dt` string)
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        LOCATION
            'ufile://opay-datalake/opay-spread/rider_signups_guarantors';
    '''
}


def getOpaySpreadTableSource(tablename):
    return opaySpreadTable.get(tablename, False)


args = {
    'owner': 'root',
    'start_date': datetime(2019, 6, 22),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    #'email': ['bigdata@opay-inc.com'],
    #'email_on_failure': True,
    #'email_on_retry': False,
}

dag = airflow.DAG(
    'opayspread_import_mysql2hive',
    schedule_interval="0 3 * * *",
    concurrency=5,
    max_active_runs=1,
    default_args=args)

table_list = [
    "promoter_user",
    "rider_signups",
    "rider_signups_guarantors"
]

'''
数据统计任务
'''
create_oride_promoter_overview = HiveOperator(
    task_id='create_oride_promoter_overview',
    hql='''
        CREATE TABLE IF NOT EXISTS oride_promoter_overview (
            daily string,
            driver_type int,
            promoter_id int,
            promoter_name string,
            promoter_mobile string,
            drivers_preregist int,
            drivers_regist int,
            drivers_written int,
            drivers_drive int,
            vehicle_status int,
            
            drivers_firstorder int,
            drivers_10order int,
            drivers_online int,
            orders_finish int,
            orders_total int,
            fullinfo int,
            kpi int,
            drivers_guarantors int,
            drivers_vehicle int
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET
    ''',
    schema='dashboard',
    dag=dag
)

promoter_user_list = HiveOperator(
    task_id='promoter_user_data',
    hql='''
        INSERT OVERWRITE TABLE oride_promoter_overview PARTITION (dt='{{ ds }}')
        SELECT 
            from_unixtime(r.create_time),
            r.driver_type,
            p.id,
            p.user_name,
            p.name,
            count(1),
            sum(if(length(r.field_sales_number)=6 and r.record_by<>'', 1, 0)),
            sum(if(r.online_test=1, 1, 0)),
            sum(if(r.drivers_test=1, 1, 0)),
            sum(if(r.driver_type=2 and r.vehicle_status=1), 1, 0)
        FROM opay_spread.promoter_user as p LEFT JOIN opay_spread.rider_signups as r 
        ON if(r.know_orider=4, r.field_sales_number=p.name, if(r.know_orider=5, r.telesales_number=p.name, 0)) 
        WHERE r.create_time >= unix_timestamp('{{ ds }}') and 
            r.create_time <= unix_timestamp('{{ ds }} 23:59:59') 
              
        
    ''',
    schema='dashboard',
    dag=dag
)

'''
导入数据任务
'''
host, port, schema, login, password = get_db_conf('opay_spread_mysql')
for opayspreadtable in table_list:
    tableStruct = getOpaySpreadTableSource(opayspreadtable)

    '''
    创建hive外部表
    '''
    create_table = HiveOperator(
        task_id='create_table_{}'.format(opayspreadtable),
        hql=tableStruct,
        schema='opay_spread',
        dag=dag
    )

    '''
    使用sqoop导入mysql数据到hive
    '''
    import_from_mysql = BashOperator(
        task_id='import_from_mysql_{}'.format(opayspreadtable),
        bash_command='''
            #!/usr/bin/env bash
            sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
            --connect "jdbc:mysql://{host}:{port}/{schema}?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8" \
            --username {username} \
            --password {password} \
            --table {table} \
            --target-dir ufile://opay-datalake/opay-spread/{table}/dt={{{{ ds }}}}/ \
            --fields-terminated-by "\\001" \
            --lines-terminated-by "\\n" \
            --hive-delims-replacement " " \
            --delete-target-dir \
            --compression-codec=snappy
        '''.format(
            host=host,
            port=3306,
            schema=schema,
            username=login,
            password=password,
            table=opayspreadtable
        ),
        dag=dag
    )

    '''
    添加hive表分区
    '''
    add_partitions = HiveOperator(
        task_id='add_partitions_{}'.format(opayspreadtable),
        hql='''
                ALTER TABLE opay_spread.{table} ADD IF NOT EXISTS PARTITION (dt = '{{{{ ds }}}}')
            '''.format(table=opayspreadtable),
        schema='opay_spread',
        dag=dag
    )

    '''
    刷新impala数据库
    '''
    refresh_impala = ImpalaOperator(
        task_id='refresh_impala_{}'.format(opayspreadtable),
        hql="""\
            REFRESH {table};
        """.format(table=opayspreadtable),
        schema='opay_spread',
        priority_weight=50,
        dag=dag
    )

    create_table >> import_from_mysql >> add_partitions >> refresh_impala >> create_oride_promoter_overview >> promoter_user_list


