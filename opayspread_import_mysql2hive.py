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
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook

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
            know_orider_extend string,
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
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    #'email': ['bigdata@opay-inc.com'],
    #'email_on_failure': True,
    #'email_on_retry': False,
}

dag = airflow.DAG(
    'opayspread_import_mysql2hive',
    schedule_interval="0 0 * * *",
    concurrency=5,
    max_active_runs=1,
    default_args=args)

table_list = [
    "promoter_user",
    "rider_signups",
    "rider_signups_guarantors"
]

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

    create_table >> import_from_mysql >> add_partitions >> refresh_impala
