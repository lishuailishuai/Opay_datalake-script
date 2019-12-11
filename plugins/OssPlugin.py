from airflow import settings
from airflow.plugins_manager import AirflowPlugin
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
import oss2
import sys

class OssSensor(BaseSensorOperator):
    """
    Check Oss File Bucket Key

    Pre:
    pip install oss2
    Example:
        t=OssSensor(
            task_id='t',
            bucket_key='xxx',
            bucket_name='opay-datalake'
            oss_conn_id='oss_default'
            dag=dag
        )
    """
    template_fields = ('bucket_key', 'bucket_name')
    ui_color = settings.WEB_COLORS['LIGHTBLUE']
    @apply_defaults
    def __init__(
            self,
            bucket_key,
            bucket_name,
            oss_conn_id='oss_default',
            *args,
            **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_key = bucket_key
        self.bucket_name = bucket_name
        self.oss_conn_id = oss_conn_id

    def poke(self, context):
        self.log.info('Poking for file oss://%s/%s', self.bucket_name, self.bucket_key)
        conn = BaseHook.get_connection(self.oss_conn_id)
        endpoint = conn.host
        access_key_id = conn.login
        access_key_secret = conn.password
        auth = oss2.Auth(access_key_id, access_key_secret)
        bucket = oss2.Bucket(auth, endpoint, self.bucket_name)

        try:
            return bucket.object_exists(self.bucket_key)
        except Exception:
            e = sys.exc_info()
            self.log.debug("Caught an exception !: %s", str(e))
            return False
class AirflowOssPlugin(AirflowPlugin):
    name = "oss_plugin"
    sensors = [OssSensor]
