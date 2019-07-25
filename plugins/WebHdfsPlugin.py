from airflow import settings
from airflow.hooks.base_hook import BaseHook
from airflow.plugins_manager import AirflowPlugin
from hdfs.client import Client
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import sys

class WebHdfsHook(BaseHook):
    def __init__(self, hdfs_conn_id='webhdfs_default'):
        self.hdfs_conn_id = hdfs_conn_id

    def get_conn(self):
        connection = self.get_connection(self.hdfs_conn_id)
        client = Client("http://%s:%s" % (connection.host, connection.port))
        return client

class WebHdfsSensor(BaseSensorOperator):
    """
    Waits for a file or folder to land in HDFS
    """
    template_fields = ('filepath',)
    ui_color = settings.WEB_COLORS['LIGHTBLUE']
    @apply_defaults
    def __init__(
            self,
            filepath,
            hdfs_conn_id='webhdfs_default',
            *args,
            **kwargs):
        super().__init__(*args, **kwargs)
        self.filepath = filepath
        self.hdfs_conn_id = hdfs_conn_id
    def get_hook(self):
        return WebHdfsHook(
            hdfs_conn_id=self.hdfs_conn_id)

    def poke(self, context):
        conn = self.get_hook().get_conn()
        self.log.info('Poking for file %s', self.filepath)
        try:
            result = conn.status(self.filepath, strict=False)
            return bool(result)
        except Exception:
            e = sys.exc_info()
            self.log.debug("Caught an exception !: %s", str(e))
            return False

class AirflowWebHdfsPlugin(AirflowPlugin):
    name = "web_hdfs_plugin"
    sensors = [WebHdfsSensor]
    hooks = [WebHdfsHook]
