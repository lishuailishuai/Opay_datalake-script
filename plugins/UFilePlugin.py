from airflow import settings
from airflow.plugins_manager import AirflowPlugin
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from ufile import filemanager, config
import sys

class UFileSensor(BaseSensorOperator):
    """
    Check UFile file path

    Pre:
    pip install ufile
    create airflow variable UCloudBucket,UCloudPrivateKey,UCloudPublicKey,UCloudUploadSuffix

    Example:
        t=UFileSensor(
            task_id='t',
            filepath='ofood/client/dt=2019-07-24',
            bucket_name='opay-datalake',
            dag=dag
        )

    """
    template_fields = ('filepath', 'bucket_name', 'public_key', 'private_key', 'upload_suffix')
    ui_color = settings.WEB_COLORS['LIGHTBLUE']
    @apply_defaults
    def __init__(
            self,
            filepath,
            bucket_name=Variable.get("UCloudBucket"),
            public_key=Variable.get("UCloudPublicKey"),
            private_key=Variable.get("UCloudPrivateKey"),
            upload_suffix=Variable.get("UCloudUploadSuffix"),
            *args,
            **kwargs):
        super().__init__(*args, **kwargs)
        self.filepath = filepath
        self.bucket_name = bucket_name
        self.public_key = public_key
        self.private_key = private_key
        self.upload_suffix = upload_suffix

    def poke(self, context):
        self.log.info('Poking for file ufile://%s/%s', self.bucket_name, self.filepath)
        config.set_default(uploadsuffix=self.upload_suffix)
        filemanager_handler=filemanager.FileManager(self.public_key, self.private_key)
        try:
            ret, resp=filemanager_handler.getfilelist(self.bucket_name, self.filepath)
            return bool('DataSet' in ret and len(ret['DataSet'])>0)
        except Exception:
            e = sys.exc_info()
            self.log.debug("Caught an exception !: %s", str(e))
            return False
class AirflowUFilePlugin(AirflowPlugin):
    name = "ufile_plugin"
    sensors = [UFileSensor]
