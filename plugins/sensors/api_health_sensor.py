from datetime import timedelta

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

from plugins.hooks.animals_api_hook import AnimalsAPIHook


class APIHealthSensor(BaseSensorOperator):
    template_fields = ("animals_api_conn_id",)

    def __init__(
        self, animals_api_conn_id: str = "animals_api_default", **kwargs
    ):
        super().__init__(**kwargs)
        self.animals_api_conn_id = animals_api_conn_id

    def poke(self, context: Context) -> bool:
        hook = AnimalsAPIHook(animals_api_conn_id=self.animals_api_conn_id)
        is_healthy = hook.health_check()

        if is_healthy:
            self.log.info("Animals API is healthy")
        else:
            self.log.info("Animals API is not healthy, waiting...")

        return is_healthy
