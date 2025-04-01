from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class MyCustomOperator(BaseOperator):
    @apply_defaults
    def __init__(self, my_param, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.my_param = my_param

    def execute(self, context):
        self.log.info(f"Executing MyCustomOperator with param: {self.my_param}")
        return f"Task completed with param: {self.my_param}"