from airflow.plugins_manager import AirflowPlugin
from operators.my_custom_operator import MyCustomOperator

class CustomPlugin(AirflowPlugin):
    name = "custom_plugin"
    operators = [MyCustomOperator]