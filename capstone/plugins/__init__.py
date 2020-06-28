import operators
from airflow.plugins_manager import AirflowPlugin


# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "capstone_plugin"
    operators = [
        operators.ReturnTracksOperator
    ]
