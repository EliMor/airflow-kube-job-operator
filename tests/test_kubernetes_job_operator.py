import pathlib
import unittest
from jinja2 import FileSystemLoader
from jinja2 import Template
from jinja2 import Environment
from datetime import datetime, timedelta
from airflow import DAG

from airflow_kjo import KubernetesJobOperator

class KubeLauncherMock:
    def __init__(self):
        pass
    def apply(self, **kwargs):
        pass
    def watch(self, **kwargs):
        pass
    def delete(self, **kwargs):
        pass

class TaskInstanceMock:
    def __init__(self):
        self.try_number = 1

class TestKubernetesJobOperator(unittest.TestCase):

    def setUp(self):
        super().setUp()
        default_args ={
        'owner': 'airflow',
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'start_date': datetime(2021, 2, 24, 12, 0),
        }
        path = str(pathlib.Path(__file__).parent.absolute())
        fixture_path = path + '/fixtures' 
        self.dag = DAG('test_kubernetes_job_op_dag', 
                        default_args=default_args,
                        template_searchpath=fixture_path)
        self.task_instance = TaskInstanceMock()
        self.kube_launcher = KubeLauncherMock()
        self.fixture_path = fixture_path


    def test_single_jinja_rendered(self):
        """
        Test that a templated yaml file will be properly rendered by the operator
        """

        yaml_file_name = 'test_single_yaml.tmpl'
        task_num = 1
        command = 'sleep 60; for i in 5 4 3 2 1 ; do echo $i ; done'

        with open(self.fixture_path+f'/{yaml_file_name}', 'r') as read_file_obj:
            yaml_content = read_file_obj.read()

        template = Template(yaml_content)
        expected_rendered = template.render(command=command, task_num=task_num)
        task = KubernetesJobOperator(task_id=yaml_file_name,
                yaml_file_name=yaml_file_name,
                yaml_template_fields={'command': command, 'task_num':task_num},
                kube_launcher=self.kube_launcher)
        
        rendered_result = task.execute({'dag':self.dag, 'ti': self.task_instance, 'task_instance':self.task_instance})

        assert rendered_result == expected_rendered


    def test_multiple_jinja_rendered(self):
        """
        Test that multiple templated yaml files using jinja scheme will be properly rendered
        """
        base_yaml_file_name = 'test_multiple_yaml_core.tmpl'
        command = 'sleep 60; for i in 5 4 3 2 1 ; do echo $i ; done'

        jinja_env = Environment(loader=FileSystemLoader(searchpath=self.fixture_path))
        template = jinja_env.get_template(base_yaml_file_name)
        expected_rendered = template.render(command=command)

        task = KubernetesJobOperator(task_id=base_yaml_file_name,
                yaml_file_name=base_yaml_file_name,
                yaml_template_fields={'command': command},
                kube_launcher=self.kube_launcher)
        
        rendered_result = task.execute({'dag':self.dag, 'ti': self.task_instance, 'task_instance':self.task_instance})

        assert rendered_result == expected_rendered
