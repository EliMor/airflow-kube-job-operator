import os
import yaml
import logging

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow_kjo.kubernetes_job_launcher import KubernetesJobLauncher

class KubernetesJobOperator(BaseOperator):
    """
    Opinionated operator for kubernetes Job type execution.
    Only allow client to pass in yaml files

    :param yaml_file_name: name of yaml file to be executed
    :param in_cluster: whether to use rbac inside the cluster rather than a config file
    :param config_file: a kube config file filename
    :param cluster_context: context to use referenced in the kube config file
    :param
    :type yaml_file_name: string
    """
    @apply_defaults
    def __init__(
        self,
        # yaml related params
        yaml_file_name,
        yaml_write_path=None,
        yaml_write_filename=None,
        yaml_template_fields={},
        # kube config related params
        in_cluster=None,
        config_file=None,
        cluster_context=None,
        # meta config
        ## log related
        tail_logs=True,
        tail_log_line_count=100,
        delete_completed_job=False,
        kube_launcher=None,
        **kwargs,
    ):
        super(KubernetesJobOperator, self).__init__(**kwargs)

        self.yaml_file_name = yaml_file_name
        self.yaml_write_path = yaml_write_path
        self.yaml_write_filename = yaml_write_filename
        self.yaml_template_fields = yaml_template_fields
        
        self.in_cluster = in_cluster
        self.config_file = config_file
        self.cluster_context = cluster_context
        self.tail_logs = tail_logs
        self.tail_log_line_count = tail_log_line_count

        self.delete_completed_job = delete_completed_job
        self.kube_launcher = kube_launcher
        if not self.kube_launcher:
            self.kube_launcher = KubernetesJobLauncher(
                in_cluster=self.in_cluster,
                cluster_context=self.cluster_context,
                config_file=self.config_file,
                tail_logs=self.tail_logs,
                tail_log_line_count=self.tail_log_line_count
            )

    def _retrieve_template_from_file(self, jinja_env):
        with open(self.yaml_file_name, "r") as yaml_file_obj:
            yaml_file = yaml_file_obj.read()
            template = jinja_env.from_string(yaml_file)
            return template

    def _write_rendered_template(self, content):
        filename = self.yaml_write_filename
        if not filename:
            filename = self.yaml_file_name
        with open(self.yaml_write_path.rstrip("/") + f"/{filename}", "w") as write_file:
            write_file.write(content)

    def execute(self, context):
        dag = context["dag"]
        jinja_env = dag.get_template_env()
        if dag.template_searchpath:
            template = jinja_env.get_template(self.yaml_file_name)
        else:
            template = self._retrieve_template_from_file(jinja_env)

        rendered_template = template.render(**self.yaml_template_fields)
        logging.info(f"Rendered....\n{rendered_template}")

        if self.yaml_write_path:
            self._write_rendered_template(rendered_template)

        yaml_obj = yaml.safe_load(rendered_template)

        self.kube_launcher.apply(yaml_obj)
        self.kube_launcher.watch(yaml_obj)
        if self.delete_completed_job:
            logging.info(f'Cleaning up Job')
            self.kube_launcher.delete(yaml_obj)

        return rendered_template
