import os
import yaml
import logging

from airflow.models.baseoperator import BaseOperator
from airflow_kjo.kubernetes_job_launcher import KubernetesJobLauncher, KubernetesJobYaml


class KubernetesJobOperator(BaseOperator):
    """
    Opinionated operator for kubernetes Job type execution.
    Only allow client to pass in yaml files

    :param yaml_file_name: name of yaml file to be executed
    :param yaml_write_path:
    :param yaml_write_filename:
    :param yaml_template_fields:
    :param in_cluster: whether to use rbac inside the cluster rather than a config file
    :param config_file: a kube config file filename
    :param cluster_context: context to use referenced in the kube config file
    :param tail_logs: should logs be output at the end, has some default behavior for simple usage
    :param tail_logs_every: frequency to output logs (seconds)
    :param tail_logs_line_count: num lines from end to output
    :param delete_completed_jobs: should completed jobs be autodeleted
    :param kube_launcher: pass in your own kube launcher if you're testing or brave
    """

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
        tail_logs=False,
        tail_logs_every=None,
        tail_logs_line_count=None,
        ##
        delete_completed_job=False,
        kube_launcher=None,
        #
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.yaml_file_name = yaml_file_name
        self.yaml_write_path = yaml_write_path
        self.yaml_write_filename = yaml_write_filename
        self.yaml_template_fields = yaml_template_fields

        self.in_cluster = in_cluster
        self.config_file = config_file
        self.cluster_context = cluster_context

        if bool(tail_logs_every) or bool(tail_logs):
            # set a default line count if they didnt provide one
            if not bool(tail_logs_line_count):
                tail_logs_line_count = 20

        self.tail_logs = tail_logs
        self.tail_logs_every = tail_logs_every
        self.tail_logs_line_count = tail_logs_line_count

        self.delete_completed_job = delete_completed_job
        self.kube_launcher = kube_launcher

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
        retry_count = (
            self.retries
        )  # provided by BaseOperator, can also set in default_args in DAG creation
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
        extra_yaml_configuration = {"backoff_limit": retry_count}
        kjy = KubernetesJobYaml(yaml_obj, extra_yaml_configuration)

        if not self.kube_launcher:
            self.kube_launcher = KubernetesJobLauncher(
                kube_yaml=kjy.yaml,
                in_cluster=self.in_cluster,
                cluster_context=self.cluster_context,
                config_file=self.config_file,
            )
        # ensure clean slate before creating job
        task_instance = context["task_instance"]
        if task_instance.try_number == 1:
            self.kube_launcher.delete(delete_failed=True, delete_completed=True)
        self.kube_launcher.apply()
        self.kube_launcher.watch(
            tail_logs=self.tail_logs,
            tail_logs_every=self.tail_logs_every,
            tail_logs_line_count=self.tail_logs_line_count
        )
        if self.delete_completed_job:
            logging.info(f"Cleaning up Job")
            self.kube_launcher.delete(delete_completed=True)

        return rendered_template
