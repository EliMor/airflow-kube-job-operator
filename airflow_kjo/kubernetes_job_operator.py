import os
import yaml
import logging

from airflow.models.baseoperator import BaseOperator
from airflow_kjo.kubernetes_job_launcher import KubernetesJobLauncher


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
    :param tail_logs: should logs be output, has some default behavior for simple usage
    :param tail_logs_every: frequency to output logs
    :param tail_logs_line_count: num lines from end to output
    :param tail_logs_only_at_end: should logs only output if job ended in complete/error
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
        ### end = (Completed, Error)
        tail_logs_only_at_end=False,
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

        # set a default line count and consider case where client only gives the line count
        if not bool(tail_logs_line_count):
            tail_logs_line_count = 20
        else:
            if not tail_logs_only_at_end:
                tail_logs = True
        # set a default cycle time if client wants logs to be tailed but didnt provide a cycle time
        tail_logs_every = (
            30 if tail_logs and not bool(tail_logs_every) else tail_logs_every
        )
        self.tail_logs_every = tail_logs_every
        self.tail_logs_line_count = tail_logs_line_count
        self.tail_logs_only_at_end = tail_logs_only_at_end
        if tail_logs and self.tail_logs_only_at_end:
            logging.info(
                'Parameter "tail_logs" unnecessary if using "tail_logs_only_at_end"'
            )

        if self.tail_logs_only_at_end and self.tail_logs_every:
            parameter_confusion_msg = (
                'Set either "tail_logs_only_at_end" or "tail_logs_every" but not both.'
            )
            raise ValueError(parameter_confusion_msg)

        self.delete_completed_job = delete_completed_job
        self.kube_launcher = kube_launcher
        if not self.kube_launcher:
            self.kube_launcher = KubernetesJobLauncher(
                in_cluster=self.in_cluster,
                cluster_context=self.cluster_context,
                config_file=self.config_file,
                tail_logs_every=self.tail_logs_every,
                tail_logs_line_count=self.tail_logs_line_count,
                tail_logs_only_at_end=self.tail_logs_only_at_end,
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

        # ensure clean slate before creating job
        task_instance = context["task_instance"]
        if task_instance.try_number == 1:
            self.kube_launcher.delete(
                yaml_obj, delete_failed=True, delete_completed=True
            )
        self.kube_launcher.apply(yaml_obj, extra_yaml_configuration)
        self.kube_launcher.watch(yaml_obj)
        if self.delete_completed_job:
            logging.info(f"Cleaning up Job")
            self.kube_launcher.delete(yaml_obj, delete_completed=True)

        return rendered_template
