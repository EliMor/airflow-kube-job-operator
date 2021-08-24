import time
import logging
from kubernetes.client.rest import ApiException

# https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/BatchV1Api.md

from airflow_kjo.kubernetes_util import (
    get_kube_client,
    get_kube_job_client,
    get_kube_pod_client,
)


class KubernetesJobYaml:
    def __init__(self, yaml, extra_configuration, overwrite=False):
        self._validate_job_yaml(yaml)
        self.yaml = self._expand_yaml_obj_with_configuration(
            yaml, extra_configuration, overwrite
        )
        self.name = self.yaml["metadata"]["name"]
        self.namespace = self.yaml["metadata"]["namespace"]

    @staticmethod
    def _validate_job_yaml(yaml):
        """
        Ensure that the yaml obj passes some requirements,
        1. must be a Job type
        2. must have a name and namespace field in metadata block
        """
        assert yaml["kind"] == "Job"
        metadata = yaml["metadata"]
        metadata["name"]
        metadata["namespace"]

    @staticmethod
    def _expand_yaml_obj_with_configuration(yaml, configuration, overwrite):
        if "parallelism" in configuration:
            if overwrite or "parallelism" not in yaml["spec"]:
                yaml["spec"]["parallelism"] = configuration["parallelism"]
        if "backoff_limit" in configuration:
            if overwrite or "backoff_limit" not in yaml["spec"]:
                yaml["spec"]["backoffLimit"] = configuration["backoff_limit"]
        return yaml


class KubernetesJobLauncherPodError(Exception):
    """
    Created Job ended in an errored pod state
    """
    ...


class KubernetesJobLauncher:
    def __init__(
        self,
        kube_yaml=None,
        kube_client=None,
        in_cluster=True,
        cluster_context=None,
        config_file=None,
    ):
        self.kube_client = kube_client or get_kube_client(
            in_cluster=in_cluster,
            cluster_context=cluster_context,
            config_file=config_file,
        )

        self.kube_job_client = get_kube_job_client(self.kube_client)
        self.kube_pod_client = get_kube_pod_client(self.kube_client)
        self.sleep_time = 5
        self.kube_yaml = kube_yaml

    def _tail_pod_logs(self, num_lines=100):
        had_logs = False
        # can only get a log if pod is in one of these states
        logable_statuses = {"Running", "Failed", "Succeeded"}
        # get all pods for the job
        job_pods = self.kube_pod_client.list_namespaced_pod(
            namespace=self.kube_yaml.namespace,
            label_selector=f"job-name={self.kube_yaml.name}",
        )
        for pod in job_pods.items:
            pod_name = pod.metadata.name
            # only continue if pod is running, completed or errored
            pod_phase = pod.status.phase
            if pod_phase not in logable_statuses:
                continue
            # TODO should see if can use since_seconds in a good way
            # https://raw.githubusercontent.com/kubernetes-client/python/master/kubernetes/client/api/core_v1_api.py
            if bool(num_lines):
                read_log = self.kube_pod_client.read_namespaced_pod_log(
                    name=pod_name,
                    namespace=self.kube_yaml.namespace,
                    tail_lines=num_lines,
                )
                line_or_lines = "line" if num_lines == 1 else "lines"
                msg = f'Reading last {num_lines} {line_or_lines} from log for Pod "{pod_name}" in Namespace "{self.kube_yaml.namespace}"'
            else:
                msg = f'Reading full logfile for Pod "{pod_name}" in Namespace "{self.kube_yaml.namespace}"'
                # could this ever be too much data?
                read_log = self.kube_pod_client.read_namespaced_pod_log(
                    name=pod_name, namespace=self.kube_yaml.namespace
                )
            lines = [line for line in read_log]
            str_lines = "".join(lines).strip()
            if str_lines:
                logging.info(msg)
                logging.info(f"Reading....\n{str_lines}")
                logging.info(
                    f'End log for Pod "{pod_name}" in Namespace "{self.kube_yaml.namespace}"'
                )
                had_logs = True
        return had_logs

    def get(self):
        try:
            job = self.kube_job_client.read_namespaced_job_status(
                self.kube_yaml.name, self.kube_yaml.namespace
            )
            return job
        except ApiException as error:
            if error.status == 404:
                # does not exist yet
                return False
            else:
                logging.error(error.body)
                raise

    def apply(self):
        try:
            self.kube_job_client.create_namespaced_job(
                namespace=self.kube_yaml.namespace, body=self.kube_yaml
            )
        except ApiException as error:
            if error.status == 409:
                # already exists
                logging.info(error.body)
                return True
            else:
                logging.error(error.body)
                raise
        return True

    def watch(
        self,
        tail_logs_every=None,
        tail_logs_line_count=100,
        tail_logs_only_at_end=False,
        running_timeout=None,
    ):

        job = self.get()
        total_time = 0
        log_cycles = 1
        while True:
            if not job:
                return False

            completed = bool(job.status.succeeded)
            if completed:
                if bool(tail_logs_every) or tail_logs_only_at_end:
                    logging.info(f'Final log output for Job "{self.kube_yaml.name}"')
                    self._tail_pod_logs(tail_logs_line_count)
                logging.info(f'Job "{self.kube_yaml.name}" status is Completed')
                return True
            if running_timeout and total_time > running_timeout:
                pass  # running timeout exceeded, probably just a warning, would allow task to continue

            failed = bool(job.status.failed)
            if failed:
                if bool(self.tail_logs_every) or self.tail_logs_only_at_end:
                    self._tail_pod_logs(tail_logs_line_count)
                raise KubernetesJobLauncherPodError(
                    f'Job "{self.kube_yaml.name}" in Namespace "{self.kube_yaml.namespace}" ended in Error state'
                )
            if bool(tail_logs_every) and not tail_logs_only_at_end:
                if (
                    total_time > 0
                    and total_time % (tail_logs_every // self.sleep_time) == 0
                ):
                    logging.info(f"Beginning new log dump cycle :: {log_cycles}")
                    had_logs = self._tail_pod_logs(tail_logs_line_count)
                    no_logs_msg = (
                        ", no logs found to output this cycle" if not had_logs else ""
                    )
                    logging.info(f"Log dump cycle {log_cycles} complete{no_logs_msg}")
                    log_cycles += 1

            time.sleep(self.sleep_time)
            total_time += self.sleep_time
            job = self.get()

    def delete(self, delete_failed=False, delete_completed=False):
        """
        :param delete_failed: bool
            will delete job if state is errored
        :param delete_complete: bool
            will delete job if state is complete
        """
        delete = False
        # Verify exists before delete
        job = self.get()
        if job:
            completed = bool(job.status.succeeded)
            failed = bool(job.status.failed)
            if delete_failed and failed:
                delete = True
            if delete_completed and completed:
                delete = True
            if delete:
                self.kube_job_client.delete_namespaced_job(
                    name=self.kube_yaml.name,
                    namespace=self.kube_yaml.namespace,
                    propagation_policy="Foreground",
                )
        return delete
