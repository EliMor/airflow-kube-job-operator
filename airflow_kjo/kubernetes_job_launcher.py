import time
import logging
import yaml
from kubernetes.client.rest import ApiException

# https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/BatchV1Api.md

from airflow_kjo.kubernetes_util import (
    get_kube_client,
    get_kube_job_client,
    get_kube_pod_client,
)


class KubernetesJobLauncherPodError(Exception):
    """
    Created Job ended in an errored pod state
    """
    ...


class KubeYamlValidationError(Exception):
    ...


class KubernetesJobLauncher:
    def __init__(
        self,
        kube_client=None,
        in_cluster=True,
        cluster_context=None,
        config_file=None
    ):
        self.kube_client = kube_client or get_kube_client(
            in_cluster=in_cluster,
            cluster_context=cluster_context,
            config_file=config_file,
        )

        self.kube_job_client = get_kube_job_client(self.kube_client)
        self.kube_pod_client = get_kube_pod_client(self.kube_client)
        self.sleep_time = 5
        self.yaml_obj = None
        self.name = None
        self.namespace = None

    def __call__(self, yaml_obj):
        self._validate_job_yaml(yaml_obj)
        self.yaml_obj = yaml_obj
        self.name, self.namespace = self._get_name_namespace()

    @staticmethod
    def _validate_job_yaml(yaml_obj):
        """
        Ensure that the yaml obj passes some requirements,
        !. must have a name and namespace field in metadata block
        """
        try:
            metadata = yaml_obj["metadata"]
            metadata["name"]
            metadata["namespace"]
        except KeyError as error:
            raise KubeYamlValidationError(f"Kube yaml must include a {error}")

    def _get_name_namespace(self):
        return self.yaml_obj["metadata"]["name"], self.yaml_obj["metadata"]["namespace"]

    def _tail_pod_logs(self, num_lines=100):
        had_logs = False
        # can only get a log if pod is in one of these states
        logable_statuses = {"Running", "Failed", "Succeeded"}
        # get all pods for the job
        job_pods = self.kube_pod_client.list_namespaced_pod(
            namespace=self.namespace, label_selector=f"job-name={self.name}"
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
                    name=pod_name, namespace=self.namespace, tail_lines=num_lines
                )
                line_or_lines = "line" if num_lines == 1 else "lines"
                msg = f'Reading last {num_lines} {line_or_lines} from log for Pod "{pod_name}" in Namespace "{self.namespace}"'
            else:
                msg = f'Reading full logfile for Pod "{pod_name}" in Namespace "{self.namespace}"'
                # could this ever be too much data?
                read_log = self.kube_pod_client.read_namespaced_pod_log(
                    name=pod_name, namespace=self.namespace
                )
            lines = [line for line in read_log]
            str_lines = "".join(lines).strip()
            if str_lines:
                logging.info(msg)
                logging.info(f"Reading....\n{str_lines}")
                logging.info(f'End log for Pod "{pod_name}" in Namespace "{self.namespace}"')
                had_logs = True
        return had_logs

    def _expand_yaml_obj_with_configuration(self, configuration, overwrite=False):
        if "parallelism" in configuration:
            if overwrite or "parallelism" not in self.yaml_obj["spec"]:
                self.yaml_obj["spec"]["parallelism"] = configuration["parallelism"]
        if "backoff_limit" in configuration:
            if overwrite or "backoff_limit" not in self.yaml_obj["spec"]:
                self.yaml_obj["spec"]["backoffLimit"] = configuration["backoff_limit"]
        return self.yaml_obj

    def get(self):
        try:
            job = self.kube_job_client.read_namespaced_job_status(self.name, self.namespace)
            return job
        except ApiException as error:
            if error.status == 404:
                # does not exist yet
                return False
            else:
                logging.error(error.body)

    def apply(self, extra_configuration={}):
        yaml_obj = self._expand_yaml_obj_with_configuration(extra_configuration)
        try:
            self.kube_job_client.create_namespaced_job(
                namespace=self.namespace, body=yaml_obj
            )
        except ApiException as error:
            if error.status == 409:
                # already exists
                logging.info(error.body)
                return True
            else:
                logging.error(error.body)
        return True

    def watch(self, tail_logs_every=None,
        tail_logs_line_count=100,
        tail_logs_only_at_end=False, running_timeout=None):
        
        job = self.get()
        total_time = 0
        log_cycles = 1
        while True:
            if not job:
                return False

            completed = bool(job.status.succeeded)
            if completed:
                if bool(tail_logs_every) or tail_logs_only_at_end:
                    logging.info(f'Final log output for Job "{self.name}"')
                    self._tail_pod_logs(self.name, self.namespace, tail_logs_line_count)
                logging.info(f'Job "{self.name}" status is Completed')
                return True
            if running_timeout and total_time > running_timeout:
                pass  # running timeout exceeded, probably just a warning, would allow task to continue

            if bool(job.status.failed):
                if bool(tail_logs_every) or tail_logs_only_at_end:
                    self._tail_pod_logs(self.name, self.namespace, tail_logs_line_count)
                raise KubernetesJobLauncherPodError(
                    f'Job "{self.name}" in Namespace "{self.namespace}" ended in Error state'
                )
            if bool(tail_logs_every) and not tail_logs_only_at_end:
                if (
                    total_time > 0
                    and total_time % (tail_logs_every // self.sleep_time) == 0
                ):
                    logging.info(f"Beginning new log dump cycle :: {log_cycles}")
                    had_logs = self._tail_pod_logs(self.name, self.namespace, tail_logs_line_count)
                    no_logs_msg = (
                        ", no logs found to output this cycle" if not had_logs else ""
                    )
                    logging.info(f"Log dump cycle {log_cycles} complete{no_logs_msg}")
                    log_cycles += 1

            time.sleep(self.sleep_time)
            total_time += self.sleep_time
            job = self.get()

    def delete(self):
        name, namespace = self._get_name_namespace()
        # Verify exists before delete
        job = self.get()
        if job:
            self.kube_job_client.delete_namespaced_job(
                name=name, namespace=namespace, propagation_policy="Foreground"
            )
        return True
