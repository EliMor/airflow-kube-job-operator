import time
import logging
import tenacity
from kubernetes.client.rest import ApiException

# https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/BatchV1Api.md

from airflow_kjo.kubernetes_util import get_kube_client, get_kube_job_client, get_kube_pod_client

class KubernetesJobLauncherPodError(Exception):
    """
    Created Job ended in an errored pod state
    """
    pass


class KubeYamlValidationError(Exception):
    pass


class KubernetesJobLauncher:
    def __init__(
        self, kube_client=None, in_cluster=True, cluster_context=None, config_file=None,
        tail_logs=False, tail_log_line_count=100
    ):
        self.kube_client = kube_client or get_kube_client(
            in_cluster=in_cluster,
            cluster_context=cluster_context,
            config_file=config_file,
        )

        self.kube_job_client = get_kube_job_client(self.kube_client)
        self.kube_pod_client = get_kube_pod_client(self.kube_client)
        self.sleep_time = 5
        self.tail_logs = tail_logs
        self.tail_logs_every = self.sleep_time*6
        self.tail_log_line_count = tail_log_line_count

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

    def _get_name_namespace(self, yaml_obj):
        self._validate_job_yaml(yaml_obj)
        return yaml_obj["metadata"]["name"], yaml_obj["metadata"]["namespace"]

    def _tail_pod_logs(self, name, namespace, job):
        num_lines = self.tail_log_line_count
        # can only get a log if pod is in one of these states
        logable_statuses = {'Running', 'Failed', 'Succeeded'}
        # get all pods for the job
        job_pods = self.kube_pod_client.list_namespaced_pod(namespace=namespace, label_selector=f'job-name={name}')
        for pod in job_pods.items: 
            pod_name = pod.metadata.name
            # only continue if pod is running, completed or errored
            pod_phase = pod.status.phase
            if pod_phase not in logable_statuses:
                continue
            # output the tail of each pod log
            line_or_lines = 'line' if num_lines == 1 else 'lines'
            # TODO should see if can use since_seconds in a good way
            # https://raw.githubusercontent.com/kubernetes-client/python/master/kubernetes/client/api/core_v1_api.py
            lines = [line for line in self.kube_pod_client.read_namespaced_pod_log(name=pod_name, namespace=namespace, tail_lines=num_lines)]
            str_lines = ''.join(lines).strip()
            if str_lines:
                logging.info(f'Reading last {num_lines} {line_or_lines} from log for pod {pod_name} in namespace {namespace}')
                logging.info(f'Reading....\n{str_lines}')
                logging.info(f'End log for {pod_name} in namespace {namespace}')

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_random_exponential(),
        reraise=True,
    )
    def apply(self, yaml_obj):
        self._validate_job_yaml(yaml_obj)
        _, namespace = self._get_name_namespace(yaml_obj)
        try:
            self.kube_job_client.create_namespaced_job(
                namespace=namespace, body=yaml_obj
            )
        except ApiException as error:
            return error.status == 409
        return True

    def watch(self, yaml_obj, running_timeout=None):
        name, namespace = self._get_name_namespace(yaml_obj)
        total_time = 0
        log_cycles = 1
        while True:
            job = self.kube_job_client.read_namespaced_job_status(
                name=name, namespace=namespace
            )
            completed = bool(job.status.succeeded)
            if completed:
                if self.tail_logs:
                    logging.info(f'Final tail of log for Job {name}')
                    self._tail_pod_logs(name, namespace, job)
                logging.info(f'Job {name} status is Completed')
                return True
            if running_timeout and total_time > running_timeout:
                pass  # running timeout exceeded, probably just a warning, would allow task to continue

            if bool(job.status.failed):
                if self.tail_logs:
                    self._tail_pod_logs(name, namespace, job)
                raise KubernetesJobLauncherPodError(
                    f"Job {name} in Namespace {namespace} ended in Error state"
                )
            if self.tail_logs:
                if total_time > 0 and total_time % self.tail_logs_every == 0:
                    logging.info(f'Beginning new log dump cycle :: {log_cycles}')
                    self._tail_pod_logs(name, namespace, job)
                    logging.info(f'Log dump cycle {log_cycles} complete')
                    log_cycles += 1

            time.sleep(self.sleep_time)
            total_time += self.sleep_time

    def delete(self, yaml_obj):
        name, namespace = self._get_name_namespace(yaml_obj)
        self.kube_job_client.delete_namespaced_job(
            name=name, namespace=namespace, propagation_policy="Foreground"
        )
        return True
