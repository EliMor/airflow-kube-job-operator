import time
import tenacity
from kubernetes.client.rest import ApiException

# https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/BatchV1Api.md

from airflow_kjo.kubernetes_util import get_kube_client, get_kube_job_client


class KubernetesJobLauncherPodError(Exception):
    """
    Created Job ended in an errored pod state
    """

    pass


class KubeYamlValidationError(Exception):
    pass


class KubernetesJobLauncher:
    def __init__(
        self, kube_client=None, in_cluster=True, cluster_context=None, config_file=None
    ):
        self.kube_client = kube_client or get_kube_client(
            in_cluster=in_cluster,
            cluster_context=cluster_context,
            config_file=config_file,
        )

        self.kube_job_client = get_kube_job_client(self.kube_client)
        self.sleep_time = 5

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
        while True:
            job = self.kube_job_client.read_namespaced_job_status(
                name=name, namespace=namespace
            )
            completed = bool(job.status.succeeded)
            if completed:
                return True
            if running_timeout and total_time > running_timeout:
                pass  # running timeout exceeded, probably just a warning, would allow task to continue

            if bool(job.status.failed):
                raise KubernetesJobLauncherPodError(
                    f"Job {name} in Namespace {namespace} ended in Error state"
                )

            time.sleep(self.sleep_time)
            total_time += self.sleep_time

    def delete(self, yaml_obj):
        name, namespace = self._get_name_namespace(yaml_obj)
        self.kube_job_client.delete_namespaced_job(
            name=name, namespace=namespace, propagation_policy="Foreground"
        )
        return True
