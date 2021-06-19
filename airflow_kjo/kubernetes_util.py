from kubernetes import client as k_client
from kubernetes import config as k_config

# https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/BatchV1Api.md

from airflow.kubernetes.kube_client import _enable_tcp_keepalive
from airflow.configuration import conf


def get_kube_client(in_cluster=False, cluster_context=None, config_file=None):
    if conf.getboolean("kubernetes", "enable_tcp_keepalive"):
        _enable_tcp_keepalive()

    configuration = k_client.Configuration()
    configuration.verify_ssl = False
    k_client.Configuration.set_default(configuration)

    client_config = None
    if not in_cluster:
        if cluster_context is None:
            cluster_context = conf.get("kubernetes", "cluster_context", fallback=None)
        if config_file is None:
            config_file = conf.get("kubernetes", "config_file", fallback=None)

        from airflow.kubernetes.refresh_config import (  # pylint: disable=ungrouped-imports
            RefreshConfiguration,
            load_kube_config,
        )

        client_config = RefreshConfiguration()
        load_kube_config(
            client_configuration=client_config,
            config_file=config_file,
            context=cluster_context,
        )
    else:
        k_config.load_incluster_config()

    kube_client = k_client.ApiClient(configuration=client_config)
    return kube_client


def get_kube_job_client(kube_client):
    batch_api = k_client.BatchV1Api(api_client=kube_client)
    return batch_api


def get_kube_pod_client(kube_client):
    core_api = k_client.CoreV1Api(api_client=kube_client)
    return core_api
