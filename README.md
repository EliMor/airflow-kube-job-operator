# Airflow Kubernetes Job Operator

## What is this?
An Airflow Operator that manages creation, watching, and deletion of a Kubernetes Job. It assumes the client passes in a path to a yaml file that may have Jinja templated fields. 

## Who is it for?
This package makes the assumption that you're using Kubernetes somehow. Airflow itself may be deployed in Kubernetes (in_cluster mode) or you may just want it to manage Jobs running remotely on a cluster (give Airflow a kube config).

## Why would I use this?
In our use of Airflow we struggled a lot with binding our business logic via many different custom Operators and Plugins directly to Airflow. Instead, we found Airflow to be a great manager of execution of code but not the best tool for writing the ETL/ML code itself. 

Ideally this should be one of the only Airflow Operators you need.

## How do I use it?

Here are the parameters.

| Parameter | Description | Type    |	
| --------- | ----------- | ------- |
| yaml_file_name | The name of the yaml file, could be a full path | str |
| yaml_write_path | If you want the rendered yaml file written, where should it be? | str |
| yaml_write_filename | If you want the rendered yaml file written, what is the filename? | str |
| yaml_template_fields | If you have variables in your yaml file you want filled out | dict 
| tail_logs | Whether to output a log tail of the pods to airflow, will only do it at an end state | bool (F) |
| tail_logs_every | every x seconds to wait to begin a new log dump (nearest 5 sec) | int |
| tail_logs_line_count | num of lines from end to output | int |
| log_yaml | Whether to log the rendered yaml | bool (T) |
| in_cluster | Whether or not Airflow has cluster permissions to create and manage Jobs | bool (F) |
| config_file | The path to the kube configfile | str |
| cluster_context | If you are using a kube config file include the cluster context | str |
| delete_completed_job | Autodelete Jobs that completed without errors | bool (F) |

### Step 1. Install the package
```sh
pip install airflow-kube-job-operator
```

#### Step 1.5 (Optional) Add Role to your Airflow deployment
If you want the Jobs to get created without having to bundle your kubeconfig file somehow into your Airflow pods, you'll need to deploy Airflow in kubernetes and give Airflow some extra RBAC permissions to handle Jobs within your cluster.

** This is needed if you want to use the option ```in_cluster=True``` **

Here's an example of what you may need
```yaml
kind: Role
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: airflow
rules:
  - verbs:
      - create
      - list
      - get
      - watch
      - delete
      - update
      - patch
    apiGroups:
      - ''
      - batch
    resources:
      - pods
      - jobs
      - jobs/status
  - verbs:
      - get
    apiGroups:
      - ''
    resources:
      - pods/log
  - verbs:
      - create
      - get
    apiGroups:
      - ''
    resources:
      - pods/exec

```
If you want to give Airflow power to run Jobs Cluster-wide modify the ClusterRole instead.

Alternatively, just give Airflow your kube cluster config. (A.ii.)

### Step 2. Create a template folder for your yaml files
This template folder can be anywhere. It's up to you. But here's a suggestion.

If you have...

```
~/airflow/dags
```
then
```
~/airflow/kubernetes/job
```
Could be a valid choice. 

Lets create a very simple job and put it there.
```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: countdown
  namespace: <WRITE YOUR AIRFLOW NAMESPACE HERE>
spec:
  template:
    metadata:
      name: countdown
    spec:
      containers:
      - name: counter
        image: centos:7
        command:
         - "bin/bash"
         - "-c"
         - "for i in 9 8 7 6 5 4 3 2 1 ; do echo $i ; done"
      restartPolicy: Never
```
Save the above at
```
~/airflow/kubernetes/job/countdown.yaml
```


### Step 3. Create your Dag

First some questions to ask yourself...

    A. How do I want my Dag to have access to kubernetes? 
        i. My Airflow has the above RBAC permissions to make Jobs
        ii. I rather just use my kube config file. It's accessible somewhere in Airflow already (web, worker, and scheduler)

    B. What does my yaml look like? 
        i. I have a simple yaml file. Just create my Job. (The yaml, 'countdown.yaml' above is like this)
        ii. I have a single yaml file for my Job but I want some templated fields filled out. 
        iii. I'm hardcore. I have multiple yaml files templated in the Jinja style so I can reuse my templates across tasks and dags. 


#### **A.i. Using ```in_cluster=True```**

```python
from airflow import DAG
from datetime import datetime, timedelta
from airflow_kjo import KubernetesJobOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, # the number of times the pod will retry, can pass in per-task
    'retry_delay': timedelta(minutes=5), 
    'start_date': datetime(2021, 2, 24, 12, 0),
}
with DAG(
    'kubernetes_job_operator',
    default_args=default_args,
    description='KJO example DAG',
    schedule_interval=None,
    catchup=False
) as dag:
    task_1 = KubernetesJobOperator(task_id='example_kubernetes_job_operator',
                                   yaml_file_name='/path/to/airflow/kubernetes/job/countdown.yaml',
                                   in_cluster=True)
```

#### **A.ii. Using ```config_file=/path/to/.kube/config```**
```python
    task_1 = KubernetesJobOperator(task_id='example_kubernetes_job_operator',
                                   yaml_file_name='/path/to/airflow/kubernetes/job/countdown.yaml',
                                   config_file='/path/to/.kube/config',
                                   cluster_context='my_kube_config_context')
```
What is this "my_kube_config_context" business? 
Read about it in the kubernetes config documentation [here](https://kubernetes.io/docs/concepts/configuration/organize-cluster-access-kubeconfig/)


#### **B.i. Simple yaml file execution**
In addition to the above Dag styles you could also make use of Airflow's native ```template_searchpath``` field to clean up the Dag a bit.

```python
from airflow import DAG
from datetime import datetime, timedelta
from airflow_kjo import KubernetesJobOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 2, 24, 12, 0),
}
with DAG(
    'kubernetes_job_operator',
    default_args=default_args,
    description='KJO example DAG',
    schedule_interval=None,
    template_searchpath='/path/to/airflow/kubernetes/job'
    catchup=False
) as dag:
    task_1 = KubernetesJobOperator(task_id='example_kubernetes_job_operator',
                                   yaml_file_name='countdown.yaml',
                                   in_cluster=True)
```


#### **B.ii. Simple yaml templating**
Let's make the yaml a little more interesting.
```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: countdown-templated-{{task_num}}
  namespace: <WRITE YOUR AIRFLOW NAMESPACE HERE>
spec:
  template:
    metadata:
      name: countdown
    spec:
      containers:
      - name: counter
        image: centos:7
        command:
         - "bin/bash"
         - "-c"
         - "{{command}}"
      restartPolicy: Never
```
And save this as
```~/airflow/kubernetes/job/countdown.yaml.tmpl```

We now have the fields ```command``` and ```task_num``` as variables in our yaml file. 
Here's how our Dag looks now...

```python
with DAG(
    'kubernetes_job_operator',
    default_args=default_args,
    description='KJO example DAG',
    schedule_interval=None,
    template_searchpath='/path/to/airflow/kubernetes/job'
    catchup=False
) as dag:
    command = 'sleep 60; for i in 5 4 3 2 1 ; do echo $i ; done'
    task_num = 1
    task_1 = KubernetesJobOperator(task_id='example_kubernetes_job_operator',
                                   yaml_file_name='countdown.yaml.tmpl',
                                   yaml_template_fields={'command': command, 'task_num':task_num},
                                   in_cluster=True)
```


#### **B.iii. Multiple yaml templates**
This is very much up to you how you want your Jinja templates separated, if its valid yaml and valid Jinja, it will render and apply just fine...

Heres an example use case.
1. Create a 'header' template at ```~/airflow/kubernetes/job/countdown_header.yaml.tmpl```
   ```yaml
    apiVersion: batch/v1
    kind: Job
    metadata:
      name: countdown-templated-separated
      namespace: <WRITE YOUR AIRFLOW NAMESPACE HERE>
    {% block spec %}{% endblock %}
   ```
2. Create a 'body' template at ```~/airflow/kubernetes/job/countdown_body.yaml.tmpl```
    ```yaml
    {% extends 'countdown_header.yaml.tmpl' %}
    {% block spec %}
    spec:
    template:
        metadata:
        name: countdown
        spec:
        containers:
        - name: counter
            image: centos:7
            command:
            - "bin/bash"
            - "-c"
            - "{{command}}"
        restartPolicy: Never
    {% endblock %}
    ```

Here's the Dag changes now
```python
    task_1 = KubernetesJobOperator(task_id='example_kubernetes_job_operator',
                                   yaml_file_name='countdown_body.yaml.tmpl',
                                   yaml_template_fields={'command': command},
                                   in_cluster=True)
```

In this situation it may be useful to have Airflow write out the rendered yaml file somewhere.

```python
    task_1 = KubernetesJobOperator(task_id='example_kubernetes_job_operator',
                                   yaml_file_name='countdown_body.yaml.tmpl',
                                   yaml_template_fields={'command': command},
                                   yaml_write_path='/tmp',
                                   yaml_write_filename='rendered.yaml', # will be on the worker pod
                                   in_cluster=True)
```
It could be very useful to have an NFS to share the same filestore across pods for writing these rendered yaml files out. 

## Logging

If you're using Kubernetes you should have a logging solution of some sort to aggregate and provide searchability of all your logs. However, here are some use cases for forwarding the logs using the KJO.

    1. I just want a simple tail of the logs, I don't care about extra behavior configuration
    2. I only want logs tailed out when the pods are in an end state; Completed, Errored
    3. I want to specify how many lines are tailed out and/or how frequently its tailed out 


1. Add 'tail_logs' to our task from above.
```python
    task_1 = KubernetesJobOperator(task_id='example_kubernetes_job_operator',
                                   yaml_file_name='countdown_body.yaml.tmpl',
                                   yaml_template_fields={'command': command},
                                   in_cluster=True,
                                   tail_logs=True)
```

If any tail_logs* parameter is set, 'tail_logs' does not need to be set.

2. Configure the behavior of the log tail
```python
    task_1 = KubernetesJobOperator(task_id='example_kubernetes_job_operator',
                                   yaml_file_name='countdown_body.yaml.tmpl',
                                   yaml_template_fields={'command': command},
                                   in_cluster=True,
                                   tail_logs_every=60, # seconds
                                   tail_logs_line_count=100)
```

This could get to be quite noisy so be mindful of your particular use case.

## Notes....

-  We need to think about how to add PVC support. If a client's Task relies on a PVC being created, they need a way to add it to their DAG and have it created and deleted as a part of the Job flow. Maybe a KubernetesPVCOperator is better than a parameter solution.

## Contributing

- This is a young project and not yet battle tested. Contributions, suggestions, etc. appreciated. 
