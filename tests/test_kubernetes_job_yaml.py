import unittest
import yaml
import unittest
import pathlib
from airflow_kjo.kubernetes_job_launcher import KubernetesJobYaml

class TestKubernetesJobYaml(unittest.TestCase):

    def setUp(self) -> None:
        job_yaml_path = str(pathlib.Path(__file__).parent.absolute()) + '/fixtures/test_valid_job.yaml'
        with open(job_yaml_path, 'r') as infile:
            job_yaml = infile.read()
        self.job_yaml = yaml.safe_load(job_yaml)

    def test_valid_job_yaml(self):
        kjy = KubernetesJobYaml(self.job_yaml)
        assert kjy._validate_job_yaml(self.job_yaml) == True

    def test_expanded_job_yaml(self):
        expected_fields = [("parallelism", "parallelism"), ("backoff_limit", "backoffLimit")]
        expected_values = [3, 5]
        expectations = zip(expected_fields, expected_values)
        for exp_field_tuple, exp_value in expectations:
            given_field, kube_field = exp_field_tuple
            expansion = {given_field: exp_value}
            kjy = KubernetesJobYaml(self.job_yaml, expansion)
            assert kjy.yaml["spec"][kube_field] == exp_value

    def test_didnt_overwrite_expanded_job_yaml(self):
        ## add some fields we dont expect to overwrite
        my_initial_value = 5
        my_overwrite_value = 10
        self.job_yaml["spec"]["parallelism"] = my_initial_value
        expansion = {"parallelism": my_overwrite_value}
        kjy = KubernetesJobYaml(self.job_yaml, expansion)
        assert kjy.yaml["spec"]["parallelism"] == my_initial_value

    def test_overwrote_expanded_job_yaml(self):
        my_initial_value = 5
        my_overwrite_value = 10
        self.job_yaml["spec"]["parallelism"] = my_initial_value
        expansion = {"parallelism": my_overwrite_value}
        kjy = KubernetesJobYaml(self.job_yaml, expansion, overwrite=True)
        assert kjy.yaml["spec"]["parallelism"] == my_overwrite_value

