from os import path
from setuptools import setup, find_packages

with open("./airflow_kjo/__version__", "r") as f:
    version = f.readline()

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()
    
requirements = [
    "apache-airflow>=2.0.0",
    "kubernetes>=11.0.0",
    "Jinja2>=2.11.3"
]

requirements_tests = [
    "pytest"
]

setup(
    name="airflow-kube-job-operator",
    version=version,
    author="EliMor",
    author_email="elimor.github@gmail.com",
    description="Kubernetes job operator for Airflow",
    long_description=long_description,
    long_description_content_type='text/markdown',
    license="Apache License 2.0",
    packages=['airflow_kjo'],
    include_package_data=True,
    install_requires=requirements,
    extras_require={'tests': requirements_tests},
    test_suite="tests",
    tests_require=requirements_tests,
    classifiers=[
        # Supported Python versions
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        # License
        "License :: OSI Approved :: Apache Software License",
    ]
)