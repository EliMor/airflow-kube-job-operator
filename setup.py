from setuptools import setup, find_packages

with open("./airflow_kjo/__version__", "r") as f:
    version = f.readline()

requirements = [
    "apache-airflow>=2.0.0",
    "kubernetes>=11.0.0",
    "Jinja2>=2.11.3"
]

requirements_tests = [
    "pytest"
]

setup(
    name="airflow-kubernetes-job-operator",
    version=version,
    author="EliMor",
    author_email="elimor.github@gmail.com",
    description="Kubernetes job operator for Airflow",
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

        # License
        "License :: OSI Approved :: Apache Software License",
    ]
)