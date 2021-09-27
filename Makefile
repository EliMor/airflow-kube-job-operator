VERSION=$(shell head -n 1 airflow_kjo/__version__ | sed 's/v//')

clean:
	rm -r *egg-info || true
	rm -r build || true
	rm -r dist || true

build:
	python3 setup.py sdist bdist_wheel

push:
	#package_cloud push verdigris/airflow dist/*${VERSION}.tar.gz
	twine upload dist/*

test:
	pytest tests

format:
	black airflow_kjo