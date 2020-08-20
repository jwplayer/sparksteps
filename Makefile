.PHONY: clean-pyc clean-build docs clean build install install-all version

help:
	@echo "clean-build - remove build artifacts"
	@echo "clean-test - remove Python file artifacts"
	@echo "clean-eggs - remove cached eggs"
	@echo "build - build package"
	@echo "lint - check style with flake8"
	@echo "test - run tests quickly with the default Python"
	@echo "test-all - run tests on every Python version with tox"
	@echo "docs - generate Sphinx HTML documentation, including API docs"
	@echo "release - package and upload a release"
	@echo "dist - package"

clean: clean-build clean-test clean-eggs
	rm -rf htmlcov/

clean-build:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info

.PHONY: clean-test
clean-test:
	find . | grep -E "(__pycache__|\.pyc|\.pyo$$)" | xargs rm -rf
	rm -rf .pytest_cache/

.PHONY: clean-eggs
clean-eggs:
	rm -rf .eggs/

.PHONY: build
build: clean-build clean-eggs
	python3 setup.py build_ext --inplace

install: clean-build
	python3 setup.py install

install-all:
	pip install -e .[all]

lint:
	pytest --flake8 sparksteps tests

test:
	python3 setup.py test

test-all:
	tox

version:
	python setup.py --version

docs:
	rm -f docs/sparksteps.rst
	rm -f docs/modules.rst
	sphinx-apidoc -o docs/ sparksteps
	$(MAKE) -C docs clean
	$(MAKE) -C docs html
	xdg-open docs/_build/html/index.html

.PHONY: release
release: clean build
	python3 setup.py sdist bdist_wheel
	twine check dist/*
	twine upload --verbose dist/*

.PHONY: dist
dist: clean build
	python3 setup.py sdist bdist_wheel
	twine check dist/*
