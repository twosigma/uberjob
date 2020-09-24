<img src="./docs/_static/logo/logo-128.png" align="right">

# uberjob

[![PyPI Status](https://img.shields.io/pypi/v/uberjob.svg)](https://pypi.python.org/pypi/uberjob)
![Tests](https://github.com/twosigma/uberjob/workflows/Tests/badge.svg)
[![Documentation Status](https://readthedocs.org/projects/uberjob/badge/?version=latest)](https://uberjob.readthedocs.io/en/latest/?badge=latest)
[![Codecov](https://codecov.io/gh/twosigma/uberjob/branch/main/graph/badge.svg)](https://codecov.io/gh/twosigma/uberjob)


uberjob is a Python package for building and running call graphs.

# Documentation

https://uberjob.readthedocs.io/

# Installation

    pip install uberjob

# Development

This repository uses
[Poetry](https://python-poetry.org/) and
[Nox](https://nox.thea.codes/en/stable/)
to manage the development environment and builds.

To list all Nox sessions:

    python -m nox --list-sessions

To run the black code formatter:

    python -m nox -rs black

To lint using flake8:

    python -m nox -rs lint

To run the test suite:

    python -m nox -rs tests

To build the documentation:

    python -m nox -rs docs
