#!/bin/bash

PYTHON=python3

rm -rf dist build */*.egg-info *.egg-info
${PYTHON} setup.py sdist
${PYTHON} -m pip install dist/*.tar.gz
${PYTHON} -m distributed_notebook.kernel.install