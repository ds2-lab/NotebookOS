#!/bin/bash

PYTHON=/home/jovyan/Python-3.11.9/debug/python

rm -rf dist build */*.egg-info *.egg-info
# ${PYTHON} setup.py sdist
# ${PYTHON} -m pip install dist/*.tar.gz
${PYTHON} -m pip install -e .
${PYTHON} -m distributed_notebook.kernel.install