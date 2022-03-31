PYTHON=python3
PIP=$(PYTHON) -m pip

all: install

debug-training-all:
	python3 -m distributed_notebook.demo distributed_notebook/demo/script/training.py distributed_notebook/demo/script/training1.py

debug-training:
	python3 -m distributed_notebook.demo distributed_notebook/demo/script/training.py

debug-training1:
	python3 -m distributed_notebook.demo --resume distributed_notebook/demo/script/training1.py

python-demo-all:
	python3 -m distributed_notebook.demo distributed_notebook/demo/script/script.py distributed_notebook/demo/script/script2.py

python-demo-step1:
	python3 -m distributed_notebook.demo distributed_notebook/demo/script/script.py

python-demo-step2:
	python3 -m distributed_notebook.demo --resume distributed_notebook/demo/script/script2.py

python-demo-step3:
	python3 -m distributed_notebook.demo --resume distributed_notebook/demo/script/script3.py

install:
	# this does a local install of the package, building the sdist and then directly installing it
	rm -rf dist build */*.egg-info *.egg-info
	$(PYTHON) setup.py sdist
	$(PIP) install dist/*.tar.gz
	$(PYTHON) -m distributed_notebook.kernel.install