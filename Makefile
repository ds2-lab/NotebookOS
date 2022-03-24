PYTHON=python3
PIP=$(PYTHON) -m pip

all: install

debug-training:
	python3 -m distributed-notebook.demo distributed-notebook/demo/script/training.py

python-demo-all:
	python3 -m distributed-notebook.demo distributed-notebook/demo/script/script.py distributed-notebook/demo/script/script2.py

python-demo-step1:
	python3 -m distributed-notebook.demo distributed-notebook/demo/script/script.py

python-demo-step2:
	python3 -m distributed-notebook.demo --resume distributed-notebook/demo/script/script2.py

python-demo-step3:
	python3 -m distributed-notebook.demo --resume distributed-notebook/demo/script/script3.py

install:
	# this does a local install of the package, building the sdist and then directly installing it
	rm -rf dist build */*.egg-info *.egg-info
	$(PYTHON) setup.py sdist
	$(PIP) install dist/*.tar.gz
	$(PYTHON) -m distributed-notebook.kernel.install