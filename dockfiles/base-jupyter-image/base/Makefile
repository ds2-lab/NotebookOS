include ../env.mk

all: build

update-pytorch-dependency:
	update_torch_dependency.sh

gen-requirements:
	python3 generate_requirements_txt.py
	update_torch_dependency.sh

docker: gen-requirements update-pytorch-dependency
	docker build ${PARAMS} -t $(DOCKER_USER)generic-base-jupyter-image:python-3.12.6 -f Dockerfile ../..

build: docker

clean:
	docker builder prune