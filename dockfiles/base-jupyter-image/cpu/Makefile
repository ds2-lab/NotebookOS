include ../../env.mk

all: build

update-pytorch-dependency:
	./update_torch_dependency.sh

gen-requirements-file:
	python3 generate_requirements_txt.py

gen-requirements: gen-requirements-file update-pytorch-dependency

docker: gen-requirements
	docker build ${PARAMS} -t $(DOCKER_USER)base-jupyter-image:python3.12.6-cpu -f Dockerfile ../../..

build: docker

clean:
	docker builder prune