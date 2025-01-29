include ../../env.mk

all: build

gen-requirements:
	python3 generate_requirements_txt.py

docker: gen-requirements
	docker build ${PARAMS} -t $(DOCKER_USER)base-jupyter-image:python3.12.6-cuda -f Dockerfile ../../..

build: docker

clean:
	docker builder prune