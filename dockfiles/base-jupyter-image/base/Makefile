include ../env.mk

all: build

docker:
	docker build ${PARAMS} -t $(DOCKER_USER)generic-base-jupyter-image:python-3.12.6 -f Dockerfile ../../..

build: docker

clean:
	docker builder prune