include ../env.mk

all: build

build:
	docker build ${PARAMS} -t $(DOCKER_USER)jupyter-cpu -f Dockerfile ../..
# ../clean.sh

build-and-push: docker
	docker push $(DOCKER_USER)jupyter-cpu:latest

clean:
	docker builder prune