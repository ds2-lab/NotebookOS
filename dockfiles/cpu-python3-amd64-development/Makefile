include ../env.mk

all: build

build:
	docker build ${PARAMS} -t $(DOCKER_USER)jupyter-cpu-dev -f Dockerfile ../..
# ../clean.sh

build-and-push: build
	docker push $(DOCKER_USER)jupyter-cpu-dev:latest

clean:
	docker builder prune