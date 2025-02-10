include ../env.mk

all: build

build:
	docker build ${PARAMS} -t $(DOCKER_USER)jupyter-gpu -f Dockerfile ../..
# ../clean.sh

build-and-push: build
	docker push $(DOCKER_USER)jupyter-gpu:latest

clean:
	docker builder prune