include ../env.mk

all: build

build:
	docker build ${PARAMS} -t $(DOCKER_USER)jupyter-debug -f Dockerfile ../..
# ../clean.sh

clean:
	docker builder prune