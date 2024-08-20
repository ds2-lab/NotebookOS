include ../env.mk

all: build

build:
	docker build ${PARAMS} -t $(DOCKER_USER)base-jupyter-image -f Dockerfile ../..
# ../clean.sh

clean:
	docker builder prune