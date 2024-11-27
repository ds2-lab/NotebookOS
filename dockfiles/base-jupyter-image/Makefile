include ../env.mk

all: build

build:
	docker build ${PARAMS} -t $(DOCKER_USER)base-jupyter-image-python3.12.6 -f Dockerfile ../..
# ../clean.sh

clean:
	docker builder prune