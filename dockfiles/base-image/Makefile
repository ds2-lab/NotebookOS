include ../env.mk

all: build

build: 
	docker build ${PARAMS} -t $(DOCKER_USER)dist-notebook-base \
		--build-arg USER_ID=$(shell id -u) \
		--build-arg GROUP_ID=$(shell id -g) \
		--build-arg DOCKER_GROUP_ID=$(shell getent group docker | cut -d: -f3) .

clean:
	docker builder prune