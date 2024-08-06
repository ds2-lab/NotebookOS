include ../env.mk

all: build

build: 
	docker build ${PARAMS} -t $(DOCKER_USER)dist-notebook-base \
		--build-arg USER_ID=$(shell id -u) \
		--build-arg GROUP_ID=$(shell id -g) .

clean:
	docker builder prune