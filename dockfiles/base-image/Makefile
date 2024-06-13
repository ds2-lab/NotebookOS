include ../env.mk

all: build

build: 
	docker build --progress=plain ${PARAMS} -t $(DOCKER_USER)dist-notebook-base .

clean:
	docker builder prune