include ../env.mk

VERSION = "0.0.1"

all: build

build-scheduler-extender-linux:
	GOOS=linux go build -buildvcs=false -ldflags "-s -w -X main.version=${VERSION}" -o scheduler_extender ../../scheduler_extender 
	chmod 777 scheduler_extender

build: build-scheduler-extender-linux
	docker build ${PARAMS} -t $(DOCKER_USER)scheduler_extender .
	rm scheduler_extender

clean:
	docker builder prune