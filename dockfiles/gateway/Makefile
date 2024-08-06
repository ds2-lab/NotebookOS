include ../env.mk

all: docker

# This actually builds the executable.
# Ideally, you should use the 'build-in-container' (or 'docker') recipes,
# as these build the executable within the base image, so there are no glibc errors.
build-gateway-linux:
	GOOS=linux go build -buildvcs=false -o gateway ../../gateway 

# This moves to the root project directory and executes `make build-gateway`
# This mounts the project within the 'base image' docker container and builds the gateway there.
# The process of building the gateway calls `make build-gateway-linux` in this directory.
build-in-container:
	cd ../../ && make build-gateway 

docker: build-in-container
	docker build ${PARAMS} -t $(DOCKER_USER)gateway .
	rm gateway
# ../clean.sh

clean:
	docker builder prune