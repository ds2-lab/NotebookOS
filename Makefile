PYTHON=python3.12
PIP=$(PYTHON) -m pip
DOCKER_USER = scusemua/

_GOPATH=$(shell go env GOPATH)

all: build

replica:
	$(PYTHON) -X faulthandler -m distributed_notebook.demo_replica $(PARAMS)

debug-training-all:
	 $(PYTHON) -m distributed_notebook.demo distributed_notebook/demo/script/training.py distributed_notebook/demo/script/training1.py

debug-training:
	 $(PYTHON) -X faulthandler -m distributed_notebook.demo $(PARAMS) distributed_notebook/demo/script/training.py

debug-training1:
	 $(PYTHON) -X faulthandler -m distributed_notebook.demo --resume $(PARAMS) distributed_notebook/demo/script/training1.py

python-demo-all:
	 $(PYTHON) -m distributed_notebook.demo distributed_notebook/demo/script/script.py distributed_notebook/demo/script/script2.py

python-demo-step1:
	 $(PYTHON) -m distributed_notebook.demo distributed_notebook/demo/script/script.py

python-demo-step2:
	 $(PYTHON) -m distributed_notebook.demo --resume distributed_notebook/demo/script/script2.py

python-demo-step3:
	 $(PYTHON) -m distributed_notebook.demo --resume distributed_notebook/demo/script/script3.py

build: build-darwin

build-darwin:
	cd smr && make build-darwin

build-smr-linux-arm64:
	docker run -it --rm -v `pwd`:/go/src/in -v `pwd`:/out scusemua/gopy /bin/bash -c "cd /go/src/in/distributed_notebook && make build-smr-linux-arm64"

build-smr-linux-amd64:
	docker run -it --rm -v `pwd`:/go/src/in -v `pwd`:/out scusemua/gopy /bin/bash -c "cd /go/src/in/distributed_notebook && make build-smr-linux-amd64"

build-gateway:
	@echo "Building 'gateway' component within base docker image '$(DOCKER_USER)dist-notebook-base:latest'"
	docker run -it --rm -v $(shell go env GOPATH)/pkg/zmq4:/go/pkg/zmq4 \
						-v `pwd`:/go/pkg/distributed_notebook \
						-v `pwd`:/out \
						-v "$(shell go env GOCACHE)":/go/.cache \
						-e GOCACHE=/go/.cache \
						$(DOCKER_USER)dist-notebook-base:latest /bin/bash -c "cd /go/pkg/distributed_notebook/dockfiles/gateway && time make build-gateway-linux"

build-gateway-new:
	@echo "Building 'gateway' component within base docker image '$(DOCKER_USER)dist-notebook-base:latest'"
	docker run -it --rm -v $(shell go env GOPATH)/pkg/zmq4:/go/pkg/zmq4 \
						-v `pwd`:/go/pkg/distributed_notebook \
						-v `pwd`:/out \
						-v "$(shell go env GOCACHE)":/go/.cache \
						-e GOCACHE=/go/.cache \
						$(DOCKER_USER)dist-notebook-base:latest /bin/bash -c "cd /go/pkg/distributed_notebook/dockfiles/gateway && time make build-gateway-linux-new"

build-remote-docker-event-forwarder:
	@echo "Building 'remote docker event forwarder' component within base docker image '$(DOCKER_USER)dist-notebook-base:latest'"
	docker run -it --rm -v $(shell go env GOPATH)/pkg/zmq4:/go/pkg/zmq4 \
						-v `pwd`:/go/pkg/distributed_notebook \
						-v `pwd`:/out \
						-v "$(shell go env GOCACHE)":/go/.cache \
						-e GOCACHE=/go/.cache \
						$(DOCKER_USER)dist-notebook-base:latest /bin/bash -c "cd /go/pkg/distributed_notebook/common/docker_events && time make build-linux"

gateway: build-gateway 

build-local_daemon:
	@echo "Building 'local daemon' component within base docker image '$(DOCKER_USER)dist-notebook-base:latest'"
	docker run -it --rm -v $(shell go env GOPATH)/pkg/zmq4:/go/pkg/zmq4 \
					    -v `pwd`:/go/pkg/distributed_notebook \
						-v `pwd`:/out \
						-v "$(shell go env GOCACHE)":/go/.cache \
						-e GOCACHE=/go/.cache \
						$(DOCKER_USER)dist-notebook-base:latest /bin/bash -c "cd /go/pkg/distributed_notebook/dockfiles/local_daemon && time make build-local-daemon-linux"

local-daemon: build-local_daemon 
local_daemon: build-local_daemon 

install-kernel:
	./install_kernel.sh

build-grpc-go:
	protoc --go_out=. --go_opt=paths=source_relative \
  	--go-grpc_out=. --go-grpc_opt=paths=source_relative \
    common/proto/gateway.proto --experimental_allow_proto3_optional

# protoc --go_out=. --go_opt=paths=source_relative \
# --go-grpc_out=. --go-grpc_opt=paths=source_relative \
# common/driver/driver.proto 

build-grpc-python:
	 $(PYTHON) -m grpc_tools.protoc -Icommon/proto \
  	--python_out=distributed_notebook/gateway \
		--grpc_python_out=distributed_notebook/gateway \
    common/proto/gateway.proto
ifeq ($(shell uname -p), x86_64)
	@sed -i -E 's/import gateway_pb2 as gateway__pb2/from . import gateway_pb2 as gateway__pb2/g' distributed_notebook/gateway/gateway_pb2_grpc.py
else
	@sed -Ei '' 's/import gateway_pb2 as gateway__pb2/from . import gateway_pb2 as gateway__pb2/g' distributed_notebook/gateway/gateway_pb2_grpc.py
endif 

# We probably don't need the Python gRPC bindings.
# 	 $(PYTHON) -m grpc_tools.protoc -Icommon/driver \
#   	--python_out=distributed_notebook/driver \
# 		--grpc_python_out=distributed_notebook/driver \
#     common/driver/driver.proto
# ifeq ($(shell uname -p), x86_64)
# 	@sed -i -E 's/import driver_pb2 as driver__pb2/from . import driver_pb2 as driver__pb2/g' distributed_notebook/driver/driver_pb2_grpc.py
# else
# 	@sed -Ei '' 's/import driver_pb2 as driver__pb2/from . import driver_pb2 as driver__pb2/g' distributed_notebook/driver/driver_pb2_grpc.py
# endif 

build-grpc: build-grpc-go build-grpc-python

# build-gateway:
# 	go build -o bin/gateway ./gateway

# build-local-daemon:
# 	go build -o bin/local_daemon ./local_daemon

build-scheduler-extender:
	go build -o bin/scheduler_extender ./scheduler_extender

# gateway: build-gateway
# 	bin/gateway

# local_daemon: build-local-daemon
# 	bin/local_daemon
	
# scheduler-extender: build-scheduler-extender
# 	bin/scheduler_extender

test:
	cd distributed_notebook && pytest

mock:
	mockgen -source=common/jupyter/client/distributed.go -destination=common/jupyter/mock_client/mock_distributed.go
	mockgen -source=common/jupyter/client/kernel.go -destination=common/jupyter/mock_client/mock_kernel.go