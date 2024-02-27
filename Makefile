PYTHON=python3
PIP=$(PYTHON) -m pip

all: build

replica:
	python3 -X faulthandler -m distributed_notebook.demo_replica $(PARAMS)

debug-training-all:
	python3 -m distributed_notebook.demo distributed_notebook/demo/script/training.py distributed_notebook/demo/script/training1.py

debug-training:
	python3 -X faulthandler -m distributed_notebook.demo $(PARAMS) distributed_notebook/demo/script/training.py

debug-training1:
	python3 -X faulthandler -m distributed_notebook.demo --resume $(PARAMS) distributed_notebook/demo/script/training1.py

python-demo-all:
	python3 -m distributed_notebook.demo distributed_notebook/demo/script/script.py distributed_notebook/demo/script/script2.py

python-demo-step1:
	python3 -m distributed_notebook.demo distributed_notebook/demo/script/script.py

python-demo-step2:
	python3 -m distributed_notebook.demo --resume distributed_notebook/demo/script/script2.py

python-demo-step3:
	python3 -m distributed_notebook.demo --resume distributed_notebook/demo/script/script3.py

build: build-darwin

build-darwin:
	cd smr && make build-darwin

build-smr-linux-arm64:
	docker run -it --rm -v `pwd`:/go/src/in -v `pwd`:/out scusemua/gopy /bin/bash -c "cd /go/src/in/distributed_notebook && make build-smr-linux-arm64"

build-smr-linux-amd64:
	docker run -it --rm -v `pwd`:/go/src/in -v `pwd`:/out scusemua/gopy /bin/bash -c "cd /go/src/in/distributed_notebook && make build-smr-linux-amd64"

install-kernel:
	./install_kernel.sh

build-grpc-go:
	protoc --go_out=. --go_opt=paths=source_relative \
  	--go-grpc_out=. --go-grpc_opt=paths=source_relative \
    common/gateway/gateway.proto

# protoc --go_out=. --go_opt=paths=source_relative \
# --go-grpc_out=. --go-grpc_opt=paths=source_relative \
# common/driver/driver.proto 

build-grpc-python:
	python3 -m grpc_tools.protoc -Icommon/gateway \
  	--python_out=distributed_notebook/gateway \
		--grpc_python_out=distributed_notebook/gateway \
    common/gateway/gateway.proto
ifeq ($(shell uname -p), x86_64)
	@sed -i -E 's/import gateway_pb2 as gateway__pb2/from . import gateway_pb2 as gateway__pb2/g' distributed_notebook/gateway/gateway_pb2_grpc.py
else
	@sed -Ei '' 's/import gateway_pb2 as gateway__pb2/from . import gateway_pb2 as gateway__pb2/g' distributed_notebook/gateway/gateway_pb2_grpc.py
endif 

# We probably don't need the Python gRPC bindings.
# 	python3 -m grpc_tools.protoc -Icommon/driver \
#   	--python_out=distributed_notebook/driver \
# 		--grpc_python_out=distributed_notebook/driver \
#     common/driver/driver.proto
# ifeq ($(shell uname -p), x86_64)
# 	@sed -i -E 's/import driver_pb2 as driver__pb2/from . import driver_pb2 as driver__pb2/g' distributed_notebook/driver/driver_pb2_grpc.py
# else
# 	@sed -Ei '' 's/import driver_pb2 as driver__pb2/from . import driver_pb2 as driver__pb2/g' distributed_notebook/driver/driver_pb2_grpc.py
# endif 

build-grpc: build-grpc-go build-grpc-python

build-gateway:
	go build -o bin/gateway ./gateway

build-scheduler:
	go build -o bin/scheduler ./scheduler

gateway: build-gateway
	bin/gateway

scheduler: build-scheduler
	bin/scheduler
	
test:
	cd distributed_notebook && pytest
