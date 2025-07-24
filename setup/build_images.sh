#!/bin/bash

CURRENT_USER=$(whoami)
GOPATH_ENV=$(go env GOPATH)

# Check if environment variable is set
if [ -z "${DOCKERUSER}" ]; then
    echo "Error: DOCKERUSER environment variable is not set" >&2
    echo "Please set the DOCKERUSER variable before running the script" >&2
    exit 1
fi


#################
# scusemua/gopy #
#################
pushd "$GOPATH_ENV/pkg/gopy"
python3.12 -m pip install pybindgen
go install golang.org/x/tools/cmd/goimports@latest
go install github.com/scusemua/gopy@go-python-master
make
docker build -t $DOCKERUSER/gopy .
popd

pushd "$GOPATH_ENV/pkg/NotebookOS"
pushd smr
make build-linux-amd64
popd
make build-smr-linux-amd64
