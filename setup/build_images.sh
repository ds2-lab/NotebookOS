#!/bin/bash

GOPATH_ENV=$(go env GOPATH)

#################
# scusemua/gopy #
#################
pushd "$GOPATH_ENV/pkg/gopy"
python3.12 -m pip install pybindgen
go install golang.org/x/tools/cmd/goimports@latest
make 
docker build -t $DOCKERUSER/gopy .
popd

pushd "$GOPATH_ENV/pkg/NotebookOS"
pushd smr
make build-linux-amd64
popd
make build-smr-linux-amd64
