# SMR

SMR module leverage etcd's [raft library](https://github.com/etcd-io/raft) to provide a network SyncLog(../distributed-notebook/sync) implementation.

## Deploy

Dockerfile for gopy

~~~
FROM golang:onbuild

RUN apt-get update && apt-get install -y pkg-config python3.9-dev python3-pip && apt-get clean
RUN python3 -m pip install pybindgen
RUN cd /go/src/app && go build -o /go/bin/goimports golang.org/x/tools/cmd/goimports
CMD /go/bin/gopy
~~~

Build in gopy

~~~
cd $GOPATH/src/github.com/scusemua/gopy
docker build -t scusemua/gopy .
~~~

Build in docker

~~~
make prepare-smr-linux-arm64
make build-smr-linux-arm64
make clean
~~~

## Compatibility

### Mac OS Intel

What to do: In go.etcd.io/etcd/client/pkg/v3/fileutil/sync_darwin.go "func Fsync(f *os.File) error", shortcut FcntlInt call. 
Error: operation not supported.
Reason: F_FUULFSSYNC is implemented on HFS, MS-DOS (FAT), and Universal Disk Format (UDF) only.
Reference:
https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fcntl.2.html

Dockerfile for golang:onbuild

~~~
FROM arm64v8/golang:1.18

COPY go-wrapper /usr/local/bin/

# On build start
RUN mkdir -p /go/src/app
WORKDIR /go/src/app

# this will ideally be built by the ONBUILD below ;)
CMD ["go-wrapper", "run"]

ONBUILD COPY . /go/src/app
ONBUILD RUN go-wrapper download
ONBUILD RUN go-wrapper install
~~~
