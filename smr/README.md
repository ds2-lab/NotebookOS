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

# Ben Note:
I will fully update this README at some point. For now, I rebuild the SMR code on Linux AMD64 via:
``` sh
sudo env PATH=$PATH make build-linux-amd64
```
from the SMR directory. Note that you need to have certain environment variables set correctly. I've written some code to try to resolve these automatically, by the `PYTHON3_LIB_PATH` resolves to a slightly incorrect directory (for my Conda-based installation of Python), so I hard-coded it for now. 

These environment variables are set in `/home/bcarver2/go/distributed-notebook/distributed_notebook/env.mk`.

Resolving them on Mac/OSx/Darwin-based systems is different than Linux/AMD64. For now, they're set to AMD64 in that `env.mk` file. Swap the uncommented lines with the commented lines if you want to use a Mac/Darwin-based system.

Again, the `PYTHON3_LIB_PATH` is hard-coded right now, at least for Linux/AMD64 systems. It needs to be the directory that has the `libpython3.11.so`, `libpython3.11.so.1.0`, and `libpython3.so` files in it. For me, this is `~/miniconda3/lib/`. 

After running `sudo env PATH=$PATH make build-linux-amd64` in the `smr/` directory, I go back to the root project directory and execute:
``` sh
sudo make build-smr-linux-amd64
```

The command in the `smr/` directory does something along the lines of regenerate the Python/Golang/C bindings. The command from the root directory compiles the generated Go code. Something like that.