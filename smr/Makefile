all: go-python

go-python:
	gopy build -output=smr -vm=python3 github.com/zhangjyr/distributed-notebook/smr

build-smr-linux-arm64:
	cp -f smr/Makefile smr/Makefile_origin
	cat Makefile_linux_arm64 >> smr/Makefile
	cd smr && make build-smr-linux-arm64
	mv -f smr/Makefile_origin smr/Makefile

clean:
	rm -rf smr