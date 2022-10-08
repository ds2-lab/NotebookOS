all: build-smr-linux-arm64

prepare-smr-linux-arm64:
ifeq (,$(wildcard smr/smr_linux_arm64.go))
	cp smr/smr.go smr/smr_darwin.go
endif
	sed -Ei 's/^#cgo CFLAGS:(.*)/#cgo CFLAGS: -I\/usr\/include\/python3.9 -Wno-error -Wno-implicit-function-declaration -Wno-int-conversion/' smr/smr.go
	sed -Ei 's/^#cgo LDFLAGS:(.*)/#cgo LDFLAGS: -L\/usr\/lib -lpython3.9 -lcrypt -lpthread -ldl  -lutil -lm -lm/' smr/smr.go

prepare-smr-linux-amd64:
ifeq (,$(wildcard smr/smr_linux_amd64.go))
	cp smr/smr.go smr/smr_darwin.go
endif
	sed -Ei 's/^#cgo CFLAGS:(.*)/#cgo CFLAGS: -I\/usr\/include\/python3.9 -Wno-error -Wno-implicit-function-declaration -Wno-int-conversion/' smr/smr.go
	sed -Ei 's/^#cgo LDFLAGS:(.*)/#cgo LDFLAGS: -L\/usr\/lib -lpython3.9 -lcrypt -lpthread -ldl  -lutil -lm -lm/' smr/smr.go

# build-smr-linux-arm64:
# 	cp -f smr/*.py ../smr/smr/
# 	cp -f smr/smr_go.h ../smr/smr/
# 	cp -f smr/smr_linux_arm64.go ../smr/smr/smr.go
# 	cd ../smr && make build-smr-linux-arm64

build-init:
	@mkdir -p smr/bak
ifeq (smr/bak/Makefile,$(wildcard smr/bak/Makefile))
	cp -f smr/bak/Makefile smr/Makefile
else
	cp -f smr/Makefile smr/bak/Makefile
endif

build-smr-common: build-init
	cat Makefile_common >> smr/Makefile
	cd smr && make build-smr-common

build-smr-linux-arm64: build-init prepare-smr-linux-arm64
	cat Makefile_linux_arm64 >> smr/Makefile
	cd smr && make build-smr-linux-arm64

build-smr-linux-amd64: build-init prepare-smr-linux-amd64
	cat Makefile_linux_amd64 >> smr/Makefile
	cd smr && make build-smr-linux-amd64

build-smr-darwin: build-init
	cat Makefile_darwin >> smr/Makefile
	cd smr && make build-smr-darwin