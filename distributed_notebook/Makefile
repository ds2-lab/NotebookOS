include ./env.mk

all: build-smr-linux-arm64

# Verify the value of PYTHON3_PATH and PYTHON3_VERSION.
test-env:
	@echo $(PYTHON3)
	@echo $(PYTHON3_PATH)
	@echo $(PYTHON3_VERSION)
	@echo $(PYTHON3_ESCAPED_PATH)

# Generate os and arch specific smr.go. Call make in project root directory to generate smr.go.
prepare-%.go:
ifneq ($(wildcard smr/smr.go),)
# Remove smr.go by backing up it.
	mv smr/smr.go smr/bak/smr.go
endif
# Generate os and arch specific smr.go
	cp smr/bak/smr.go	smr/$*.go

prepare-smr-darwin: prepare-smr_darwin.go
	sed -Ei '' 's/^#cgo CFLAGS:(.*)/#cgo CFLAGS: -I$(PYTHON3_ESCAPED_PATH)\/include\/python$(PYTHON3_VERSION) -Wno-error -Wno-implicit-function-declaration -Wno-int-conversion/' smr/smr_darwin.go
	sed -Ei '' 's/^#cgo LDFLAGS:(.*)/#cgo LDFLAGS: -L$(PYTHON3_ESCAPED_PATH)\/lib -lpython$(PYTHON3_VERSION) -ldl -framework CoreFoundation/' smr/smr_darwin.go

prepare-smr-linux-arm64: prepare-smr_linux_arm64.go
# We don't use -i because it may cause permission problem with created temporary file.
	sed -E 's/^#cgo CFLAGS:(.*)/#cgo CFLAGS: -I\/usr\/include\/python3. -Wno-error -Wno-implicit-function-declaration -Wno-int-conversion/' smr/smr_linux_arm64.go > smr/smr_linux_arm64.go.tmp
	mv smr/smr_linux_arm64.go.tmp smr/smr_linux_arm64.go
	sed -E 's/^#cgo LDFLAGS:(.*)/#cgo LDFLAGS: -L\/usr\/lib -lpython3.9 -lcrypt -lpthread -ldl  -lutil -lm -lm/' smr/smr_linux_arm64.go > smr/smr_linux_arm64.go.tmp
	mv smr/smr_linux_arm64.go.tmp smr/smr_linux_arm64.go

prepare-smr-linux-amd64: prepare-smr_linux_amd64.go
# We don't use -i because it may cause permission problem with created temporary file.
	sed -Ei 's/^#cgo CFLAGS:(.*)/#cgo CFLAGS: -I\/usr\/local\/include\/python3.11 -Wno-error -Wno-implicit-function-declaration -Wno-int-conversion/' smr/smr_linux_amd64.go > smr/smr_linux_arm64.go.tmp
	mv smr/smr_linux_arm64.go.tmp smr/smr_linux_arm64.go
	sed -Ei 's/^#cgo LDFLAGS:(.*)/#cgo LDFLAGS: -L\/usr\/local\/lib -lpython3.11 -lcrypt -lpthread -ldl  -lutil -lm -lm/' smr/smr_linux_amd64.go > smr/smr_linux_arm64.go.tmp
	mv smr/smr_linux_arm64.go.tmp smr/smr_linux_arm64.go

build-init:
	@mkdir -p smr/bak
ifeq (smr/bak/Makefile,$(wildcard smr/bak/Makefile))
# Restore Makefile if available. The backup must not exist initially bacause the folder is regenerated.
	cp -f smr/bak/Makefile smr/Makefile
else
# Backup Makefile if not available.
	cp -f smr/Makefile smr/bak/Makefile
endif

# Don't call this target directly. This target is used by smr/Makefile.
build-smr-common: build-init
	cat Makefile_common >> smr/Makefile
	cd smr && make build-smr-common

build-smr-linux-arm64: build-init prepare-smr-linux-arm64
	cat Makefile_linux_arm64 >> smr/Makefile
	cd smr && make build-smr-linux-arm64
	make reset

build-smr-linux-amd64: build-init prepare-smr-linux-amd64
	@echo "Building SMR for Linux AMD64"
	cat Makefile_linux_amd64 >> smr/Makefile
	cd smr && make build-smr-linux-amd64
	make reset

build-smr-darwin: build-init prepare-smr-darwin
	cat Makefile_darwin >> smr/Makefile
	cd smr && make build-smr-darwin
	make reset

reset:
# Restore Makefile
	cp smr/bak/Makefile smr/Makefile
# Restore original smr.go
	cp smr/bak/smr.go	smr/smr.go