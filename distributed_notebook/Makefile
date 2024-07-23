include ./env.mk

all: build-smr-linux-arm64

# Verify the value of PYTHON3_PATH and PYTHON3_VERSION.
test-env:
	@echo $(PYTHON3)
	@echo $(PYTHON3_PATH)
	@echo $(PYTHON3_VERSION)
	@echo $(PYTHON3_ESCAPED_PATH)

test-env-amd64:
	@echo ""
	@echo "Environment: "
	@echo "PYTHON3: $(PYTHON3)"
	@echo "PYTHON3_PATH: $(PYTHON3_PATH)"
	@echo "PYTHON3_VERSION: $(PYTHON3_VERSION)"
	@echo "PYTHON3_ESCAPED_PATH: $(PYTHON3_ESCAPED_PATH)"
	@echo "PYTHON3_INCLUDES_PATH: $(PYTHON3_INCLUDES_PATH)"
	@echo "PYTHON3_LIB_PATH: $(PYTHON3_LIB_PATH)"
	@echo ""

# Generate os and arch specific smr.go. Call make in project root directory to generate smr.go.
prepare-%.go:
ifneq ($(wildcard smr/smr.go),)
# Remove smr.go by backing up it.
	mv smr/smr.go smr/bak/smr.go
endif
# Generate os and arch specific smr.go
	cp smr/bak/smr.go	smr/$*.go

prepare-smr-linux-amd64: prepare-smr_linux_amd64.go
	@echo "Preparing SMR Linux AMD64"
# We don't use -i because it may cause permission problem with created temporary file.
	sed -Ei 's/^#cgo CFLAGS:(.*)/#cgo CFLAGS: -I$(PYTHON3_INCLUDES_PATH_ESCAPED) -g -ggdb -Wno-error -Wno-implicit-function-declaration -Wno-int-conversion/' smr/smr_linux_amd64.go > smr/smr_linux_arm64.go.tmp
	mv smr/smr_linux_arm64.go.tmp smr/smr_linux_arm64.go
	sed -Ei 's/^#cgo LDFLAGS:(.*)/#cgo LDFLAGS: -L$(PYTHON3_LIB_PATH_ESCAPED) -lpython3.11d -lcrypt -lpthread -ldl -lutil -lm/' smr/smr_linux_amd64.go > smr/smr_linux_arm64.go.tmp
	mv smr/smr_linux_arm64.go.tmp smr/smr_linux_arm64.go
	sed -i -e 's/\(CFLAGS = "[^"]*\)"/\1 -g -ggdb"/' ../distributed_notebook/smr/Makefile

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
	@echo "\nBuilding SMR common now!!!"
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
	@echo "Restoring Makefile -- copying smr/bak/Makefile to smr/Makefile"
# Restore Makefile
	cp smr/bak/Makefile smr/Makefile
	@echo "Restoring original smr.go -- copying smr/bak/smr.go to smr/smr.go"
# Restore original smr.go
	cp smr/bak/smr.go	smr/smr.go