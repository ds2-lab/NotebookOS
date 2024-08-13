DEST = ../distributed_notebook/smr
PATCHS = $(shell ls patch/*.py)
PATCH_TARGETS = $(PATCHS:patch/%=patch.%)
RESTORE_TARGETS = $(PATCHS:patch/%=restore.%)
CC = gcc

include ../distributed_notebook/env.mk

all: build-darwin

prepare:
	@mkdir -p $(DEST)/bak

restore: $(RESTORE_TARGETS)

restore.%:
	@echo "\nrestoring $(DEST)/bak/$*"
	-@cp $(DEST)/bak/$* $(DEST)/$*

patch: prepare $(PATCH_TARGETS)
	@echo "\npatching go files"
	cp -f patch/*.go $(DEST)/

patch.build.py: patch/build.py restore.build.py prepare
ifeq (,$(wildcard $(DEST)/bak/build.py))
	@echo "backuping $(DEST)/bak/build.py"
	@cp $(DEST)/build.py $(DEST)/bak/build.py 
endif
	@echo "\npatching $(DEST)/build.py"
ifeq ($(shell uname -p), x86_64)
	@sed -i -E '/^mod\.generate\(/d' $(DEST)/build.py
else
	@sed -Ei '' '/^mod\.generate\(/d' $(DEST)/build.py
endif
	cat $< >> $(DEST)/build.py
	
patch.%: patch/% restore.% prepare
	@echo "backuping $(DEST)/bak/$*"
	@cp $(DEST)/$* $(DEST)/bak/$*
	@echo "patching $(DEST)/$*"
	cat $< >> $(DEST)/$*

patch.smr.c: patch/smr.c restore.smr.c prepare
ifeq (,$(wildcard $(DEST)/bak/smr.c))
	@echo "backuping $(DEST)/bak/smr.c"
	@cp $(DEST)/smr.c $(DEST)/bak/smr.c 
endif
	@echo "patching $(DEST)/smr.c"
ifeq ($(shell uname -p), x86_64)
	@sed -i -E -e '/^_wrap__smr_smr_NewBytes\(/r $<' -e '/^_wrap__smr_smr_NewBytes\(/,/^\}/d' $(DEST)/smr.c
else
	@sed -Ei '' -e '/^_wrap__smr_smr_NewBytes\(/r $<' -e '/^_wrap__smr_smr_NewBytes\(/,/^\}/d' $(DEST)/smr.c
endif

patch.darwin:
	sed -Ei '' 's/^CFLAGS =(.*)/CFLAGS = -I$(PYTHON3_ESCAPED_PATH)\/include\/python$(PYTHON3_VERSION)/' ../distributed_notebook/smr/Makefile
	sed -Ei '' 's/^LDFLAGS =(.*)/LDFLAGS = -L$(PYTHON3_ESCAPED_PATH)\/lib -lpython$(PYTHON3_VERSION) -ldl -lSystem -framework CoreFoundation/' ../distributed_notebook/smr/Makefile
	sed -Ei '' 's/^#cgo CFLAGS:(.*)/#cgo CFLAGS: -I$(PYTHON3_ESCAPED_PATH)\/include\/python$(PYTHON3_VERSION) -Wno-error -Wno-implicit-function-declaration -Wno-int-conversion/' ../distributed_notebook/smr/smr.go
	sed -Ei '' 's/^#cgo LDFLAGS:(.*)/#cgo LDFLAGS: -L$(PYTHON3_ESCAPED_PATH)\/lib -lpython$(PYTHON3_VERSION) -ldl -framework CoreFoundation/' ../distributed_notebook/smr/smr.go

# Add debug symbols
patch.debug:
	@echo "Patching ../distributed_notebook/smr/Makefile"
ifeq ($(shell uname -p), x86_64)
	@sed -i -e 's/\(CFLAGS = "[^"]*\)"/\1 -ggdb3 -Wall -O0"/' ../distributed_notebook/smr/Makefile
else
	sed i '' 's/\(CFLAGS = "[^"]*\)"/\1 -ggdb3 -Wall -O0"/' ../distributed_notebook/smr/Makefile
endif

gen-python: clean
	@echo "\nGenerating Go/Python bindings via gopy now"
	gopy gen -build-tags='-gcflags=all="-N -l"' -output=../distributed_notebook/smr -vm=$(PYTHON3_PATH) github.com/zhangjyr/distributed-notebook/smr

build-python: clean
	@echo "\nBuilding Python"
	gopy build -output=../distributed_notebook/smr -build-tags='-gcflags=all="-N -l"' -vm=$(PYTHON3_PATH) github.com/zhangjyr/distributed-notebook/smr -symbols=true 

build-common:
	@echo "\nBuilding 'smr-common' now."
	@echo "Directory: \"$(dir $(DEST))\""
	cd $(dir $(DEST)) && make build-smr-common

gen-darwin: gen-python patch patch.darwin
	-rm $(DEST)/smr_darwin.go

gen-linux: gen-python patch

gen-%: gen-python patch

build-linux-amd64: gen-linux build-common patch.smr.c patch.debug
	@echo "\nBuilding 'build-linux-amd64' now."
	@echo "DIRECTORY: $(dir $(DEST))"
	#cd $(dir $(DEST)) && make build-smr-linux-amd64

build-%: gen-% build-common patch.smr.c
	cd $(dir $(DEST)) && make build-smr-$*

clean:
	@echo "\nRemoving directory \"$(DEST)\""
	rm -rf $(DEST)
#	rm -rf $(DEST)/bak

