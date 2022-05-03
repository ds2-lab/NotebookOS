DEST = ../distributed_notebook/smr
PATCHS = $(shell ls patch/*.py)
PATCH_TARGETS = $(PATCHS:patch/%=patch.%)
RESTORE_TARGETS = $(PATCHS:patch/%=restore.%)
CC = gcc

prepare:
	@mkdir -p $(DEST)/bak

restore: $(RESTORE_TARGETS)

restore.%:
	@echo "restoring $(DEST)/bak/$*"
	-@cp $(DEST)/bak/$* $(DEST)/$*

patch: prepare $(PATCH_TARGETS)
	@echo "patching go files"
	cp -f patch/*.go $(DEST)/

patch.build.py: patch/build.py restore.build.py prepare
ifeq (,$(wildcard $(DEST)/bak/build.py))
	@echo "backuping $(DEST)/bak/build.py"
	@cp $(DEST)/build.py $(DEST)/bak/build.py 
endif
	@echo "patching $(DEST)/build.py"
	@sed -Ei '' '/^mod\.generate\(/d' $(DEST)/build.py
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
	@sed -Ei '' -e '/^_wrap__smr_smr_NewBytes\(/r $<' -e '/^_wrap__smr_smr_NewBytes\(/,/^\}/d' $(DEST)/smr.c

gen-python: clean
	gopy gen -output=../distributed_notebook/smr -vm=python3 github.com/zhangjyr/distributed-notebook/smr

build-python: clean
	gopy build -output=../distributed_notebook/smr -vm=python3 github.com/zhangjyr/distributed-notebook/smr

build-common:
	cd $(dir $(DEST)) && make build-smr-common

gen-darwin: gen-python patch
	-rm $(DEST)/smr_darwin.go

gen-%: gen-python patch

build-%: gen-% build-common patch.smr.c
	cd $(dir $(DEST)) && make build-smr-$*

clean:
	rm -rf $(DEST)
#	rm -rf $(DEST)/bak