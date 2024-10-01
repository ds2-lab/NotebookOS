PYTHON3=$(shell which python3.12)
#PYTHON3_PATH=$(shell python3.11 -c "import sys; import re; p=list(filter(lambda s: s.find('Versions/3.') > -1, sys.path)); print(re.sub(r'^(.+?)/(Versions/3\.\d+)/(.*)$$', r'\1/\2', p[0]) if len(p) > 0 else r'')")
PYTHON3_PATH=$(shell python3.12 -c "import sys; print(sys.executable)")
PYTHON3_VERSION:=$(shell python3.12 -c "from sysconfig import get_paths;import sys;info = get_paths();sys.stdout.write(info['include'])") # $(shell echo $(PYTHON3_PATH) | sed -E 's/^.+\/Versions\///')
PYTHON3_INCLUDES_PATH:=$(shell python3.12 -c "from sysconfig import get_paths;import sys;info = get_paths();sys.stdout.write(info['include'])")
PYTHON3_LIB_PATH="/usr/lib/x86_64-linux-gnu/" # /home/bcarver2/miniconda3/lib/
PYTHON3_INCLUDES_PATH_ESCAPED:=$(subst /,\/,$(PYTHON3_INCLUDES_PATH))
PYTHON3_LIB_PATH_ESCAPED:=$(subst /,\/,$(PYTHON3_LIB_PATH))
PYTHON3_ESCAPED_PATH:=$(subst /,\/,$(PYTHON3_PATH))

# TODO: Test this.
# ifeq ($(shell uname -p), x86_64)
# 	PYTHON3=$(shell which python3)
# 	PYTHON3_PATH=$(shell python3 -c "import sys; import re; p=list(filter(lambda s: s.find('Versions/3.') > -1, sys.path)); print(re.sub(r'^(.+?)/(Versions/3\.\d+)/(.*)$$', r'\1/\2', p[0]) if len(p) > 0 else r'')")
# 	PYTHON3_VERSION:=$(shell python3 -c "from sysconfig import get_paths;import sys;info = get_paths();sys.stdout.write(info['include'])") # $(shell echo $(PYTHON3_PATH) | sed -E 's/^.+\/Versions\///')
# 	PYTHON3_INCLUDES_PATH:=$(shell python3 -c "from sysconfig import get_paths;import sys;info = get_paths();sys.stdout.write(info['include'])")
# 	PYTHON3_LIB_PATH:=$(shell python3 -c "from sysconfig import get_paths;import sys;info = get_paths();sys.stdout.write(info['stdlib'])")
# 	PYTHON3_ESCAPED_PATH:=$(subst /,\/,$(PYTHON3_PATH))
# else
# 	PYTHON3=$(shell which python3)
# 	PYTHON3_PATH=$(shell python3 -c "import sys; import re; p=list(filter(lambda s: s.find('Versions/3.') > -1, sys.path)); print(re.sub(r'^(.+?)/(Versions/3\.\d+)/(.*)$$', r'\1/\2', p[0]) if len(p) > 0 else r'')")
# 	PYTHON3_VERSION:=$(shell echo $(PYTHON3_PATH) | sed -E 's/^.+\/Versions\///')
# 	PYTHON3_ESCAPED_PATH:=$(subst /,\/,$(PYTHON3_PATH))
# endif 