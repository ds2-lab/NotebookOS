all: build test

build:
	gopy build -output=out -vm=python3 github.com/zhangjyr/distributed-notebook/smr/test/funcs

test:
	python3 test.py

clean:
	rm -rf out