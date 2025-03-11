all: build test

build:
	gopy build -symbols=true -output=out -vm=python3 github.com/scusemua/distributed-notebook/smr/test/funcs

build-darwin:
	export CGO_CFLAGS="-I/usr/local/opt/python@3.9/Frameworks/Python.framework/Versions/3.9/include/python3.9 -fPIC -Ofast"
	export CGO_LDFLAGS="-L/usr/local/opt/python@3.9/Frameworks/Python.framework/Versions/3.9/lib -lpython3.9 -ldl -framework CoreFoundation"
	cd out && go build -mod=mod -buildmode=c-shared -gcflags=all="-N -l" -o _funcs.cpython-39-darwin.so .

test:
	python3 test.py

clean:
	rm -rf out