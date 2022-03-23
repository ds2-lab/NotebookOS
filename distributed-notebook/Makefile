from-go:
	cd ../smr && make go-python

clean:
	cd ../smr && make clean

prepare-smr-linux-arm64: from-go
	cp smr/smr.go smr/smr_linux_arm64.go
	# In smr_linux_arm64.go
	# Replace 
	#cgo CFLAGS: ....
	#cgo LDFLAGS: ....
	# With (as in ../smr/smr/smr.go)
	#cgo CFLAGS: -I/usr/include/python3.9 -Wno-error -Wno-implicit-function-declaration -Wno-int-conversion
	#cgo LDFLAGS: -L/usr/lib -lpython3.9 -lcrypt -lpthread -ldl  -lutil -lm -lm

build-smr-linux-arm64:
	cp -f smr/*.py ../smr/smr/
	cp -f smr/smr_go.h ../smr/smr/
	cp -f smr/smr_linux_arm64.go ../smr/smr/smr.go
	cd ../smr && make build-smr-linux-arm64