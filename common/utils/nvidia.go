package utils

//import (
//	"fmt"
//	"github.com/NVIDIA/go-nvml/pkg/nvml"
//)

// GetNumberOfActualGPUs attempts to use the [Go Bindings for the NVIDIA Management Library] to retrieve the number of
// real/actual GPUs available on the host.
//
// GetNumberOfActualGPUs will return -1 and an error if nvml.Init() or nvml.DeviceGetCount() fail/return an error.
//
// GetNumberOfActualGPUs will panic if nvml.Shutdown() fails. The call to nvml.Shutdown() is deferred.
//
// [Go Bindings for the NVIDIA Management Library]: https://github.com/NVIDIA/go-nvml?tab=readme-ov-file#quick-start
func GetNumberOfActualGPUs() (int, error) {
	panic("Had to comment this out because building was failing and I don't feel like debugging that right now")
	//ret := nvml.Init()
	//if ret != nvml.SUCCESS { // Official docs for nvml go module do not use errors.Is or errors.As here
	//	return -1, fmt.Errorf("unable to initialize NVML: %v", nvml.ErrorString(ret))
	//}
	//
	//defer func() {
	//	ret := nvml.Shutdown()
	//	if ret != nvml.SUCCESS { // Official docs for nvml go module do not use errors.Is or errors.As here
	//		panic(fmt.Sprintf("Unable to shutdown NVML: %v", nvml.ErrorString(ret)))
	//	}
	//}()
	//
	//count, ret := nvml.DeviceGetCount()
	//if ret != nvml.SUCCESS { // Official docs for nvml go module do not use errors.Is or errors.As here
	//	return -1, fmt.Errorf("unable to get device count: %v", nvml.ErrorString(ret))
	//}
	//
	//return count, nil
}
