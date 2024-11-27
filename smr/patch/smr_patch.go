package main

/*
#include <Python.h>
*/
import "C"
import (
	"unsafe"

	"github.com/scusemua/distributed-notebook/smr"
)

// ---- Constructors ---

//export smr_NewBytes
func smr_NewBytes(bytes *C.char, len int) CGoHandle {
	return handleFromPtr_Ptr_smr_Bytes(smr.NewBytes(unsafe.Pointer(bytes), len))
}
