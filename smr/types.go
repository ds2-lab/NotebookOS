package smr

import "C"
import (
	"unsafe"
)

// Manager of python bytes for buffered stream.
type Bytes struct {
	bytes []byte
}

// Create a new bytes wrapper object.
func NewBytes(p unsafe.Pointer, len int) *Bytes {
	return &Bytes{
		bytes: unsafe.Slice((*byte)(p), len),
	}
}

// Return the underlying byte slice as buffer.
func (b *Bytes) Bytes() []byte {
	return b.bytes
}

// Get the length of the underlying byte slice.
func (b *Bytes) Len() int {
	return len(b.bytes)
}

type IntRet struct {
	N   int
	Err string
}

type WriteCloser interface {
	Write(p Bytes) *IntRet
	Close() string
}

type ReadCloser interface {
	Read(p Bytes) *IntRet
	Close() string
}
