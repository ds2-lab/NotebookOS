package smr

import (
	"io"
)

type IntRet struct {
	N   int
	Err string
}

type WriteCloser interface {
	Write(p []byte) *IntRet
	Close() string
}

type writeCloserWrapper struct {
	writer io.WriteCloser
}

func (wc *writeCloserWrapper) Write(p []byte) *IntRet {
	n, err := wc.writer.Write(p)
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	return &IntRet{
		N:   n,
		Err: msg,
	}
}

func (wc *writeCloserWrapper) Close() string {
	err := wc.writer.Close()
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	return msg
}
