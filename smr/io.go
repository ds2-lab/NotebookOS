package smr

import (
	"fmt"
	"io"
)

type writerWrapper struct {
	writer io.Writer
}

func (wc *writerWrapper) Write(p Bytes) *IntRet {
	n, err := wc.writer.Write(p.Bytes())
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	return &IntRet{
		N:   n,
		Err: msg,
	}
}

func (wc *writerWrapper) Close() string {
	msg := ""
	if c, ok := wc.writer.(io.Closer); ok {
		err := c.Close()
		if err != nil {
			msg = err.Error()
		}
	}
	return msg
}

type readerWrapper struct {
	reader io.Reader
}

func (wc *readerWrapper) Read(p Bytes) *IntRet {
	// fmt.Printf("[readerWrapper] Reading bytes of length: %d\n", p.Len())
	n, err := wc.reader.Read(p.Bytes())
	fmt.Printf("[readerWrapper] Read %d bytes into buffer of maximum size %d. Error: %v.\n", n, p.Len(), err)
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	return &IntRet{
		N:   n,
		Err: msg,
	}
}

func (wc *readerWrapper) Close() string {
	msg := ""
	if c, ok := wc.reader.(io.Closer); ok {
		err := c.Close()
		if err != nil {
			msg = err.Error()
		}
	}
	return msg
}
