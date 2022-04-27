package smr

import (
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
	n, err := wc.reader.Read(p.Bytes())
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
