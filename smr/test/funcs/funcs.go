// Copyright 2015 The go-python Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package funcs

import (
	"fmt"
	"log"
	"time"

	"github.com/zhangjyr/gopy/_examples/cpkg"
)

type ByteCallback func(*[]byte, string)
type Resolver func(interface{})
type CallbackWithRet func() string

type Verbose interface {
	String() string
}

type FunStruct struct {
	FieldI      int
	FieldS      string
	FieldB      []byte
	cbBytes     ByteCallback
	cbInterface func(Verbose)
	done        chan struct{}
}

type Reader struct {
	buff []byte
}

func (r *Reader) ReadAll() []byte {
	return r.buff
}

func (fs *FunStruct) SetInterfaceCallback(cb func(Verbose)) {
	fs.cbInterface = cb
}

func (fs *FunStruct) CallBack(i int, fun func(fs *FunStruct, i int, s string)) {
	fun(fs, i, fs.FieldS)
}

type RecvFunc func(fs *FunStruct, i int, v interface{})

func (fs *FunStruct) CallBackIf(i int, fun RecvFunc) {
	fun(fs, i, fs.FieldS)
}

func (fs *FunStruct) CallBackRval(i int, fun func(fs *FunStruct, i int, v interface{}) bool) {
	rv := fun(fs, i, fs.FieldS)
	fmt.Printf("got return value: %v\n", rv)
}

func (fs *FunStruct) CallBackBytes(fun ByteCallback) {
	fs.cbBytes = fun
	done := make(chan struct{})
	cbs := make(chan func())
	go fs.callBackBytesRouting(cbs, fun, done)
	select {
	case cb := <-cbs:
		cb()
	case <-done:
	}
}

func (fs *FunStruct) callBackBytesRouting(cbs chan func(), fun ByteCallback, done chan struct{}) {
	cbs <- func() {
		fs.cbBytes(&fs.FieldB, "id")
		close(done)
	}
}

func (fs *FunStruct) CallBackInterface(fun func(v Verbose)) {
	fun(fs)
}

func (fs *FunStruct) CallBackOutOfThread(resolve Resolver) {
	if fs.done == nil {
		fs.done = make(chan struct{})
	}
	go fs.callBackOutOfThread(resolve)
}

func (fs *FunStruct) callBackOutOfThread(resolve Resolver) {
	for i := 0; i < 5; i++ {
		fs.cbInterface(fs)
		time.Sleep(1 * time.Second)
	}
	fmt.Println("Wait for 5 seconds")
	time.Sleep(5 * time.Second)
	close(fs.done)
	fs.done = nil
	if resolve != nil {
		fmt.Println("Calling resolve")
		resolve("resolved")
	}
}

func (fs *FunStruct) WaitOutOfThread() {
	done := fs.done
	if done != nil {
		<-fs.done
	}
}

func (fs *FunStruct) CallbackWithRet(cb CallbackWithRet) {
	log.Printf("callback ret: %s", cb())
}

func (fs *FunStruct) OtherMeth(i int, s string) {
	fs.FieldI = i
	fs.FieldS = s
	fmt.Printf("i=%d s=%s\n", i, s)
}

func (fs *FunStruct) ObjArg(ofs *FunStruct) {
	if ofs == nil {
		fmt.Printf("got nil\n")
	} else {
		fmt.Printf("ofs FieldI: %d FieldS: %s\n", ofs.FieldI, ofs.FieldS)
	}
}

func (fs *FunStruct) String() string {
	return fs.FieldS
}

func (fs *FunStruct) Byte() []byte {
	return []byte(fs.FieldS)
}

var (
	F1 func()
	F2 Func
	F3 S1
	F4 S2
	F5 []func()
	F6 []Func
	F7 [2]func()
	F8 [3]Func
)

type Func func()

type S1 struct {
	F1 Func
	F2 []Func
	F3 [4]Func
}

type S2 struct {
	F1 func()
	F2 []func()
	F3 [5]func()
}

func init() {
	F1 = func() {
		cpkg.Printf("calling F1\n")
	}

	F2 = Func(func() {
		cpkg.Printf("calling F2\n")
	})
}
