# Copyright 2015 The go-python Authors.  All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

## py2/py3 compat
from __future__ import print_function
from ast import Slice
import asyncio
import pickle
import io
import time
from threading import Thread

from out import go, funcs

fs = funcs.FunStruct()
fs.FieldS = "str field"
fs.FieldI = 42
fs.FieldB = go.Slice_byte(pickle.dumps("val"))

print(fs.Byte())
exit()

def cbfun(afs, ival, sval):
    tfs = funcs.FunStruct(handle=afs)
    print("in python cbfun: FieldI: ", tfs.FieldI, " FieldS: ", tfs.FieldS, " ival: ", ival, " sval: ", sval)

def cbfunif(afs, ival, ifval):
    tfs = funcs.FunStruct(handle=afs)
    print("in python cbfunif: FieldI: ", tfs.FieldI, " FieldS: ", tfs.FieldS, " ival: ", ival, " ifval: ", ifval)

def cbfunrval(afs, ival, ifval):
    tfs = funcs.FunStruct(handle=afs)
    print("in python cbfunrval: FieldI: ", tfs.FieldI, " FieldS: ", tfs.FieldS, " ival: ", ival, " ifval: ", ifval)
    return True

def getCbfunbytes(handler):
    def cbfunbytes(buff, id):
        val = pickle.load(io.BytesIO(bytes(go.Slice_byte(handle=buff))))
        print("in python cbbytes: Val: ", val, " Id: ", id)
        handler()

    return cbfunbytes

def cbfuninterface(v):
    iv = funcs.Verbose(handle=v)
    print("in python cbfuninterface: Output: ", iv.String())

def cbret() -> bytes:
    print("in python cbret")
    ret = "result of cbret"
    return str.encode(ret, "utf-8")

start_outofthread = 0
def cbfunoutofthread(v):
    iv = funcs.Verbose(handle=v)
    end_outofthread = time.time()
    print("in python cbfunOutOTthread: Output: {}, end: {}, elapsed: {}", iv.String(), end_outofthread, end_outofthread - start_outofthread)

def closureHandler():
    print("in closureHandler")
    
class MyClass(go.GoClass):
    def __init__(self, *args, **kwargs):
        self.misc = 2
    
    def ClassFun(self, afs, ival, sval):
        tfs = funcs.FunStruct(handle=afs)
        print("in python class fun: FieldI: ", tfs.FieldI, " FieldS: ", tfs.FieldS, " ival: ", ival, " sval: ", sval)

    def CallSelf(self):
        fs.CallBack(77, self.ClassFun)

    def CallBytes(self, handler):
        print(handler)

        def cbfunbytes(buff, id):
            val = pickle.load(io.BytesIO(bytes(go.Slice_byte(handle=buff))))
            print("in python cbbytes: Val: ", val, " Id: ", id)
            handler()

        fs.CallBackBytes(self.cbfunbytes)

    def cbfunbytes(self, buff, id):
        val = pickle.load(io.BytesIO(bytes(go.Slice_byte(handle=buff))))
        print("in python cbbytes: Val: ", val, " Id: ", id)

    def closureHandler(self):
        print("in instance closureHandler")
   
print("fs.CallBack(22, cbfun)...")
fs.CallBack(22, cbfun)

print("fs.CallBackIf(22, cbfunif)...")
fs.CallBackIf(22, cbfunif)

print("fs.CallBackRval(22, cbfunrval)...")
fs.CallBackRval(22, cbfunrval)

print("fs.CallBackInterface(cbfuninterface)...")
fs.CallBackInterface(cbfuninterface)

print("fs.CallBackBytes(cbfunbytes)...")
fs.CallBackBytes(getCbfunbytes(closureHandler))

start_outofthread = time.time()
print("fs.CallBackOutOfThread(cbfuninterface)...start: {}".format(start_outofthread))

def threadCall():
    fs.CallBackOutOfThread(cbfunoutofthread)
    thread = Thread(target=fs.WaitOutOfThread)
    thread.start()
    thread.join()

class Promise:
    def __init__(self, loop=None):
        if loop is None:
            loop = asyncio.get_running_loop()
        self.loop = loop
        self.future = loop.create_future()
    
    async def resolve(self, value):
        # print("wait 10 seconds")
        # await asyncio.sleep(0)
        # time.sleep(10)

        print("in resolver")
        # coro = asyncio.to_thread(self.future.set_result, value)
        # asyncio.run_coroutine_threadsafe(promise.resolve(value), loop)
        self.future.set_result(value)
        print("in resolver: result set")

    async def result(self):
        return await self.future

async def asyncCall(msg):
    loop = asyncio.get_running_loop()
    promise = Promise(loop = loop)

    def dummyResolver(value):
        # Schedule and run from other threads
        # promise.resolve(value)
        asyncio.run_coroutine_threadsafe(promise.resolve(value), loop)
        

    fs.SetInterfaceCallback(cbfunoutofthread)
    fs.CallBackOutOfThread(dummyResolver)
    # dummyResolver()
    print("wait for 2 seconds")
    await asyncio.sleep(2)
    print("waited 2 seconds")
    return await promise.result()

print(asyncio.run(asyncCall("CallBackOutOfThread executed")))
# threadCall() #Failed

print("fs.CallbackWithRet(cbret)...")
fs.CallbackWithRet(cbret)

cls = MyClass()

# note: no special code needed to work with methods in callback (PyObject_CallObject just works)
# BUT it does NOT work if the callback is initiated from a different thread!  Then only regular
# functions work.
print("fs.CallBack(32, cls.ClassFun)...")
fs.CallBack(32, cls.ClassFun)

print("cls.CallSelf...")
cls.CallSelf()

print("fs.ObjArg with nil")
fs.ObjArg(go.nil)

print("fs.ObjArg with fs")
fs.ObjArg(fs)

print("fs.CallBackBytes(cbfunbytes)...")
cls.CallBytes(cls.closureHandler)

# TODO: not currently supported:

# print("funcs.F1()...")
# f1 = funcs.F1()
# print("f1()= %s" % f1())
# 
# print("funcs.F2()...")
# f2 = funcs.F2()
# print("f2()= %s" % f2())
# 
# print("s1 = funcs.S1()...")
# s1 = funcs.S1()
# print("s1.F1 = funcs.F2()...")
# s1.F1 = funcs.F2()
# print("s1.F1() = %s" % s1.F1())
# 
# print("s2 = funcs.S2()...")
# s2 = funcs.S2()
# print("s2.F1 = funcs.F1()...")
# s2.F1 = funcs.F1()
# print("s2.F1() = %s" % s2.F1())

print("OK")
