import argparse 

from distributed_notebook.smr import smr 
from distributed_notebook.smr.go import Slice_string, Slice_int

import time 

def changeCallback(rc, sz: int, id: str) -> bytes:
    print(f"changeCallback({rc}, {sz}, {id})") 
    return str.encode("", "utf-8")

def restoreCallback(rc, sz) -> bytes:
    print(f"restoreCallback({rc}, {sz})") 
    return str.encode("", "utf-8")

parser = argparse.ArgumentParser()

parser.add_argument("--hdfs", default = "127.0.0.1:9000", type = str) # "172.17.0.1:9000"
parser.add_argument("-i", "--id", default = 1, type = int)

args = parser.parse_args()

logNode = smr.NewLogNode("store", args.id, args.hdfs , "", Slice_string(["http://127.0.0.1:8000", "http://127.0.0.1:8001", "http://127.0.0.1:8002"]), Slice_int([1, 2, 3]), False, 8585)
print(logNode)

smr.PrintTestMessage()

config = smr.NewConfig()
config.ElectionTick = 10
config.HeartbeatTick = 1
config = config.WithChangeCallback(changeCallback).WithRestoreCallback(restoreCallback)

logNode.Start(config)

print("Started LogNode")

while True:
    smr.PrintTestMessage()

    time.sleep(3)

    print("Slept for 3 seconds.")