package proto

import (
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

func (ci *KernelConnectionInfo) ConnectionInfo() *jupyter.ConnectionInfo {
	return &jupyter.ConnectionInfo{
		IP:              ci.Ip,
		ControlPort:     int(ci.ControlPort),
		ShellPort:       int(ci.ShellPort),
		StdinPort:       int(ci.StdinPort),
		IOSubPort:       int(ci.IosubPort),
		IOPubPort:       int(ci.IopubPort),
		HBPort:          int(ci.HbPort),
		Transport:       ci.Transport,
		SignatureScheme: ci.SignatureScheme,
		Key:             ci.Key,
	}
}
