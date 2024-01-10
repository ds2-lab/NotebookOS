package main

import (
	"os"
	"text/template"
)

type Session struct {
	SessionId string
}

type DataA struct {
	Name string
	Age  int
}

type DataB struct {
	Address string
}

type Data struct {
	DataA     *DataA
	DataB     *DataB
	SessionId string
}

func main() {
	// var templateFile = "./distributed-kernel-deployment-template.yaml"
	// tmpl, err := template.New("distributed-kernel-deployment-template.yaml").ParseFiles(templateFile)
	// if err != nil {
	// 	panic(err)
	// }
	// sess := &Session{SessionId: "Session1"}
	// err = tmpl.Execute(os.Stdout, sess)
	// if err != nil {
	// 	panic(err)
	// }

	data_a := &DataA{Name: "Bob", Age: 25}
	data_b := &DataB{Address: "123 Union Street"}
	data := &Data{DataA: data_a, DataB: data_b, SessionId: "Session1"}

	var templateFile = "./test-template.yaml"
	tmpl2, err2 := template.New("test-template.yaml").ParseFiles(templateFile)
	if err2 != nil {
		panic(err2)
	}
	err2 = tmpl2.Execute(os.Stdout, data)
	if err2 != nil {
		panic(err2)
	}
}
