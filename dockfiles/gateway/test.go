package main

import (
	"os"
	"text/template"
)

type Session struct {
	SessionId string
}

func main() {
	var templateFile = "./distributed-kernel-deployment-template.yaml"
	tmpl, err := template.New("distributed-kernel-deployment-template.yaml").ParseFiles(templateFile)
	if err != nil {
		panic(err)
	}
	sess := &Session{SessionId: "Session1"}
	err = tmpl.Execute(os.Stdout, sess)
	if err != nil {
		panic(err)
	}
}
