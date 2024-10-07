package main

import (
	"encoding/json"
	"fmt"
	"github.com/mitchellh/mapstructure"
)

type MyStructWrapper struct {
	MyStruct          *MyStruct              `json:"my_struct" mapstructure:"my_struct"`
	RemainingMetadata map[string]interface{} `mapstructure:",remain"`
}

func (w *MyStructWrapper) String() string {
	m, err := json.Marshal(w)
	if err != nil {
		panic(err)
	}

	return string(m)
}

type MyStruct struct {
	MyField int64 `json:"my_field" mapstructure:"my_field"`
}

func main() {
	var metadata map[string]interface{} = make(map[string]interface{})
	var serialized_my_struct map[string]int64 = make(map[string]int64)

	serialized_my_struct["my_field"] = 999

	metadata["my_struct"] = serialized_my_struct
	metadata["other_data"] = 42

	var wrapper *MyStructWrapper
	err := mapstructure.Decode(metadata, &wrapper)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Wrapper: %s\n", wrapper.String())
}
