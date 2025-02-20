package proto

import "encoding/json"

var (
	VOID = &Void{}
)

func (x *KernelConnectionInfo) PrettyString() string {
	m, err := json.MarshalIndent(x, "", "  ")
	if err != nil {
		panic(err)
	}

	return string(m)
}
