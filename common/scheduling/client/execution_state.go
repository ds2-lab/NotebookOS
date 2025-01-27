package client

var (
	Pending   State = "pending"
	Running   State = "running"
	Completed State = "completed"
	Erred     State = "erred"
	Unknown   State = "unknown"
)

type State string

func (s State) String() string {
	return string(s)
}
