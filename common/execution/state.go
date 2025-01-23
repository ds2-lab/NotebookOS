package execution

var (
	Pending State = "pending"
	Running State = "running"
	Erred   State = "erred"
	Unknown State = "unknown"
)

type State string

func (s State) String() string {
	return string(s)
}
