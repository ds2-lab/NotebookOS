package daemon

// ExecutionSubmitter ensures that "execute_request" (and "yield_request") messages are sent one-at-a-time.
type ExecutionSubmitter struct {
}
