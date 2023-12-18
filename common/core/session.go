package core

type Session interface {
	// SetID sets the id of the session.
	// Session will be created first and acquires id from kernel_info_request.
	SetID(string)

	Kernel
}
