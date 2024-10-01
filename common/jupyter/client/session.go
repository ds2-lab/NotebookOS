package client

type sessionManagerImpl struct {
	sessions []string
}

func NewSessionManager(kernelSess string) SessionManager {
	sm := &sessionManagerImpl{
		sessions: make([]string, 1, 2), // Reserve 2 slots for kernel session and notebook session.
	}
	sm.sessions[0] = kernelSess
	return sm
}

// Sessions returns the associated session ID.
func (c *sessionManagerImpl) Sessions() []string {
	return c.sessions
}

// BindSession binds a session ID to the client.
func (c *sessionManagerImpl) BindSession(sess string) {
	// TODO: No deduplication for now. Add if needed.
	c.sessions = append(c.sessions, sess)
}

// UnbindSession unbinds a session ID from the client.
func (c *sessionManagerImpl) UnbindSession(sess string) {
	for i, s := range c.sessions {
		if s == sess {
			c.sessions = append(c.sessions[:i], c.sessions[i+1:]...)
			return
		}
	}
}

// ClearSessions clears all sessions.
func (c *sessionManagerImpl) ClearSessions() {
	c.sessions = c.sessions[:0]
}
