package client

import (
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
)

type sessionManagerImpl struct {
	sessions hashmap.HashMap[string, struct{}]
}

func NewSessionManager(kernelSess string) scheduling.SessionManager {
	sm := &sessionManagerImpl{
		sessions: hashmap.NewThreadsafeCornelkMap[string, struct{}](2),
	}
	sm.sessions.Store(kernelSess, struct{}{})
	return sm
}

// Sessions returns the associated session ID.
func (c *sessionManagerImpl) Sessions() []string {
	sessions := make([]string, 0, c.sessions.Len())

	c.sessions.Range(func(sessionId string, _ struct{}) bool {
		sessions = append(sessions, sessionId)
		return true
	})

	return sessions
}

// BindSession binds a session ID to the client.
func (c *sessionManagerImpl) BindSession(sess string) {
	c.sessions.Store(sess, struct{}{})
}

// UnbindSession unbinds a session ID from the client.
func (c *sessionManagerImpl) UnbindSession(sess string) {
	c.sessions.Delete(sess)
}

// ClearSessions clears all sessions.
func (c *sessionManagerImpl) ClearSessions() {
	c.sessions = hashmap.NewThreadsafeCornelkMap[string, struct{}](2)
}
