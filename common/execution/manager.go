package execution

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"sync"
)

// Manager manages the Execution
type Manager struct {
	log logger.Logger
	mu  sync.Mutex

	// ActiveExecutions is a map from Jupyter "msg_id" to the Execution encapsulating
	// the code submission with the aforementioned ID.
	//
	// ActiveExecutions contains only code submissions that have not yet completed.
	// There should typically just be one entry in the ActiveExecutions map at a time,
	// but because messages can be weirdly delayed and reordered, there may be multiple.
	ActiveExecutions map[string]*Execution

	// FinishedExecutions is a map from Jupyter "msg_id" to the Execution encapsulating
	// the code submission with the aforementioned ID.
	//
	// FinishedExecutions contains only Execution structs that represent code submissions
	// that completed successfully, without error.
	FinishedExecutions map[string]*Execution

	// ErredExecutions is a map from Jupyter "msg_id" to the Execution encapsulating
	// the code submission with the aforementioned ID.
	//
	// ErredExecutions contains only Execution structs that represent code submissions
	// that failed to complete successfully and were abandoned.
	ErredExecutions map[string]*Execution

	// AllExecutions is a map from Jupyter "msg_id" to the Execution encapsulating
	// the code submission with the aforementioned ID.
	//
	// AllExecutions contains an entry for every single Execution, regardless of
	// the State of the Execution.
	AllExecutions map[string]*Execution

	// Kernel is the kernel associated with the Manager.
	Kernel Kernel
}

// NewManager creates a new Manager struct to be associated with the given Kernel.
//
// NewManager returns a pointer to the new Manager struct.
func NewManager(kernel Kernel) *Manager {
	manager := &Manager{
		ActiveExecutions:   make(map[string]*Execution),
		FinishedExecutions: make(map[string]*Execution),
		ErredExecutions:    make(map[string]*Execution),
		AllExecutions:      make(map[string]*Execution),
		Kernel:             kernel,
	}

	config.InitLogger(&manager.log, manager)

	return manager
}

func (m *Manager) RegisterExecution(msg *messaging.JupyterMessage) error {
	return nil
}
