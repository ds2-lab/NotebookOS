package daemon

import (
	"context"

	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/core"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

// MembershipScheduler is a core.Scheduler that for testing the membership changing.
type MembershipScheduler struct {
	daemon *SchedulerDaemon
	log    logger.Logger
}

func NewMembershipScheduler(daemon *SchedulerDaemon) *MembershipScheduler {
	scheduler := &MembershipScheduler{
		daemon: daemon,
	}
	config.InitLogger(&scheduler.log, scheduler)
	return scheduler
}

func (s *MembershipScheduler) OnTaskStart(kernel core.Kernel, task *jupyter.MessageSMRLeadTask) error {
	// return s.triggerMigration(kernel)
	return nil
}

func (s *MembershipScheduler) triggerMigration(kernel core.Kernel) error {
	persistentId := kernel.(*client.KernelClient).PersistentID()
	s.log.Info("Triggering hard-coded migration of replica %d of kernel %s", kernel.(*client.KernelClient).ReplicaID(), kernel.ID())

	resp, err := s.daemon.Provisioner.MigrateKernelReplica(context.Background(), &gateway.MigrationRequest{
		TargetReplica: &gateway.ReplicaInfo{
			KernelId:     kernel.ID(),
			ReplicaId:    kernel.(*client.KernelClient).ReplicaID(),
			PersistentId: persistentId,
		},
		PersistentId: persistentId,
	})

	if err != nil {
		s.log.Error("Hard-coded migration failed: %v", err)
		return err
	}

	s.log.Info("Received response from kernel migration: ID=%d, Hostname=%s", resp.Id, resp.Hostname)

	return nil
}
