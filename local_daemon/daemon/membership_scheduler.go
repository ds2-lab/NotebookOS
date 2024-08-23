package daemon

import (
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/core"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/local_daemon/domain"
)

// MembershipScheduler is a core.Scheduler that for testing the membership changing.
type MembershipScheduler struct {
	daemon domain.SchedulerDaemon
	log    logger.Logger
}

func NewMembershipScheduler(daemon domain.SchedulerDaemon) *MembershipScheduler {
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

// func (s *MembershipScheduler) triggerMigration(kernel core.Kernel) error {
// 	persistentId := kernel.(client.KernelReplicaClient).PersistentID()
// 	s.log.Info("Triggering hard-coded migration of replica %d of kernel %s", kernel.(client.KernelReplicaClient).ReplicaID(), kernel.ID())

// 	resp, err := s.daemon.Provisioner().MigrateKernelReplica(context.Background(), &gateway.MigrationRequest{
// 		TargetReplica: &gateway.ReplicaInfo{
// 			KernelId:     kernel.ID(),
// 			ReplicaId:    kernel.(client.KernelReplicaClient).ReplicaID(),
// 			PersistentId: persistentId,
// 		},
// 	})

// 	if err != nil {
// 		s.log.Error("Hard-coded migration failed: %v", err)
// 		return err
// 	}

// 	s.log.Info("Received response from kernel migration: ID=%d, Hostname=%s", resp.Id, resp.Hostname)

// 	return nil
// }
