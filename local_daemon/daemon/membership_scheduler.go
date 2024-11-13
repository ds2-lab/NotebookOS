package daemon

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	"github.com/zhangjyr/distributed-notebook/local_daemon/domain"
)

// MembershipScheduler is a scheduling.Scheduler that for testing the membership changing.
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

func (s *MembershipScheduler) OnTaskStart(kernel scheduling.Kernel, task *jupyter.MessageSMRLeadTask) error {
	// return s.triggerMigration(kernel)
	return nil
}

// func (s *MembershipScheduler) triggerMigration(kernel scheduling.Kernel) error {
// 	persistentId := kernel.(client.Kernel).PersistentID()
// 	s.log.Info("Triggering hard-coded migration of replica %d of kernel %s", kernel.(client.Kernel).ReplicaID(), kernel.ID())

// 	resp, err := s.daemon.Provisioner().MigrateKernelReplica(context.Background(), &gateway.MigrationRequest{
// 		TargetReplica: &gateway.ReplicaInfo{
// 			KernelId:     kernel.ID(),
// 			ReplicaID:    kernel.(client.Kernel).ReplicaID(),
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
