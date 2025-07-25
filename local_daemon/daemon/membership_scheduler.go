package daemon

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	jupyter "github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/local_daemon/domain"
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

// func (s *MembershipScheduler) triggerMigration(kernel scheduling.kernel) error {
// 	persistentId := kernel.(client.kernel).PersistentID()
// 	s.log.Info("Triggering hard-coded migration of replica %d of kernel %s", kernel.(client.kernel).ReplicaID(), kernel.ID())

// 	resp, err := s.daemon.Provisioner().MigrateKernelReplica(context.Background(), &gateway.MigrationRequest{
// 		TargetReplica: &gateway.ReplicaInfo{
// 			kernelId:     kernel.ID(),
// 			ReplicaID:    kernel.(client.kernel).ReplicaID(),
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
