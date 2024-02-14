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
	persistentId := kernel.(*client.KernelClient).PersistentID()
	s.log.Info("Triggering hard-coded migration of replica %d of kernel %s", kernel.(*client.KernelClient).ReplicaID(), kernel.ID())
	// migrateKernelResponse, err := s.daemon.Provisioner.MigrateKernelReplica(context.Background(), &gateway.ReplicaInfo{
	_, err := s.daemon.Provisioner.MigrateKernelReplica(context.Background(), &gateway.ReplicaInfo{
		KernelId:     kernel.ID(),
		ReplicaId:    kernel.(*client.KernelClient).ReplicaID(),
		PersistentId: persistentId,
	})
	if err != nil {
		s.log.Error("Hard-coded migration failed: %v", err)
		return err
	}

	// s.log.Debug("Now that kernel %s(%d) has been added, notify the existing members.", kernel.ID(), migrateKernelResponse.Id)
	// frames := jupyter.NewJupyterFramesWithHeader(jupyter.MessageTypeAddReplicaRequest, kernel.(*client.KernelClient).Sessions()[0])
	// frames.EncodeContent(&jupyter.MessageSMRAddReplicaRequest{
	// 	NodeID:  migrateKernelResponse.Id,
	// 	Address: fmt.Sprintf("%s:%d", migrateKernelResponse.Hostname, s.daemon.smrPort), // s.daemon.getInvoker(kernel).GetReplicaAddress(kernel.KernelSpec(), migrateKernelResponse.Id),
	// })
	// if _, err := frames.Sign(kernel.ConnectionInfo().SignatureScheme, []byte(kernel.ConnectionInfo().Key)); err != nil {
	// 	return err
	// }

	// msg := &zmq4.Msg{Frames: frames}
	// var wg sync.WaitGroup
	// wg.Add(1)
	// err = kernel.(*client.KernelClient).RequestWithHandler(context.Background(), "Sending", jupyter.ControlMessage, msg, nil, wg.Done, server.DefaultRequestTimeout)
	// if err != nil {
	// 	return err
	// }
	// wg.Wait()

	// return s.daemon.stopKernel(context.Background(), kernel.(*client.KernelClient), true)
	return nil
}
