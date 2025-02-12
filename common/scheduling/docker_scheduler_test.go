package scheduling_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	jupyter "github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/mock_proto"
	"github.com/scusemua/distributed-notebook/common/mock_scheduling"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/cluster"
	"github.com/scusemua/distributed-notebook/common/scheduling/entity"
	"github.com/scusemua/distributed-notebook/common/scheduling/index"
	"github.com/scusemua/distributed-notebook/common/scheduling/mock_scheduler"
	"github.com/scusemua/distributed-notebook/common/scheduling/placer"
	"github.com/scusemua/distributed-notebook/common/scheduling/scheduler"
	distNbTesting "github.com/scusemua/distributed-notebook/common/testing"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/gateway/domain"
	"go.uber.org/mock/gomock"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"sync"
	"sync/atomic"
	"time"
)

var (
	dockerSchedulerTestOpsAsJson = `
{
	"jaeger_addr": "",
	"consul_addr": "",
	"port": 8080,
	"provisioner_port": 8081,
	"logger_options": {
		"Debug": true,
		"Verbose": false
	},
	"connection_info": {
		"ip": "",
		"control_port": 9001,
		"shell_port": 9002,
		"stdin_port": 9003,
		"hb_port": 9000,
		"iopub_port": 9004,
		"iosub_port": 9005,
		"ack_port": 9006,
		"transport": "tcp",
		"signature_scheme": "",
		"key": "",
		"starting_resource_port": 9007,
		"num_resource_ports": 256
	},
	"cluster_daemon_options": {
		"cluster_scheduler_options": {
			"num-virtual-gpus-per-node": 72,
			"subscribed-ratio-update-interval": 1,
			"mean_scale_out_per_host_sec": 0.75,
			"std_dev_scale_out_per_host_sec": 0.5,
			"mean_scale_in_per_host_sec": 0.75,
			"std_dev_scale_in_per_host_sec": 0.5,
			"scaling-factor": 1.05,
			"scaling-interval": 15,
			"scaling-limit": 1.1,
			"scaling-in-limit": 2,
			"predictive_autoscaling": false,
			"scaling-buffer-size": 3,
			"min_cluster_nodes": 24,
			"max_cluster_nodes": 32,
			"gpu_poll_interval": 5,
			"num-replicas": 3,
			"max-subscribed-ratio": 7,
			"execution-time-sampling-window": 10,
			"migration-time-sampling-window": 10,
			"scheduler-http-port": 8078,
			"initial-cluster-size": -1,
			"initial-connection-period": 0,
			"common_options": {
				"gpus-per-host": 8,
				"deployment_mode": "docker-swarm",
				"using-wsl": true,
				"docker_network_name": "distributed_cluster_default",
				"prometheus_interval": 15,
				"prometheus_port": -1,
				"num_resend_attempts": 1,
				"acks_enabled": false,
				"scheduling-policy": "static",
				"idle-session-reclamation-policy": "none",
				"remote-remote_storage-endpoint": "host.docker.internal:10000",
				"smr-port": 8080,
				"debug_mode": true,
				"debug_port": 9996,
				"simulate_checkpointing_latency": true,
				"disable_prometheus_metrics_publishing": true
			}
		},
		"local-daemon-service-name": "local-daemon-network",
		"local-daemon-service-port": 8075,
		"global-daemon-service-name": "daemon-network",
		"global-daemon-service-port": 0,
		"kubernetes-namespace": "",
		"use-stateful-set": false,
		"notebook-image-name": "scusemua/jupyter-gpu",
		"notebook-image-tag": "latest",
		"distributed-cluster-service-port": 8079,
		"remote-docker-event-aggregator-port": 5821
	}
}`
)

// dockerSchedulerTestHostMapper implements the scheduling.HostMapper interface and exists for use in the
// Docker Swarm Scheduler unit tests.
type dockerSchedulerTestHostMapper struct{}

// GetHostsOfKernel returns the Host instances on which the replicas of the specified kernel are scheduled.
func (m *dockerSchedulerTestHostMapper) GetHostsOfKernel(kernelId string) ([]scheduling.Host, error) {
	return nil, nil
}

func addHost(idx int, hostSpec types.Spec, disableHost bool, cluster scheduling.Cluster, mockCtrl *gomock.Controller) (scheduling.UnitTestingHost, *mock_proto.MockLocalGatewayClient, *distNbTesting.ResourceSpoofer, error) {
	hostId := uuid.NewString()
	nodeName := fmt.Sprintf("TestNode%d", idx)
	resourceSpoofer := distNbTesting.NewResourceSpoofer(nodeName, hostId, hostSpec)
	host, localGatewayClient, err := distNbTesting.NewHostWithSpoofedGRPC(mockCtrl, cluster, hostId, nodeName, resourceSpoofer)

	Expect(host).ToNot(BeNil())

	if disableHost {
		disableErr := host.Disable()
		Expect(disableErr).To(BeNil())
	}

	err2 := cluster.NewHostAddedOrConnected(host)

	if err != nil && err2 != nil {
		err = errors.Join(err, err2)
	} else if err2 != nil {
		// err is nil, but that's what we return, so let's return the non-nil err2 instead.
		err = err2
	}

	return host, localGatewayClient, resourceSpoofer, err
}

var _ = Describe("Docker Scheduler Tests", func() {
	var (
		mockCtrl        *gomock.Controller
		dockerScheduler *scheduler.DockerScheduler
		dockerCluster   scheduling.Cluster
		clusterPlacer   scheduling.Placer
		hostSpec        *types.DecimalSpec
		opts            *domain.ClusterGatewayOptions

		kernelProvider *mock_scheduler.MockKernelProvider
		hostMapper     *mock_scheduler.MockHostMapper
		// notificationBroker *mock_scheduler.MockNotificationBroker
	)

	BeforeEach(func() {
		hostSpec = types.NewDecimalSpec(8000, 64000, 8, 32)

		err := json.Unmarshal([]byte(dockerSchedulerTestOpsAsJson), &opts)
		if err != nil {
			panic(err)
		}

		//optsStr, _ := json.MarshalIndent(&opts, "", "  ")
		//fmt.Printf("%s\n", optsStr)
	})

	Context("Static Scheduling", func() {
		BeforeEach(func() {
			mockCtrl = gomock.NewController(GinkgoT())

			kernelProvider = mock_scheduler.NewMockKernelProvider(mockCtrl)
			hostMapper = mock_scheduler.NewMockHostMapper(mockCtrl)
			// notificationBroker = mock_scheduler.NewMockNotificationBroker(mockCtrl)

			opts.SchedulingPolicy = string(scheduling.Static) // Should already be set to static, but just to be sure.
			schedulingPolicy, err := scheduler.GetSchedulingPolicy(&opts.SchedulerOptions)
			Expect(err).To(BeNil())
			Expect(schedulingPolicy).ToNot(BeNil())
			Expect(schedulingPolicy.NumReplicas()).To(Equal(3))
			Expect(schedulingPolicy.Name()).To(Equal("Static Scheduling"))

			Expect(opts.MeanScaleInPerHostSec).To(Equal(0.75))
			Expect(opts.MeanScaleOutPerHostSec).To(Equal(0.75))

			// clusterPlacer, err = placer.NewRandomPlacer(nil, schedulingPolicy.NumReplicas(), schedulingPolicy)
			clusterPlacer, err = schedulingPolicy.GetNewPlacer(nil)
			Expect(err).To(BeNil())
			Expect(clusterPlacer).ToNot(BeNil())
			_, ok := clusterPlacer.(*placer.BasicPlacer)
			Expect(ok).To(BeTrue())

			_, ok = clusterPlacer.GetIndex().(*index.StaticIndex)
			Expect(ok).To(BeTrue())

			dockerCluster = cluster.NewDockerCluster(hostSpec, clusterPlacer, hostMapper, kernelProvider,
				nil, nil, schedulingPolicy, func(f func(stats *metrics.ClusterStatistics)) {},
				&opts.ClusterDaemonOptions.SchedulerOptions)

			Expect(dockerCluster).ToNot(BeNil())

			genericScheduler := dockerCluster.Scheduler()
			Expect(genericScheduler).ToNot(BeNil())

			dockerScheduler, ok = genericScheduler.(*scheduler.DockerScheduler)
			Expect(ok).To(BeTrue())
			Expect(dockerScheduler).ToNot(BeNil())

			// hostMapper = &dockerSchedulerTestHostMapper{}
		})

		AfterEach(func() {
			mockCtrl.Finish()
		})

		Context("Will handle basic scheduling operations correctly", func() {
			var numHosts int
			var hosts map[int]scheduling.UnitTestingHost
			var localGatewayClients map[int]*mock_proto.MockLocalGatewayClient
			var resourceSpoofers map[int]*distNbTesting.ResourceSpoofer

			Context("Starting with 3 hosts", func() {
				BeforeEach(func() {
					hosts = make(map[int]scheduling.UnitTestingHost)
					localGatewayClients = make(map[int]*mock_proto.MockLocalGatewayClient)
					resourceSpoofers = make(map[int]*distNbTesting.ResourceSpoofer)
					numHosts = 3

					for i := 0; i < numHosts; i++ {
						host, localGatewayClient, resourceSpoofer, err := addHost(i, hostSpec, false, dockerCluster, mockCtrl)
						Expect(err).To(BeNil())
						Expect(host).ToNot(BeNil())
						Expect(localGatewayClient).ToNot(BeNil())
						Expect(resourceSpoofer).ToNot(BeNil())

						hosts[i] = host
						localGatewayClients[i] = localGatewayClient
						resourceSpoofers[i] = resourceSpoofer
					}
				})

				validateVariablesNonNil := func() {
					Expect(dockerScheduler).ToNot(BeNil())
					Expect(clusterPlacer).ToNot(BeNil())
					Expect(dockerCluster).ToNot(BeNil())
					Expect(hostMapper).ToNot(BeNil())
					Expect(len(hosts)).To(Equal(3))
					Expect(len(localGatewayClients)).To(Equal(3))
					Expect(len(resourceSpoofers)).To(Equal(3))
				}

				It("Will correctly identify candidate hosts when candidate hosts are available", func() {
					validateVariablesNonNil()

					kernelId := uuid.NewString()
					kernelKey := uuid.NewString()
					resourceSpec := proto.NewResourceSpec(1250, 2000, 2, 4)

					kernelSpec := &proto.KernelSpec{
						Id:              kernelId,
						Session:         kernelId,
						Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
						SignatureScheme: jupyter.JupyterSignatureScheme,
						Key:             kernelKey,
						ResourceSpec:    resourceSpec,
					}

					candidateHosts, err := dockerScheduler.GetCandidateHosts(context.Background(), kernelSpec)
					Expect(err).To(BeNil())
					Expect(candidateHosts).ToNot(BeNil())
					Expect(len(candidateHosts)).To(Equal(3))

					for _, host := range candidateHosts {
						Expect(host.NumReservations()).To(Equal(1))
						Expect(host.PendingResources().Equals(kernelSpec.DecimalSpecFromKernelSpec())).To(BeTrue())

						reservation, loaded := host.GetReservation(kernelId)
						Expect(loaded).To(BeTrue())
						// Matches kernel.
						Expect(reservation.GetKernelId()).To(Equal(kernelId))
						// Matches host.
						Expect(reservation.GetHostId()).To(Equal(host.GetID()))
						// Not pending.
						Expect(reservation.IsPending()).To(BeTrue())
						// Created recently.
						Expect(time.Since(reservation.GetTimestamp()) < (time.Second * 5)).To(BeTrue())
						// Correct amount of resources.
						Expect(reservation.ToSpec().Equals(kernelSpec.DecimalSpecFromKernelSpec())).To(BeTrue())
					}
				})

				It("Will correctly return an error when no candidate hosts are available", func() {
					validateVariablesNonNil()

					kernelId := uuid.NewString()
					kernelKey := uuid.NewString()
					resourceSpec := proto.NewResourceSpec(1250, 2000, 64 /* too many */, 4)

					kernelSpec := &proto.KernelSpec{
						Id:              kernelId,
						Session:         kernelId,
						Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
						SignatureScheme: jupyter.JupyterSignatureScheme,
						Key:             kernelKey,
						ResourceSpec:    resourceSpec,
					}

					candidateHosts, err := dockerScheduler.GetCandidateHosts(context.Background(), kernelSpec)
					Expect(err).ToNot(BeNil())
					Expect(candidateHosts).To(BeNil())
					Expect(len(candidateHosts)).To(Equal(0))
					fmt.Printf("Error: %v\n", err)
				})

				It("Will try to scale out if necessary", func() {
					initialSize := len(hosts)

					// First, add two new hosts, so that there are 5 hosts.
					var hostIndex int
					for hostIndex = initialSize; hostIndex < initialSize+2; hostIndex++ {
						host, localGatewayClient, resourceSpoofer, err := addHost(hostIndex, hostSpec, false, dockerCluster, mockCtrl)
						Expect(err).To(BeNil())
						Expect(host).ToNot(BeNil())
						Expect(localGatewayClient).ToNot(BeNil())
						Expect(resourceSpoofer).ToNot(BeNil())

						hosts[hostIndex] = host
						localGatewayClients[hostIndex] = localGatewayClient
						resourceSpoofers[hostIndex] = resourceSpoofer
					}

					Expect(dockerCluster.Len()).To(Equal(5))

					// Add a sixth host, but set it to be disabled initially.
					hostId := uuid.NewString()
					nodeName := fmt.Sprintf("TestNode%d", 5)
					resourceSpoofer := distNbTesting.NewResourceSpoofer(nodeName, hostId, hostSpec)
					Expect(resourceSpoofer).ToNot(BeNil())

					host, localGatewayClient, err := distNbTesting.NewHostWithSpoofedGRPC(mockCtrl, dockerCluster, hostId, nodeName, resourceSpoofer)
					Expect(err).To(BeNil())
					Expect(host).ToNot(BeNil())
					Expect(localGatewayClient).ToNot(BeNil())

					err = host.Disable()
					Expect(err).To(BeNil())

					err = dockerCluster.NewHostAddedOrConnected(host)
					Expect(err).To(BeNil())

					hosts[hostIndex] = host
					localGatewayClients[hostIndex] = localGatewayClient
					resourceSpoofers[hostIndex] = resourceSpoofer
					hostIndex++

					Expect(dockerCluster.Len()).To(Equal(5))
					Expect(dockerCluster.NumDisabledHosts()).To(Equal(1))

					By("First scheduling a bunch of sessions into/onto the dockerCluster")

					// Schedule a bunch of sessions so that, when we try to schedule another one, it'll oversubscribe one or more hosts.
					for i := 0; i < 21; i++ {
						kernelId := uuid.NewString()
						kernelKey := uuid.NewString()
						resourceSpec := proto.NewResourceSpec(1250, 2000, 8, 4)
						decimalSpec := resourceSpec.ToDecimalSpec()
						kernelSpec := &proto.KernelSpec{
							Id:              kernelId,
							Session:         kernelId,
							Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
							SignatureScheme: jupyter.JupyterSignatureScheme,
							Key:             kernelKey,
							ResourceSpec:    resourceSpec,
						}

						session := entity.NewSessionBuilder().
							WithContext(context.Background()).
							WithID(kernelId).
							WithKernelSpec(kernelSpec).
							WithMigrationTimeSampleWindowSize(opts.MigrationTimeSamplingWindow).
							WithTrainingTimeSampleWindowSize(opts.ExecutionTimeSamplingWindow).
							Build()

						dockerCluster.AddSession(kernelId, session)
						dockerCluster.Scheduler().UpdateRatio(true)

						Expect(dockerCluster.Sessions().Len()).To(Equal(i + 1))

						for j := 0; j < 3; j++ {
							host := hosts[j]

							//fmt.Printf("Cluster subscribed ratio: %f, host %s (ID=%s) subscription ratio: %f, oversubscription factor: %s\n",
							//	dockerCluster.SubscriptionRatio(), host.GetNodeName(), host.GetID(), host.SubscribedRatio(), host.OversubscriptionFactor().StringFixed(4))

							err := host.AddToPendingResources(decimalSpec)
							Expect(err).To(BeNil())

							dockerCluster.Scheduler().UpdateRatio(true)

							//fmt.Printf("Cluster subscribed ratio: %f, host %s (ID=%s) subscription ratio: %f, oversubscription factor: %s\n\n",
							//	dockerCluster.SubscriptionRatio(), host.GetNodeName(), host.GetID(), host.SubscribedRatio(), host.OversubscriptionFactor().StringFixed(4))
						}
					}

					kernelId := uuid.NewString()
					kernelKey := uuid.NewString()
					resourceSpec := proto.NewResourceSpec(1250, 2000, 2, 4)

					kernelSpec := &proto.KernelSpec{
						Id:      kernelId,
						Session: kernelId,
						Argv: []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel",
							"-f", "{connection_file}", "--debug",
							"--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
						SignatureScheme: jupyter.JupyterSignatureScheme,
						Key:             kernelKey,
						ResourceSpec:    resourceSpec,
					}

					By("Correctly returning only 2 candidate hosts due to the other hosts becoming too oversubscribed")

					candidateHosts, err := dockerScheduler.FindCandidateHosts(3, kernelSpec)
					Expect(err).To(BeNil())
					Expect(candidateHosts).ToNot(BeNil())
					Expect(len(candidateHosts)).To(Equal(2))

					By("Releasing the reservations on the 2 candidate hosts without any errors")

					for _, host := range candidateHosts {
						Expect(host.NumReservations()).To(Equal(1))
						err = host.ReleaseReservation(kernelSpec)
						Expect(err).To(BeNil())

						Expect(host.HasAnyReplicaOfKernel(kernelId)).To(BeFalse())
						Expect(host.HasReservationForKernel(kernelId)).To(BeFalse())
					}

					// Ensure that no hosts have a reservation for the kernel.
					for _, host := range hosts {
						Expect(host.HasAnyReplicaOfKernel(kernelId)).To(BeFalse())
						Expect(host.HasReservationForKernel(kernelId)).To(BeFalse())
					}

					Expect(dockerCluster.Len()).To(Equal(5))
					Expect(dockerCluster.NumScalingOperationsAttempted()).To(Equal(0))
					Expect(dockerCluster.NumScaleOutOperationsAttempted()).To(Equal(0))
					Expect(dockerCluster.NumScaleOutOperationsSucceeded()).To(Equal(0))

					By("Correctly identifying three candidate hosts")

					fmt.Printf("\nSearching for candidate hosts again.\n\n")
					candidateHosts, err = dockerScheduler.GetCandidateHosts(context.Background(), kernelSpec)
					Expect(err).To(BeNil())
					Expect(candidateHosts).ToNot(BeNil())
					Expect(len(candidateHosts)).To(Equal(3))

					Expect(dockerCluster.Len()).To(Equal(6))
					Expect(dockerCluster.NumScalingOperationsAttempted()).To(Equal(1))
					Expect(dockerCluster.NumScaleOutOperationsAttempted()).To(Equal(1))
					Expect(dockerCluster.NumScaleOutOperationsSucceeded()).To(Equal(1))

					for _, host := range candidateHosts {
						Expect(host.NumReservations()).To(Equal(1))

						reservation, loaded := host.GetReservation(kernelId)
						Expect(loaded).To(BeTrue())
						// Matches kernel.
						Expect(reservation.GetKernelId()).To(Equal(kernelId))
						// Matches host.
						Expect(reservation.GetHostId()).To(Equal(host.GetID()))
						// Not pending.
						Expect(reservation.IsPending()).To(BeTrue())
						// Created recently.
						Expect(time.Since(reservation.GetTimestamp()) < (time.Second * 5)).To(BeTrue())
						// Correct amount of resources.
						Expect(reservation.ToSpec().Equals(kernelSpec.DecimalSpecFromKernelSpec())).To(BeTrue())
					}
				})

				It("Will select a host with available idle resources when doing so for a replica that is training", func() {
					// hostSpec = types.NewDecimalSpec(8000, 64000, 8, 64)

					validateVariablesNonNil()

					numAdditionalHosts := 3
					for i := numHosts; i < numAdditionalHosts+numHosts; i++ {
						fmt.Printf("Adding host #%d\n", i)
						host, localGatewayClient, resourceSpoofer, err := addHost(i, hostSpec, false, dockerCluster, mockCtrl)
						Expect(err).To(BeNil())
						Expect(host).ToNot(BeNil())
						Expect(localGatewayClient).ToNot(BeNil())
						Expect(resourceSpoofer).ToNot(BeNil())

						if i != (numAdditionalHosts + numHosts - 1) {
							err := host.AddToCommittedResources(types.NewDecimalSpec(0, 0, 8, 32))
							Expect(err).To(BeNil())
							err = host.SubtractFromIdleResources(types.NewDecimalSpec(0, 0, 8, 32))
							Expect(err).To(BeNil())
						}

						hosts[i] = host
						localGatewayClients[i] = localGatewayClient
						resourceSpoofers[i] = resourceSpoofer
					}

					kernelId := uuid.NewString()
					kernelKey := uuid.NewString()
					resourceSpec := proto.NewResourceSpec(1250, 2000, 2, 4)
					dataDirectory := uuid.NewString()
					workloadId := uuid.NewString()

					kernelSpec := &proto.KernelSpec{
						Id:              kernelId,
						Session:         kernelId,
						Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
						SignatureScheme: jupyter.JupyterSignatureScheme,
						Key:             kernelKey,
						ResourceSpec:    resourceSpec,
					}

					kernelReplicaSpec1 := &proto.KernelReplicaSpec{
						Kernel:                    kernelSpec,
						ReplicaId:                 1,
						Join:                      true,
						NumReplicas:               3,
						DockerModeKernelDebugPort: -1,
						PersistentId:              &dataDirectory,
						WorkloadId:                workloadId,
						Replicas:                  []string{"10.0.0.1:8000", "10.0.0.2:8000", "10.0.0.3:8000"},
					}

					kernelReplicaSpec2 := &proto.KernelReplicaSpec{
						Kernel:                    kernelSpec,
						ReplicaId:                 2,
						Join:                      true,
						NumReplicas:               3,
						DockerModeKernelDebugPort: -1,
						PersistentId:              &dataDirectory,
						WorkloadId:                workloadId,
						Replicas:                  []string{"10.0.0.1:8000", "10.0.0.2:8000", "10.0.0.3:8000"},
					}

					kernelReplicaSpec3 := &proto.KernelReplicaSpec{
						Kernel:                    kernelSpec,
						ReplicaId:                 3,
						Join:                      true,
						NumReplicas:               3,
						DockerModeKernelDebugPort: -1,
						PersistentId:              &dataDirectory,
						WorkloadId:                workloadId,
						Replicas:                  []string{"10.0.0.1:8000", "10.0.0.2:8000", "10.0.0.3:8000"},
					}

					host1, loaded := hosts[0]
					Expect(loaded).To(BeTrue())
					Expect(host1).ToNot(BeNil())

					localGatewayClient1, loaded := localGatewayClients[0]
					Expect(loaded).To(BeTrue())
					Expect(localGatewayClient1).ToNot(BeNil())

					host2, loaded := hosts[1]
					Expect(loaded).To(BeTrue())
					Expect(host2).ToNot(BeNil())

					localGatewayClient2, loaded := localGatewayClients[1]
					Expect(loaded).To(BeTrue())
					Expect(localGatewayClient2).ToNot(BeNil())

					host3, loaded := hosts[2]
					Expect(loaded).To(BeTrue())
					Expect(host3).ToNot(BeNil())

					localGatewayClient3, loaded := localGatewayClients[2]
					Expect(loaded).To(BeTrue())
					Expect(localGatewayClient3).ToNot(BeNil())

					localGatewayClient1.EXPECT().PrepareToMigrate(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(&proto.PrepareToMigrateResponse{
						KernelId: kernelId,
						Id:       1,
						DataDir:  dataDirectory,
					}, nil)

					container1 := mock_scheduling.NewMockKernelContainer(mockCtrl)
					container1.EXPECT().ReplicaId().AnyTimes().Return(int32(1))
					container1.EXPECT().KernelID().AnyTimes().Return(kernelId)
					container1.EXPECT().ContainerID().AnyTimes().Return(fmt.Sprintf("%s-%d", kernelId, 1))
					container1.EXPECT().ResourceSpec().AnyTimes().Return(resourceSpec.ToDecimalSpec())
					container1.EXPECT().String().AnyTimes().Return("MockedContainer")

					container2 := mock_scheduling.NewMockKernelContainer(mockCtrl)
					container2.EXPECT().ReplicaId().AnyTimes().Return(int32(2))
					container2.EXPECT().KernelID().AnyTimes().Return(kernelId)
					container2.EXPECT().ContainerID().AnyTimes().Return(fmt.Sprintf("%s-%d", kernelId, 2))
					container2.EXPECT().ResourceSpec().AnyTimes().Return(resourceSpec.ToDecimalSpec())
					container2.EXPECT().String().AnyTimes().Return("MockedContainer")

					container3 := mock_scheduling.NewMockKernelContainer(mockCtrl)
					container3.EXPECT().ReplicaId().AnyTimes().Return(int32(3))
					container3.EXPECT().KernelID().AnyTimes().Return(kernelId)
					container3.EXPECT().ContainerID().AnyTimes().Return(fmt.Sprintf("%s-%d", kernelId, 3))
					container3.EXPECT().ResourceSpec().AnyTimes().Return(resourceSpec.ToDecimalSpec())
					container3.EXPECT().String().AnyTimes().Return("MockedContainer")

					kernelReplica1 := mock_scheduling.NewMockKernelReplica(mockCtrl)
					kernelReplica1.EXPECT().KernelSpec().AnyTimes().Return(kernelSpec)
					kernelReplica1.EXPECT().ReplicaID().AnyTimes().Return(int32(1))
					kernelReplica1.EXPECT().ID().AnyTimes().Return(kernelId)
					kernelReplica1.EXPECT().ResourceSpec().AnyTimes().Return(resourceSpec.ToDecimalSpec())
					kernelReplica1.EXPECT().Container().AnyTimes().Return(container1)
					kernelReplica1.EXPECT().String().AnyTimes().Return("MockedKernelReplica")
					kernelReplica1.EXPECT().KernelReplicaSpec().AnyTimes().Return(kernelReplicaSpec1)

					kernelReplica2 := mock_scheduling.NewMockKernelReplica(mockCtrl)
					kernelReplica2.EXPECT().KernelSpec().AnyTimes().Return(kernelSpec)
					kernelReplica2.EXPECT().ReplicaID().AnyTimes().Return(int32(2))
					kernelReplica2.EXPECT().ID().AnyTimes().Return(kernelId)
					kernelReplica2.EXPECT().ResourceSpec().AnyTimes().Return(resourceSpec.ToDecimalSpec())
					kernelReplica2.EXPECT().Container().AnyTimes().Return(container2)
					kernelReplica2.EXPECT().String().AnyTimes().Return("MockedKernelReplica")
					kernelReplica2.EXPECT().KernelReplicaSpec().AnyTimes().Return(kernelReplicaSpec2)

					kernelReplica3 := mock_scheduling.NewMockKernelReplica(mockCtrl)
					kernelReplica3.EXPECT().KernelSpec().AnyTimes().Return(kernelSpec)
					kernelReplica3.EXPECT().ReplicaID().AnyTimes().Return(int32(3))
					kernelReplica3.EXPECT().ID().AnyTimes().Return(kernelId)
					kernelReplica3.EXPECT().ResourceSpec().AnyTimes().Return(resourceSpec.ToDecimalSpec())
					kernelReplica3.EXPECT().Container().AnyTimes().Return(container3)
					kernelReplica3.EXPECT().String().AnyTimes().Return("MockedKernelReplica")
					kernelReplica3.EXPECT().KernelReplicaSpec().AnyTimes().Return(kernelReplicaSpec3)

					kernel := mock_scheduling.NewMockKernel(mockCtrl)
					kernel.EXPECT().KernelSpec().AnyTimes().Return(kernelSpec)
					kernel.EXPECT().ID().AnyTimes().Return(kernelId)
					kernel.EXPECT().ResourceSpec().AnyTimes().Return(resourceSpec.ToDecimalSpec())

					success, err := host1.ReserveResources(kernelSpec, true)
					Expect(success).To(BeTrue())
					Expect(err).To(BeNil())

					err = host1.ContainerStartedRunningOnHost(container1)
					Expect(err).To(BeNil())
					container1.EXPECT().Host().AnyTimes().Return(host1)
					kernelReplica1.EXPECT().Host().AnyTimes().Return(host1)

					success, err = host2.ReserveResources(kernelSpec, true)
					Expect(success).To(BeTrue())
					Expect(err).To(BeNil())

					err = host2.ContainerStartedRunningOnHost(container2)
					Expect(err).To(BeNil())
					container2.EXPECT().Host().AnyTimes().Return(host2)
					kernelReplica2.EXPECT().Host().AnyTimes().Return(host2)

					success, err = host3.ReserveResources(kernelSpec, true)
					Expect(success).To(BeTrue())
					Expect(err).To(BeNil())

					err = host3.ContainerStartedRunningOnHost(container3)
					Expect(err).To(BeNil())
					container3.EXPECT().Host().AnyTimes().Return(host3)
					kernelReplica3.EXPECT().Host().AnyTimes().Return(host3)

					kernelProvider.EXPECT().GetKernel(gomock.Any()).AnyTimes().DoAndReturn(func(id string) (scheduling.Kernel, bool) {
						if id == kernelId {
							return kernel, true
						}

						return kernel, false
					})

					hostMapper.EXPECT().GetHostsOfKernel(kernelId).AnyTimes().Return([]scheduling.Host{host1, host2, host3}, nil)

					nextKernelId := uuid.NewString()
					nextKernelKey := uuid.NewString()
					nextResourceSpec := proto.NewResourceSpec(1250, 2000, 4, 4)

					nextKernelSpec := &proto.KernelSpec{
						Id:              nextKernelId,
						Session:         nextKernelId,
						Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
						SignatureScheme: jupyter.JupyterSignatureScheme,
						Key:             nextKernelKey,
						ResourceSpec:    nextResourceSpec,
					}

					for i := 0; i < numHosts; i++ {
						host := hosts[i]
						err := host.AddToCommittedResources(types.NewDecimalSpec(0, 0, 8, 32))
						Expect(err).To(BeNil())
						err = host.SubtractFromIdleResources(types.NewDecimalSpec(0, 0, 8, 32))
						Expect(err).To(BeNil())
					}

					candidates, err := dockerScheduler.GetCandidateHosts(context.Background(), nextKernelSpec)
					Expect(err).To(BeNil())
					Expect(candidates).ToNot(BeNil())

					for _, candidate := range candidates {
						fmt.Printf("host %s resources: %s\n", candidate.GetID(), candidate.GetResourceCountsAsString())
						Expect(candidate.IdleResources().GPUs.IsZero()).To(BeTrue())
					}

					nextDataDirectory := uuid.NewString()
					nextKernelReplicaSpec := &proto.KernelReplicaSpec{
						Kernel:                    nextKernelSpec,
						ReplicaId:                 2,
						Join:                      true,
						NumReplicas:               3,
						DockerModeKernelDebugPort: -1,
						PersistentId:              &nextDataDirectory,
						WorkloadId:                workloadId,
						Replicas:                  []string{"10.0.0.4:8000", "10.0.0.5:8000", "10.0.0.6:8000"},
					}

					nextContainer := mock_scheduling.NewMockKernelContainer(mockCtrl)
					nextContainer.EXPECT().ReplicaId().AnyTimes().Return(int32(2))
					nextContainer.EXPECT().KernelID().AnyTimes().Return(nextKernelId)
					nextContainer.EXPECT().ContainerID().AnyTimes().Return(fmt.Sprintf("%s-%d", nextKernelId, 2))
					nextContainer.EXPECT().ResourceSpec().AnyTimes().Return(nextResourceSpec.ToDecimalSpec())
					nextContainer.EXPECT().String().AnyTimes().Return("NextMockedContainer")

					nextKernelReplica := mock_scheduling.NewMockKernelReplica(mockCtrl)
					nextKernelReplica.EXPECT().KernelSpec().AnyTimes().Return(nextKernelSpec)
					nextKernelReplica.EXPECT().ReplicaID().AnyTimes().Return(int32(2))
					nextKernelReplica.EXPECT().ID().AnyTimes().Return(nextKernelId)
					nextKernelReplica.EXPECT().ResourceSpec().AnyTimes().Return(nextResourceSpec.ToDecimalSpec())
					nextKernelReplica.EXPECT().Container().AnyTimes().Return(nextContainer)
					nextKernelReplica.EXPECT().String().AnyTimes().Return("NextMockedKernelReplica")
					nextKernelReplica.EXPECT().KernelReplicaSpec().AnyTimes().Return(nextKernelReplicaSpec)
					nextKernelReplica.EXPECT().Host().Times(1).Return(nil)

					hostMapper.EXPECT().GetHostsOfKernel(nextKernelId).AnyTimes().Return([]scheduling.Host{}, nil)

					host, err := dockerScheduler.GetCandidateHost(nextKernelReplica, nil, true)
					Expect(err).To(BeNil())
					Expect(host).ToNot(BeNil())
					Expect(host.IdleResources().IsZero()).To(BeFalse())
				})

				It("Will correctly migrate a kernel container that needs to train", func() {
					validateVariablesNonNil()

					numAdditionalHosts := 3
					for i := numHosts; i < numAdditionalHosts+numHosts; i++ {
						fmt.Printf("Adding host #%d\n", i)
						host, localGatewayClient, resourceSpoofer, err := addHost(i, hostSpec, false, dockerCluster, mockCtrl)
						Expect(err).To(BeNil())
						Expect(host).ToNot(BeNil())
						Expect(localGatewayClient).ToNot(BeNil())
						Expect(resourceSpoofer).ToNot(BeNil())

						if i != (numAdditionalHosts + numHosts - 1) {
							err := host.AddToCommittedResources(types.NewDecimalSpec(0, 0, 8, 32))
							Expect(err).To(BeNil())
							err = host.SubtractFromIdleResources(types.NewDecimalSpec(0, 0, 8, 32))
							Expect(err).To(BeNil())
						}

						hosts[i] = host
						localGatewayClients[i] = localGatewayClient
						resourceSpoofers[i] = resourceSpoofer
					}

					kernelId := uuid.NewString()
					kernelKey := uuid.NewString()
					resourceSpec := proto.NewResourceSpec(1250, 2000, 2, 4)
					dataDirectory := uuid.NewString()
					workloadId := uuid.NewString()

					kernelSpec := &proto.KernelSpec{
						Id:              kernelId,
						Session:         kernelId,
						Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
						SignatureScheme: jupyter.JupyterSignatureScheme,
						Key:             kernelKey,
						ResourceSpec:    resourceSpec,
					}

					kernelReplicaSpec1 := &proto.KernelReplicaSpec{
						Kernel:                    kernelSpec,
						ReplicaId:                 1,
						Join:                      true,
						NumReplicas:               3,
						DockerModeKernelDebugPort: -1,
						PersistentId:              &dataDirectory,
						WorkloadId:                workloadId,
						Replicas:                  []string{"10.0.0.1:8000", "10.0.0.2:8000", "10.0.0.3:8000"},
					}

					kernelReplicaSpec2 := &proto.KernelReplicaSpec{
						Kernel:                    kernelSpec,
						ReplicaId:                 2,
						Join:                      true,
						NumReplicas:               3,
						DockerModeKernelDebugPort: -1,
						PersistentId:              &dataDirectory,
						WorkloadId:                workloadId,
						Replicas:                  []string{"10.0.0.1:8000", "10.0.0.2:8000", "10.0.0.3:8000"},
					}

					kernelReplicaSpec3 := &proto.KernelReplicaSpec{
						Kernel:                    kernelSpec,
						ReplicaId:                 3,
						Join:                      true,
						NumReplicas:               3,
						DockerModeKernelDebugPort: -1,
						PersistentId:              &dataDirectory,
						WorkloadId:                workloadId,
						Replicas:                  []string{"10.0.0.1:8000", "10.0.0.2:8000", "10.0.0.3:8000"},
					}

					host1, loaded := hosts[0]
					Expect(loaded).To(BeTrue())
					Expect(host1).ToNot(BeNil())

					localGatewayClient1, loaded := localGatewayClients[0]
					Expect(loaded).To(BeTrue())
					Expect(localGatewayClient1).ToNot(BeNil())

					host2, loaded := hosts[1]
					Expect(loaded).To(BeTrue())
					Expect(host2).ToNot(BeNil())

					localGatewayClient2, loaded := localGatewayClients[1]
					Expect(loaded).To(BeTrue())
					Expect(localGatewayClient2).ToNot(BeNil())

					host3, loaded := hosts[2]
					Expect(loaded).To(BeTrue())
					Expect(host3).ToNot(BeNil())

					localGatewayClient3, loaded := localGatewayClients[2]
					Expect(loaded).To(BeTrue())
					Expect(localGatewayClient3).ToNot(BeNil())

					localGatewayClient1.EXPECT().PrepareToMigrate(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(&proto.PrepareToMigrateResponse{
						KernelId: kernelId,
						Id:       1,
						DataDir:  dataDirectory,
					}, nil)

					container1 := mock_scheduling.NewMockKernelContainer(mockCtrl)
					container1.EXPECT().ReplicaId().AnyTimes().Return(int32(1))
					container1.EXPECT().KernelID().AnyTimes().Return(kernelId)
					container1.EXPECT().ContainerID().AnyTimes().Return(fmt.Sprintf("%s-%d", kernelId, 1))
					container1.EXPECT().ResourceSpec().AnyTimes().Return(resourceSpec.ToDecimalSpec())
					container1.EXPECT().String().AnyTimes().Return("MockedContainer")

					container2 := mock_scheduling.NewMockKernelContainer(mockCtrl)
					container2.EXPECT().ReplicaId().AnyTimes().Return(int32(2))
					container2.EXPECT().KernelID().AnyTimes().Return(kernelId)
					container2.EXPECT().ContainerID().AnyTimes().Return(fmt.Sprintf("%s-%d", kernelId, 2))
					container2.EXPECT().ResourceSpec().AnyTimes().Return(resourceSpec.ToDecimalSpec())
					container2.EXPECT().String().AnyTimes().Return("MockedContainer")

					container3 := mock_scheduling.NewMockKernelContainer(mockCtrl)
					container3.EXPECT().ReplicaId().AnyTimes().Return(int32(3))
					container3.EXPECT().KernelID().AnyTimes().Return(kernelId)
					container3.EXPECT().ContainerID().AnyTimes().Return(fmt.Sprintf("%s-%d", kernelId, 3))
					container3.EXPECT().ResourceSpec().AnyTimes().Return(resourceSpec.ToDecimalSpec())
					container3.EXPECT().String().AnyTimes().Return("MockedContainer")

					kernelReplica1 := mock_scheduling.NewMockKernelReplica(mockCtrl)
					kernelReplica1.EXPECT().KernelSpec().AnyTimes().Return(kernelSpec)
					kernelReplica1.EXPECT().ReplicaID().AnyTimes().Return(int32(1))
					kernelReplica1.EXPECT().ID().AnyTimes().Return(kernelId)
					kernelReplica1.EXPECT().ResourceSpec().AnyTimes().Return(resourceSpec.ToDecimalSpec())
					kernelReplica1.EXPECT().Container().AnyTimes().Return(container1)
					kernelReplica1.EXPECT().String().AnyTimes().Return("MockedKernelReplica")
					kernelReplica1.EXPECT().KernelReplicaSpec().AnyTimes().Return(kernelReplicaSpec1)
					kernelReplica1.EXPECT().PersistentID().AnyTimes().Return(dataDirectory)

					kernelReplica2 := mock_scheduling.NewMockKernelReplica(mockCtrl)
					kernelReplica2.EXPECT().KernelSpec().AnyTimes().Return(kernelSpec)
					kernelReplica2.EXPECT().ReplicaID().AnyTimes().Return(int32(2))
					kernelReplica2.EXPECT().ID().AnyTimes().Return(kernelId)
					kernelReplica2.EXPECT().ResourceSpec().AnyTimes().Return(resourceSpec.ToDecimalSpec())
					kernelReplica2.EXPECT().Container().AnyTimes().Return(container2)
					kernelReplica2.EXPECT().String().AnyTimes().Return("MockedKernelReplica")
					kernelReplica2.EXPECT().KernelReplicaSpec().AnyTimes().Return(kernelReplicaSpec2)
					kernelReplica2.EXPECT().PersistentID().AnyTimes().Return(dataDirectory)

					kernelReplica3 := mock_scheduling.NewMockKernelReplica(mockCtrl)
					kernelReplica3.EXPECT().KernelSpec().AnyTimes().Return(kernelSpec)
					kernelReplica3.EXPECT().ReplicaID().AnyTimes().Return(int32(3))
					kernelReplica3.EXPECT().ID().AnyTimes().Return(kernelId)
					kernelReplica3.EXPECT().ResourceSpec().AnyTimes().Return(resourceSpec.ToDecimalSpec())
					kernelReplica3.EXPECT().Container().AnyTimes().Return(container3)
					kernelReplica3.EXPECT().String().AnyTimes().Return("MockedKernelReplica")
					kernelReplica3.EXPECT().KernelReplicaSpec().AnyTimes().Return(kernelReplicaSpec3)
					kernelReplica3.EXPECT().PersistentID().AnyTimes().Return(dataDirectory)

					kernel := mock_scheduling.NewMockKernel(mockCtrl)
					kernel.EXPECT().KernelSpec().AnyTimes().Return(kernelSpec)
					kernel.EXPECT().ID().AnyTimes().Return(kernelId)
					kernel.EXPECT().ResourceSpec().AnyTimes().Return(resourceSpec.ToDecimalSpec())

					success, err := host1.ReserveResources(kernelSpec, true)
					Expect(success).To(BeTrue())
					Expect(err).To(BeNil())

					err = host1.ContainerStartedRunningOnHost(container1)
					Expect(err).To(BeNil())
					container1.EXPECT().Host().AnyTimes().Return(host1)
					kernelReplica1.EXPECT().Host().AnyTimes().Return(host1)

					success, err = host2.ReserveResources(kernelSpec, true)
					Expect(success).To(BeTrue())
					Expect(err).To(BeNil())

					err = host2.ContainerStartedRunningOnHost(container2)
					Expect(err).To(BeNil())
					container2.EXPECT().Host().AnyTimes().Return(host2)
					kernelReplica2.EXPECT().Host().AnyTimes().Return(host2)

					success, err = host3.ReserveResources(kernelSpec, true)
					Expect(success).To(BeTrue())
					Expect(err).To(BeNil())

					err = host3.ContainerStartedRunningOnHost(container3)
					Expect(err).To(BeNil())
					container3.EXPECT().Host().AnyTimes().Return(host3)
					kernelReplica3.EXPECT().Host().AnyTimes().Return(host3)

					kernelProvider.EXPECT().GetKernel(gomock.Any()).AnyTimes().DoAndReturn(func(id string) (scheduling.Kernel, bool) {
						if id == kernelId {
							return kernel, true
						}

						return kernel, false
					})

					hostMapper.EXPECT().GetHostsOfKernel(kernelId).AnyTimes().Return([]scheduling.Host{host1, host2, host3}, nil)

					for i := 0; i < numHosts; i++ {
						host := hosts[i]
						err := host.AddToCommittedResources(types.NewDecimalSpec(0, 0, 8, 32))
						Expect(err).To(BeNil())
						err = host.SubtractFromIdleResources(types.NewDecimalSpec(0, 0, 8, 32))
						Expect(err).To(BeNil())
					}

					session := mock_scheduling.NewMockUserSession(mockCtrl)
					session.EXPECT().ID().AnyTimes().Return(kernelId)
					session.EXPECT().GetReplicaContainer(int32(1)).AnyTimes().Return(container1, true)
					session.EXPECT().GetReplicaContainer(int32(2)).AnyTimes().Return(container2, true)
					session.EXPECT().GetReplicaContainer(int32(3)).AnyTimes().Return(container3, true)
					container1.EXPECT().Session().AnyTimes().Return(session)

					kernel.EXPECT().RemoveReplicaByID(int32(1), gomock.Any(), false).Times(1).Return(host1, nil)
					session.EXPECT().RemoveReplicaById(int32(1)).Times(1).Return(nil)

					var addOpActive atomic.Bool
					addOpActive.Store(false)

					kernel.EXPECT().AddOperationStarted().Times(1).Do(func() {
						addOpActive.Store(true)
					})
					kernel.EXPECT().AddOperationCompleted().Times(1).Do(func() {
						addOpActive.Store(false)
					})
					kernel.EXPECT().NumActiveMigrationOperations().AnyTimes().DoAndReturn(func() int {
						if addOpActive.Load() {
							return 1
						}

						return 0
					})
					returnedSpec := &proto.KernelReplicaSpec{
						Kernel:       kernelSpec,
						NumReplicas:  3,
						Join:         true,
						PersistentId: &dataDirectory,
						ReplicaId:    int32(1),
					}
					kernel.EXPECT().PrepareNewReplica(dataDirectory, int32(1)).Times(1).Return(returnedSpec)
					kernel.EXPECT().GetReplicaByID(int32(1)).Times(1).Return(kernelReplica1, nil)
					kernelReplica1.EXPECT().SetReady().Times(1)

					targetGatewayClient := localGatewayClients[5]
					Expect(targetGatewayClient).ToNot(BeNil())
					targetGatewayClient.EXPECT().StartKernelReplica(gomock.Any(), returnedSpec, gomock.Any()).Times(1).DoAndReturn(func(ctx context.Context, in *proto.KernelReplicaSpec, opts ...grpc.CallOption) (*proto.KernelConnectionInfo, error) {
						fmt.Printf("StartKernelReplica called on LocalGateway #%d with kernelReplicaSpec:\n%v\n", 5, in)

						return &proto.KernelConnectionInfo{
							Ip:              "10.0.0.1",
							Transport:       "tcp",
							ControlPort:     9000,
							ShellPort:       9001,
							StdinPort:       9002,
							HbPort:          9003,
							IopubPort:       9004,
							IosubPort:       9005,
							SignatureScheme: jupyter.JupyterSignatureScheme,
							Key:             uuid.NewString(),
						}, nil
					}).AnyTimes()

					var wg sync.WaitGroup
					wg.Add(1)

					go func() {
						// defer GinkgoRecover()
						resp, reason, err := dockerScheduler.MigrateKernelReplica(kernelReplica1, "", true)
						Expect(err).To(BeNil())
						Expect(reason).To(BeNil())
						Expect(resp).ToNot(BeNil())
						fmt.Printf("Response: %v\n", resp)
						wg.Done()
					}()

					//Eventually(migrateReplica).Should(Not(Panic()))

					key := fmt.Sprintf("%s-%d", kernelId, 1)
					getAddReplicaOp := func() *scheduling.AddReplicaOperation {
						addReplicaOp, _ := dockerScheduler.GetAddReplicaOperation(key)
						return addReplicaOp
					}

					Eventually(getAddReplicaOp, "2s").ShouldNot(BeNil())

					addReplicaOp, loaded := dockerScheduler.GetAddReplicaOperationManager().Load(key)
					Expect(loaded).To(BeTrue())
					Expect(addReplicaOp).ToNot(BeNil())

					addReplicaOp.SetReplicaStarted()

					addReplicaOp.SetContainerName("UnitTestDockerContainer")

					err = addReplicaOp.SetReplicaRegistered()
					Expect(err).To(BeNil())

					addReplicaOp.SetReplicaJoinedSMR()

					Expect(addReplicaOp.ReplicaRegistered()).To(BeTrue())
					Expect(addReplicaOp.ReplicaJoinedSMR()).To(BeTrue())
					Expect(addReplicaOp.PodOrContainerStarted()).To(BeTrue())

					wg.Wait()
				})
			})

			It("Will correctly return whatever viable hosts it finds, even if it cannot find all of them, via the FindCandidateHosts method", func() {
				hosts = make(map[int]scheduling.UnitTestingHost)
				localGatewayClients = make(map[int]*mock_proto.MockLocalGatewayClient)
				resourceSpoofers = make(map[int]*distNbTesting.ResourceSpoofer)

				largerHostSpec := types.NewDecimalSpec(8000, 64000, 8, 64)

				// Create a new, larger host.
				i := len(hosts)
				bigHost1, _, _, err := addHost(i, largerHostSpec, false, dockerCluster, mockCtrl)
				Expect(err).To(BeNil())
				Expect(bigHost1).ToNot(BeNil())

				hosts[i] = bigHost1

				Expect(dockerCluster.Len()).To(Equal(1))

				kernelId := uuid.NewString()
				kernelKey := uuid.NewString()
				resourceSpec := proto.NewResourceSpec(1250, 2000, 8, 4)

				kernelSpec := &proto.KernelSpec{
					Id:              kernelId,
					Session:         kernelId,
					Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
					SignatureScheme: jupyter.JupyterSignatureScheme,
					Key:             kernelKey,
					ResourceSpec:    resourceSpec,
				}

				candidateHosts, err := dockerScheduler.FindCandidateHosts(3, kernelSpec)
				Expect(err).To(BeNil())
				Expect(candidateHosts).ToNot(BeNil())
				Expect(len(candidateHosts)).To(Equal(1))
				Expect(candidateHosts[0]).To(Equal(bigHost1))

				Expect(bigHost1.NumReservations()).To(Equal(1))
				Expect(bigHost1.PendingResources().Equals(kernelSpec.DecimalSpecFromKernelSpec())).To(BeTrue())

				i = len(hosts)
				bigHost2, _, _, err := addHost(i, largerHostSpec, false, dockerCluster, mockCtrl)
				Expect(err).To(BeNil())
				Expect(bigHost2).ToNot(BeNil())

				hosts[i] = bigHost2

				hostBatch, err := dockerScheduler.FindCandidateHosts(3-len(candidateHosts), kernelSpec)
				Expect(err).To(BeNil())
				Expect(hostBatch).ToNot(BeNil())

				candidateHosts = append(candidateHosts, hostBatch...)
				Expect(len(candidateHosts)).To(Equal(2))
				Expect(candidateHosts[0]).To(Equal(bigHost1))
				Expect(candidateHosts[1]).To(Equal(bigHost2))

				Expect(bigHost1.NumReservations()).To(Equal(1))
				Expect(bigHost1.PendingResources().Equals(kernelSpec.DecimalSpecFromKernelSpec())).To(BeTrue())

				Expect(bigHost2.NumReservations()).To(Equal(1))
				Expect(bigHost2.PendingResources().Equals(kernelSpec.DecimalSpecFromKernelSpec())).To(BeTrue())

				smallKernelKey := uuid.NewString()
				smallKernelId := uuid.NewString()

				smallResourceSpec := proto.NewResourceSpec(1250, 2000, 2, 4)
				smallKernelSpec := &proto.KernelSpec{
					Id:              smallKernelId,
					Session:         smallKernelId,
					Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
					SignatureScheme: jupyter.JupyterSignatureScheme,
					Key:             smallKernelKey,
					ResourceSpec:    smallResourceSpec,
				}

				// Remove two of the smaller hosts so that the only hosts left are one small host and two big hosts.
				dockerCluster.RemoveHost(hosts[0].GetID())
				dockerCluster.RemoveHost(hosts[1].GetID())

				Expect(dockerCluster.Len()).To(Equal(0))

				for i := 0; i < 3; i++ {
					host, localGatewayClient, resourceSpoofer, err := addHost(i, hostSpec, false, dockerCluster, mockCtrl)
					Expect(err).To(BeNil())
					Expect(host).ToNot(BeNil())
					Expect(localGatewayClient).ToNot(BeNil())
					Expect(resourceSpoofer).ToNot(BeNil())

					hosts[i] = host
					localGatewayClients[i] = localGatewayClient
					resourceSpoofers[i] = resourceSpoofer
				}

				Expect(dockerCluster.Len()).To(Equal(3))

				candidateHosts, err = dockerScheduler.GetCandidateHosts(context.Background(), smallKernelSpec)
				Expect(err).To(BeNil())
				Expect(candidateHosts).ToNot(BeNil())
				Expect(len(candidateHosts)).To(Equal(3))

				combinedSpec := smallKernelSpec.DecimalSpecFromKernelSpec().Add(kernelSpec.DecimalSpecFromKernelSpec())

				for _, candidateHost := range candidateHosts {
					if candidateHost.ResourceSpec().GPU() > 8 {
						Expect(candidateHost.NumReservations()).To(Equal(2))
						Expect(candidateHost.PendingResources().Equals(combinedSpec)).To(BeTrue())
					} else {
						Expect(candidateHost.NumReservations()).To(Equal(1))
						Expect(candidateHost.PendingResources().Equals(smallKernelSpec.DecimalSpecFromKernelSpec())).To(BeTrue())
					}
				}
			})

		})

		Context("Scaling Operations", func() {
			var numHosts int
			var hosts map[int]scheduling.Host
			var localGatewayClients map[int]*mock_proto.MockLocalGatewayClient
			var resourceSpoofers map[int]*distNbTesting.ResourceSpoofer

			BeforeEach(func() {
				hosts = make(map[int]scheduling.Host)
				localGatewayClients = make(map[int]*mock_proto.MockLocalGatewayClient)
				resourceSpoofers = make(map[int]*distNbTesting.ResourceSpoofer)
				numHosts = 3

				for i := 0; i < numHosts; i++ {
					host, localGatewayClient, resourceSpoofer, err := addHost(i, hostSpec, false, dockerCluster, mockCtrl)
					Expect(err).To(BeNil())
					Expect(host).ToNot(BeNil())
					Expect(localGatewayClient).ToNot(BeNil())
					Expect(resourceSpoofer).ToNot(BeNil())

					hosts[i] = host
					localGatewayClients[i] = localGatewayClient
					resourceSpoofers[i] = resourceSpoofer
				}
			})

			validateVariablesNonNil := func() {
				Expect(dockerScheduler).ToNot(BeNil())
				Expect(clusterPlacer).ToNot(BeNil())
				Expect(dockerCluster).ToNot(BeNil())
				Expect(hostMapper).ToNot(BeNil())
				Expect(len(hosts)).To(Equal(3))
				Expect(len(localGatewayClients)).To(Equal(3))
				Expect(len(resourceSpoofers)).To(Equal(3))
			}

			It("Will correctly do nothing when scale-to-size is passed the current dockerCluster size", func() {
				validateVariablesNonNil()

				initialSize := len(hosts)

				p := dockerCluster.ScaleToSize(context.Background(), int32(initialSize))
				Expect(p).ToNot(BeNil())

				err := p.Error()
				GinkgoWriter.Printf("Error: %v\n", err)
				Expect(err).ToNot(BeNil())
				Expect(errors.Is(err, scheduling.ErrInvalidTargetNumHosts)).To(BeTrue())
			})

			It("Will correctly do nothing when instructed to scale below minimum dockerCluster size", func() {
				validateVariablesNonNil()

				initialSize := len(hosts)
				Expect(initialSize).To(Equal(dockerCluster.NumReplicas()))

				p := dockerCluster.ScaleToSize(context.Background(), 0)
				Expect(p).ToNot(BeNil())

				err := p.Error()
				GinkgoWriter.Printf("Error: %v\n", err)
				Expect(err).ToNot(BeNil())
				Expect(errors.Is(err, scheduling.ErrInvalidTargetNumHosts)).To(BeTrue())
			})

			It("Will correctly scale-out", func() {
				validateVariablesNonNil()

				initialSize := len(hosts)

				// First, add three more disabled hosts, so that there are 6 hosts total -- 3 active and 3 inactive.
				for i := initialSize; i < initialSize+3; i++ {
					host, localGatewayClient, resourceSpoofer, err := addHost(i, hostSpec, true, dockerCluster, mockCtrl)
					Expect(err).To(BeNil())
					Expect(host).ToNot(BeNil())
					Expect(localGatewayClient).ToNot(BeNil())
					Expect(resourceSpoofer).ToNot(BeNil())

					hosts[i] = host
					localGatewayClients[i] = localGatewayClient
					resourceSpoofers[i] = resourceSpoofer
				}

				targetNumHosts := int32(initialSize * 2)

				Expect(dockerCluster.Len()).To(Equal(initialSize))
				Expect(dockerCluster.NumDisabledHosts()).To(Equal(initialSize))

				p := dockerCluster.ScaleToSize(context.Background(), targetNumHosts)
				Expect(p).ToNot(BeNil())
				Expect(p.Error()).To(BeNil())

				Expect(dockerCluster.Len()).To(Equal(int(targetNumHosts)))
				Expect(dockerCluster.NumDisabledHosts()).To(Equal(0))
			})

			It("Will correctly scale-in", func() {
				initialSize := len(hosts)

				// First, add three more hosts, so that there are 6 active hosts.
				for i := initialSize; i < initialSize+3; i++ {
					host, localGatewayClient, resourceSpoofer, err := addHost(i, hostSpec, false, dockerCluster, mockCtrl)
					Expect(err).To(BeNil())
					Expect(host).ToNot(BeNil())
					Expect(localGatewayClient).ToNot(BeNil())
					Expect(resourceSpoofer).ToNot(BeNil())

					hosts[i] = host
					localGatewayClients[i] = localGatewayClient
					resourceSpoofers[i] = resourceSpoofer
				}

				Expect(dockerCluster.Len()).To(Equal(initialSize * 2))
				Expect(dockerCluster.NumDisabledHosts()).To(Equal(0))

				p := dockerCluster.ScaleToSize(context.Background(), int32(initialSize))
				Expect(p).ToNot(BeNil())
				Expect(p.Error()).To(BeNil())

				Expect(dockerCluster.Len()).To(Equal(int(initialSize)))
				Expect(dockerCluster.NumDisabledHosts()).To(Equal(3))
			})
		})
	})

	Context("Reservation-Based Scheduling", func() {
		BeforeEach(func() {
			mockCtrl = gomock.NewController(GinkgoT())

			kernelProvider = mock_scheduler.NewMockKernelProvider(mockCtrl)
			hostMapper = mock_scheduler.NewMockHostMapper(mockCtrl)

			opts.SchedulingPolicy = string(scheduling.Reservation)
			schedulingPolicy, err := scheduler.GetSchedulingPolicy(&opts.SchedulerOptions)
			Expect(err).To(BeNil())
			Expect(schedulingPolicy).ToNot(BeNil())
			Expect(schedulingPolicy.NumReplicas()).To(Equal(1))
			Expect(schedulingPolicy.Name()).To(Equal("Reservation-Based"))
			Expect(schedulingPolicy.ResourceBindingMode()).To(Equal(scheduling.BindResourcesWhenContainerScheduled))

			//clusterPlacer, err = placer.NewRandomPlacer(nil, schedulingPolicy.NumReplicas(), schedulingPolicy)
			clusterPlacer, err = schedulingPolicy.GetNewPlacer(nil)
			Expect(err).To(BeNil())
			Expect(clusterPlacer).ToNot(BeNil())
			_, ok := clusterPlacer.(*placer.BasicPlacer)
			Expect(ok).To(BeTrue())

			_, ok = clusterPlacer.GetIndex().(*index.RandomClusterIndex)
			Expect(ok).To(BeTrue())

			dockerCluster = cluster.NewDockerCluster(hostSpec, clusterPlacer, hostMapper, kernelProvider,
				nil, nil, schedulingPolicy, func(f func(stats *metrics.ClusterStatistics)) {},
				&opts.ClusterDaemonOptions.SchedulerOptions)

			Expect(dockerCluster).ToNot(BeNil())

			genericScheduler := dockerCluster.Scheduler()
			Expect(genericScheduler).ToNot(BeNil())

			dockerScheduler, ok = genericScheduler.(*scheduler.DockerScheduler)
			Expect(ok).To(BeTrue())
			Expect(dockerScheduler).ToNot(BeNil())
		})

		Context("Will handle basic scheduling operations correctly", func() {
			var numHosts int
			var hosts map[int]scheduling.Host
			var localGatewayClients map[int]*mock_proto.MockLocalGatewayClient
			var resourceSpoofers map[int]*distNbTesting.ResourceSpoofer

			BeforeEach(func() {
				hosts = make(map[int]scheduling.Host)
				localGatewayClients = make(map[int]*mock_proto.MockLocalGatewayClient)
				resourceSpoofers = make(map[int]*distNbTesting.ResourceSpoofer)
				numHosts = 3

				for i := 0; i < numHosts; i++ {
					host, localGatewayClient, resourceSpoofer, err := addHost(i, hostSpec, false, dockerCluster, mockCtrl)
					Expect(err).To(BeNil())
					Expect(host).ToNot(BeNil())
					Expect(localGatewayClient).ToNot(BeNil())
					Expect(resourceSpoofer).ToNot(BeNil())

					hosts[i] = host
					localGatewayClients[i] = localGatewayClient
					resourceSpoofers[i] = resourceSpoofer
				}
			})

			validateVariablesNonNil := func() {
				Expect(dockerScheduler).ToNot(BeNil())
				Expect(clusterPlacer).ToNot(BeNil())
				Expect(dockerCluster).ToNot(BeNil())
				Expect(hostMapper).ToNot(BeNil())
				Expect(len(hosts)).To(Equal(3))
				Expect(len(localGatewayClients)).To(Equal(3))
				Expect(len(resourceSpoofers)).To(Equal(3))
			}

			It("Will correctly identify candidate hosts when candidate hosts are available", func() {
				validateVariablesNonNil()

				kernelId := uuid.NewString()
				kernelKey := uuid.NewString()
				resourceSpec := proto.NewResourceSpec(1250, 2000, 2, 4)

				kernelSpec := &proto.KernelSpec{
					Id:              kernelId,
					Session:         kernelId,
					Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
					SignatureScheme: jupyter.JupyterSignatureScheme,
					Key:             kernelKey,
					ResourceSpec:    resourceSpec,
				}

				candidateHosts, err := dockerScheduler.GetCandidateHosts(context.Background(), kernelSpec)
				Expect(err).To(BeNil())
				Expect(candidateHosts).ToNot(BeNil())
				Expect(len(candidateHosts)).To(Equal(1))

				for _, host := range candidateHosts {
					Expect(host.NumReservations()).To(Equal(1))
					Expect(host.PendingResources().Equals(kernelSpec.DecimalSpecFromKernelSpec())).To(BeFalse())
					Expect(host.CommittedResources().Equals(kernelSpec.DecimalSpecFromKernelSpec())).To(BeTrue())

					reservation, loaded := host.GetReservation(kernelId)
					Expect(loaded).To(BeTrue())
					// Matches kernel.
					Expect(reservation.GetKernelId()).To(Equal(kernelId))
					// Matches host.
					Expect(reservation.GetHostId()).To(Equal(host.GetID()))
					// Not pending.
					Expect(reservation.IsPending()).To(BeFalse())
					// Created recently.
					Expect(time.Since(reservation.GetTimestamp()) < (time.Second * 5)).To(BeTrue())
					// Correct amount of resources.
					Expect(reservation.ToSpec().Equals(kernelSpec.DecimalSpecFromKernelSpec())).To(BeTrue())
				}
			})

			It("Will correctly return an error when no candidate hosts are available", func() {
				validateVariablesNonNil()

				kernelId := uuid.NewString()
				kernelKey := uuid.NewString()
				resourceSpec := proto.NewResourceSpec(1250, 2000, 64 /* too many */, 4)

				kernelSpec := &proto.KernelSpec{
					Id:              kernelId,
					Session:         kernelId,
					Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
					SignatureScheme: jupyter.JupyterSignatureScheme,
					Key:             kernelKey,
					ResourceSpec:    resourceSpec,
				}

				candidateHosts, err := dockerScheduler.GetCandidateHosts(context.Background(), kernelSpec)
				Expect(err).ToNot(BeNil())
				Expect(candidateHosts).To(BeNil())
				Expect(len(candidateHosts)).To(Equal(0))
				fmt.Printf("Error: %v\n", err)
			})

			It("Will correctly return whatever viable hosts it finds, even if it cannot find all of them, via the FindCandidateHosts method", func() {
				validateVariablesNonNil()

				largerHostSpec := types.NewDecimalSpec(8000, 64000, 64, 32)

				// Create a new, larger host.
				i := len(hosts)
				bigHost1, _, _, err := addHost(i, largerHostSpec, false, dockerCluster, mockCtrl)
				GinkgoWriter.Printf("Created bigHost1: name=%s & ID=%s\n", bigHost1.GetNodeName(), bigHost1.GetID())
				Expect(err).To(BeNil())
				Expect(bigHost1).ToNot(BeNil())

				hosts[i] = bigHost1

				Expect(dockerCluster.Len()).To(Equal(4))

				kernelId := uuid.NewString()
				kernelKey := uuid.NewString()
				bigResourceSpec := proto.NewResourceSpec(1250, 2000, 32 /* too many */, 4)

				bigKernelSpec := &proto.KernelSpec{
					Id:              kernelId,
					Session:         kernelId,
					Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
					SignatureScheme: jupyter.JupyterSignatureScheme,
					Key:             kernelKey,
					ResourceSpec:    bigResourceSpec,
				}

				candidateHosts, err := dockerScheduler.FindCandidateHosts(3, bigKernelSpec)
				Expect(err).To(BeNil())
				GinkgoWriter.Printf("Found candidate: host %s (ID=%s)\n",
					candidateHosts[0].GetNodeName(), candidateHosts[0].GetID())
				Expect(candidateHosts).ToNot(BeNil())
				Expect(len(candidateHosts)).To(Equal(1))
				Expect(candidateHosts[0]).To(Equal(bigHost1))

				Expect(bigHost1.NumReservations()).To(Equal(1))
				Expect(bigHost1.PendingResources().Equals(bigKernelSpec.DecimalSpecFromKernelSpec())).To(BeFalse())
				Expect(bigHost1.CommittedResources().Equals(bigKernelSpec.DecimalSpecFromKernelSpec())).To(BeTrue())

				i = len(hosts)
				bigHost2, _, _, err := addHost(i, largerHostSpec, false, dockerCluster, mockCtrl)
				Expect(err).To(BeNil())
				Expect(bigHost2).ToNot(BeNil())
				GinkgoWriter.Printf("Created bigHost2: name=%s & ID=%s\n", bigHost1.GetNodeName(), bigHost1.GetID())

				hosts[i] = bigHost2

				hostBatch, err := dockerScheduler.FindCandidateHosts(3-len(candidateHosts), bigKernelSpec)
				Expect(err).To(BeNil())
				Expect(hostBatch).ToNot(BeNil())
				Expect(len(hostBatch)).To(Equal(1))
				GinkgoWriter.Printf("Found candidate: host %s (ID=%s)\n",
					hostBatch[0].GetNodeName(), hostBatch[0].GetID())
				Expect(hostBatch[0]).To(Equal(bigHost2))
				candidateHosts = append(candidateHosts, hostBatch...)
				Expect(candidateHosts[0]).To(Equal(bigHost1))
				Expect(candidateHosts[1]).To(Equal(bigHost2))

				Expect(bigHost1.NumReservations()).To(Equal(1))
				reservation, loaded := bigHost1.GetReservation(kernelId)
				Expect(loaded).To(BeTrue())
				// Matches kernel.
				Expect(reservation.GetKernelId()).To(Equal(kernelId))
				// Matches host.
				Expect(reservation.GetHostId()).To(Equal(bigHost1.GetID()))
				// Not pending.
				Expect(reservation.IsPending()).To(BeFalse())
				// Created recently.
				Expect(time.Since(reservation.GetTimestamp()) < (time.Second * 5)).To(BeTrue())
				// Correct amount of resources.
				Expect(reservation.ToSpec().Equals(bigKernelSpec.DecimalSpecFromKernelSpec())).To(BeTrue())

				Expect(bigHost1.PendingResources().Equals(bigKernelSpec.DecimalSpecFromKernelSpec())).To(BeFalse())
				Expect(bigHost1.CommittedResources().Equals(bigKernelSpec.DecimalSpecFromKernelSpec())).To(BeTrue())

				smallKernelKey := uuid.NewString()
				smallKernelId := uuid.NewString()

				smallResourceSpec := proto.NewResourceSpec(1250, 2000, 2, 4)
				smallKernelSpec := &proto.KernelSpec{
					Id:              smallKernelId,
					Session:         smallKernelId,
					Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
					SignatureScheme: jupyter.JupyterSignatureScheme,
					Key:             smallKernelKey,
					ResourceSpec:    smallResourceSpec,
				}

				// Remove two of the smaller hosts so that the only hosts left are one small host and two big hosts.
				dockerCluster.RemoveHost(hosts[0].GetID())
				dockerCluster.RemoveHost(hosts[1].GetID())

				Expect(dockerCluster.Len()).To(Equal(3))

				candidateHosts, err = dockerScheduler.GetCandidateHosts(context.Background(), smallKernelSpec)
				Expect(err).To(BeNil())
				Expect(candidateHosts).ToNot(BeNil())
				// Expect(len(candidateHosts)).To(Equal(3))
				Expect(len(candidateHosts)).To(Equal(1))
			})

			It("Will fail to schedule replicas and not attempt to scale if resources are unavailable", func() {
				validateVariablesNonNil()

				kernel1Id := uuid.NewString()
				kernel1Key := uuid.NewString()
				kernel1ResourceSpec := proto.NewResourceSpec(1250, 2000, 8 /* too many */, 4)

				kernel1Spec := &proto.KernelSpec{
					Id:              kernel1Id,
					Session:         kernel1Id,
					Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
					SignatureScheme: jupyter.JupyterSignatureScheme,
					Key:             kernel1Key,
					ResourceSpec:    kernel1ResourceSpec,
				}

				candidateHosts, err := dockerScheduler.GetCandidateHosts(context.Background(), kernel1Spec)
				Expect(err).To(BeNil())
				Expect(candidateHosts).ToNot(BeNil())
				Expect(len(candidateHosts)).To(Equal(1))

				// Create a second reservation.
				candidateHosts, err = dockerScheduler.GetCandidateHosts(context.Background(), kernel1Spec)
				Expect(err).To(BeNil())
				Expect(candidateHosts).ToNot(BeNil())
				Expect(len(candidateHosts)).To(Equal(1))

				// Create a second reservation.
				candidateHosts, err = dockerScheduler.GetCandidateHosts(context.Background(), kernel1Spec)
				Expect(err).To(BeNil())
				Expect(candidateHosts).ToNot(BeNil())
				Expect(len(candidateHosts)).To(Equal(1))

				kernel2Id := uuid.NewString()
				kernel2Key := uuid.NewString()
				kernel2ResourceSpec := proto.NewResourceSpec(1250, 2000, 1 /* too many */, 4)

				kernel2Spec := &proto.KernelSpec{
					Id:              kernel2Id,
					Session:         kernel2Id,
					Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
					SignatureScheme: jupyter.JupyterSignatureScheme,
					Key:             kernel2Key,
					ResourceSpec:    kernel2ResourceSpec,
				}

				candidateHosts, err = dockerScheduler.GetCandidateHosts(context.Background(), kernel2Spec)
				Expect(err).ToNot(BeNil())
				Expect(candidateHosts).To(BeNil())
				Expect(len(candidateHosts)).To(Equal(0))
			})
		})
	})

	Context("FCFS Batch Scheduling", func() {
		BeforeEach(func() {
			mockCtrl = gomock.NewController(GinkgoT())

			kernelProvider = mock_scheduler.NewMockKernelProvider(mockCtrl)
			hostMapper = mock_scheduler.NewMockHostMapper(mockCtrl)

			opts.SchedulingPolicy = string(scheduling.FcfsBatch)
			schedulingPolicy, err := scheduler.GetSchedulingPolicy(&opts.SchedulerOptions)
			Expect(err).To(BeNil())
			Expect(schedulingPolicy).ToNot(BeNil())
			Expect(schedulingPolicy.NumReplicas()).To(Equal(1))
			Expect(schedulingPolicy.Name()).To(Equal("First-Come, First-Serve Batch Scheduling"))
			Expect(schedulingPolicy.ResourceBindingMode()).To(Equal(scheduling.BindResourcesWhenContainerScheduled))

			//clusterPlacer, err = placer.NewRandomPlacer(nil, schedulingPolicy.NumReplicas(), schedulingPolicy)
			clusterPlacer, err = schedulingPolicy.GetNewPlacer(nil)
			Expect(err).To(BeNil())
			Expect(clusterPlacer).ToNot(BeNil())
			_, ok := clusterPlacer.(*placer.BasicPlacer)
			Expect(ok).To(BeTrue())

			_, ok = clusterPlacer.GetIndex().(*index.RandomClusterIndex)
			Expect(ok).To(BeTrue())

			dockerCluster = cluster.NewDockerCluster(hostSpec, clusterPlacer, hostMapper, kernelProvider,
				nil, nil, schedulingPolicy, func(f func(stats *metrics.ClusterStatistics)) {},
				&opts.ClusterDaemonOptions.SchedulerOptions)

			Expect(dockerCluster).ToNot(BeNil())

			genericScheduler := dockerCluster.Scheduler()
			Expect(genericScheduler).ToNot(BeNil())

			dockerScheduler, ok = genericScheduler.(*scheduler.DockerScheduler)
			Expect(ok).To(BeTrue())
			Expect(dockerScheduler).ToNot(BeNil())
		})

		Context("Will handle basic scheduling operations correctly", func() {
			var numHosts int
			var hosts map[int]scheduling.Host
			var localGatewayClients map[int]*mock_proto.MockLocalGatewayClient
			var resourceSpoofers map[int]*distNbTesting.ResourceSpoofer

			Context("Start with 3 Hosts", func() {
				BeforeEach(func() {
					hosts = make(map[int]scheduling.Host)
					localGatewayClients = make(map[int]*mock_proto.MockLocalGatewayClient)
					resourceSpoofers = make(map[int]*distNbTesting.ResourceSpoofer)
					numHosts = 3

					for i := 0; i < numHosts; i++ {
						host, localGatewayClient, resourceSpoofer, err := addHost(i, hostSpec, false, dockerCluster, mockCtrl)
						Expect(err).To(BeNil())
						Expect(host).ToNot(BeNil())
						Expect(localGatewayClient).ToNot(BeNil())
						Expect(resourceSpoofer).ToNot(BeNil())

						hosts[i] = host
						localGatewayClients[i] = localGatewayClient
						resourceSpoofers[i] = resourceSpoofer
					}
				})

				validateVariablesNonNil := func() {
					Expect(dockerScheduler).ToNot(BeNil())
					Expect(clusterPlacer).ToNot(BeNil())
					Expect(dockerCluster).ToNot(BeNil())
					Expect(hostMapper).ToNot(BeNil())
					Expect(len(hosts)).To(Equal(3))
					Expect(len(localGatewayClients)).To(Equal(3))
					Expect(len(resourceSpoofers)).To(Equal(3))
				}

				It("Will correctly identify candidate hosts when candidate hosts are available", func() {
					validateVariablesNonNil()

					kernelId := uuid.NewString()
					kernelKey := uuid.NewString()
					resourceSpec := proto.NewResourceSpec(1250, 2000, 2, 4)

					kernelSpec := &proto.KernelSpec{
						Id:              kernelId,
						Session:         kernelId,
						Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
						SignatureScheme: jupyter.JupyterSignatureScheme,
						Key:             kernelKey,
						ResourceSpec:    resourceSpec,
					}

					candidateHosts, err := dockerScheduler.GetCandidateHosts(context.Background(), kernelSpec)
					Expect(err).To(BeNil())
					Expect(candidateHosts).ToNot(BeNil())
					Expect(len(candidateHosts)).To(Equal(1))

					for _, host := range candidateHosts {
						Expect(host.NumReservations()).To(Equal(1))
						Expect(host.PendingResources().Equals(kernelSpec.DecimalSpecFromKernelSpec())).To(BeFalse())
						Expect(host.CommittedResources().Equals(kernelSpec.DecimalSpecFromKernelSpec())).To(BeTrue())

						reservation, loaded := host.GetReservation(kernelId)
						Expect(loaded).To(BeTrue())
						// Matches kernel.
						Expect(reservation.GetKernelId()).To(Equal(kernelId))
						// Matches host.
						Expect(reservation.GetHostId()).To(Equal(host.GetID()))
						// Not pending.
						Expect(reservation.IsPending()).To(BeFalse())
						// Created recently.
						Expect(time.Since(reservation.GetTimestamp()) < (time.Second * 5)).To(BeTrue())
						// Correct amount of resources.
						Expect(reservation.ToSpec().Equals(kernelSpec.DecimalSpecFromKernelSpec())).To(BeTrue())
					}
				})

				It("Will correctly return an error when no candidate hosts are available", func() {
					validateVariablesNonNil()

					kernelId := uuid.NewString()
					kernelKey := uuid.NewString()
					resourceSpec := proto.NewResourceSpec(1250, 2000, 64 /* too many */, 4)

					kernelSpec := &proto.KernelSpec{
						Id:              kernelId,
						Session:         kernelId,
						Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
						SignatureScheme: jupyter.JupyterSignatureScheme,
						Key:             kernelKey,
						ResourceSpec:    resourceSpec,
					}

					candidateHosts, err := dockerScheduler.GetCandidateHosts(context.Background(), kernelSpec)
					Expect(err).ToNot(BeNil())
					Expect(candidateHosts).To(BeNil())
					Expect(len(candidateHosts)).To(Equal(0))
					fmt.Printf("Error: %v\n", err)
				})
			})

			It("Will correctly return whatever viable hosts it finds, even if it cannot find all of them, via the FindCandidateHosts method", func() {
				hosts = make(map[int]scheduling.Host)
				localGatewayClients = make(map[int]*mock_proto.MockLocalGatewayClient)
				resourceSpoofers = make(map[int]*distNbTesting.ResourceSpoofer)

				largerHostSpec := types.NewDecimalSpec(8000, 64000, 8, 32)

				// Create a new, larger host.
				i := len(hosts)
				bigHost1, _, _, err := addHost(i, largerHostSpec, false, dockerCluster, mockCtrl)
				Expect(err).To(BeNil())
				Expect(bigHost1).ToNot(BeNil())
				GinkgoWriter.Printf("Created bigHost1: name=%s & ID=%s\n", bigHost1.GetNodeName(), bigHost1.GetID())

				hosts[i] = bigHost1

				GinkgoWriter.Printf("dockerCluster.Len(): %d\n", dockerCluster.Len())
				Expect(dockerCluster.Len()).To(Equal(1))

				kernelId := uuid.NewString()
				kernelKey := uuid.NewString()
				bigResourceSpec := proto.NewResourceSpec(1250, 2000, 8, 4)

				bigKernelSpec := &proto.KernelSpec{
					Id:              kernelId,
					Session:         kernelId,
					Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
					SignatureScheme: jupyter.JupyterSignatureScheme,
					Key:             kernelKey,
					ResourceSpec:    bigResourceSpec,
				}

				candidateHosts, err := dockerScheduler.FindCandidateHosts(1, bigKernelSpec)
				Expect(err).To(BeNil())
				Expect(candidateHosts).ToNot(BeNil())
				Expect(len(candidateHosts)).To(Equal(1))
				Expect(candidateHosts[0]).To(Equal(bigHost1))

				Expect(bigHost1.NumReservations()).To(Equal(1))
				Expect(bigHost1.PendingResources().Equals(bigKernelSpec.DecimalSpecFromKernelSpec())).To(BeFalse())
				Expect(bigHost1.CommittedResources().Equals(bigKernelSpec.DecimalSpecFromKernelSpec())).To(BeTrue())

				i = len(hosts)
				bigHost2, _, _, err := addHost(i, largerHostSpec, false, dockerCluster, mockCtrl)
				Expect(err).To(BeNil())
				Expect(bigHost2).ToNot(BeNil())
				GinkgoWriter.Printf("Created bigHost2: name=%s & ID=%s\n", bigHost1.GetNodeName(), bigHost1.GetID())

				hosts[i] = bigHost2

				hostBatch, err := dockerScheduler.FindCandidateHosts(1-len(candidateHosts), bigKernelSpec)
				Expect(err).To(BeNil())
				Expect(hostBatch).ToNot(BeNil())

				candidateHosts = append(candidateHosts, hostBatch...)
				Expect(len(candidateHosts)).To(Equal(1))
				Expect(candidateHosts[0]).To(Equal(bigHost1))
				//Expect(candidateHosts[1]).To(Equal(bigHost2))

				Expect(bigHost1.NumReservations()).To(Equal(1))
				reservation, loaded := bigHost1.GetReservation(kernelId)
				Expect(loaded).To(BeTrue())
				// Matches kernel.
				Expect(reservation.GetKernelId()).To(Equal(kernelId))
				// Matches host.
				Expect(reservation.GetHostId()).To(Equal(bigHost1.GetID()))
				// Not pending.
				Expect(reservation.IsPending()).To(BeFalse())
				// Created recently.
				Expect(time.Since(reservation.GetTimestamp()) < (time.Second * 5)).To(BeTrue())
				// Correct amount of resources.
				Expect(reservation.ToSpec().Equals(bigKernelSpec.DecimalSpecFromKernelSpec())).To(BeTrue())

				Expect(bigHost1.PendingResources().Equals(bigKernelSpec.DecimalSpecFromKernelSpec())).To(BeFalse())
				Expect(bigHost1.CommittedResources().Equals(bigKernelSpec.DecimalSpecFromKernelSpec())).To(BeTrue())

				smallKernelKey := uuid.NewString()
				smallKernelId := uuid.NewString()

				smallResourceSpec := proto.NewResourceSpec(1250, 2000, 2, 4)
				smallKernelSpec := &proto.KernelSpec{
					Id:              smallKernelId,
					Session:         smallKernelId,
					Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
					SignatureScheme: jupyter.JupyterSignatureScheme,
					Key:             smallKernelKey,
					ResourceSpec:    smallResourceSpec,
				}

				// Remove two of the smaller hosts so that the only hosts left are one small host and two big hosts.
				dockerCluster.RemoveHost(hosts[0].GetID())
				dockerCluster.RemoveHost(hosts[1].GetID())

				Expect(dockerCluster.Len()).To(Equal(0))

				numHosts = 3

				for i := 0; i < numHosts; i++ {
					host, localGatewayClient, resourceSpoofer, err := addHost(i, hostSpec, false, dockerCluster, mockCtrl)
					Expect(err).To(BeNil())
					Expect(host).ToNot(BeNil())
					Expect(localGatewayClient).ToNot(BeNil())
					Expect(resourceSpoofer).ToNot(BeNil())

					hosts[i] = host
					localGatewayClients[i] = localGatewayClient
					resourceSpoofers[i] = resourceSpoofer
				}

				Expect(dockerCluster.Len()).To(Equal(3))

				candidateHosts, err = dockerScheduler.GetCandidateHosts(context.Background(), smallKernelSpec)
				Expect(err).To(BeNil())
				Expect(candidateHosts).ToNot(BeNil())
				// Expect(len(candidateHosts)).To(Equal(3))
				Expect(len(candidateHosts)).To(Equal(1))
			})

			It("Will correctly return an error when requested to scale up when using docker swarm cluster", func() {
				hosts = make(map[int]scheduling.Host)
				localGatewayClients = make(map[int]*mock_proto.MockLocalGatewayClient)
				resourceSpoofers = make(map[int]*distNbTesting.ResourceSpoofer)

				numHosts = 3

				for i := 0; i < numHosts; i++ {
					host, localGatewayClient, resourceSpoofer, err := addHost(i, hostSpec, false, dockerCluster, mockCtrl)
					Expect(err).To(BeNil())
					Expect(host).ToNot(BeNil())
					Expect(localGatewayClient).ToNot(BeNil())
					Expect(resourceSpoofer).ToNot(BeNil())

					hosts[i] = host
					localGatewayClients[i] = localGatewayClient
					resourceSpoofers[i] = resourceSpoofer
				}

				initialSize := len(hosts)
				Expect(initialSize).To(Equal(3))

				p := dockerCluster.ScaleToSize(context.Background(), int32(initialSize+1))
				Expect(p).ToNot(BeNil())

				err := p.Error()
				GinkgoWriter.Printf("dockerCluster.ScaleToSize(context.Background(), int32(initialSize+1)) --> Error: %v\n", err)
				Expect(err).ToNot(BeNil())
				Expect(errors.Is(err, scheduling.ErrUnsupportedOperation)).To(BeTrue())

				p = dockerCluster.ScaleToSize(context.Background(), int32(initialSize-1))
				Expect(p).ToNot(BeNil())

				err = p.Error()
				Expect(err).To(BeNil())
				GinkgoWriter.Printf("dockerCluster.ScaleToSize(context.Background(), int32(initialSize-1)) --> Error: %v\n", err)
				//Expect(errors.Is(err, scheduling.ErrUnsupportedOperation)).To(BeTrue())
			})
		})
	})
})
