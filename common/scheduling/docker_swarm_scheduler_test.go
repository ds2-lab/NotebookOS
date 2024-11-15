package scheduling_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	jupyter "github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/mock_proto"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/cluster"
	"github.com/scusemua/distributed-notebook/common/scheduling/entity"
	"github.com/scusemua/distributed-notebook/common/scheduling/resource"
	"github.com/scusemua/distributed-notebook/common/scheduling/scheduler"
	distNbTesting "github.com/scusemua/distributed-notebook/common/testing"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/gateway/domain"
	"go.uber.org/mock/gomock"
	"golang.org/x/net/context"
)

var (
	optsAsJson = `{
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
			"scaling-factor": 1.05,
			"scaling-interval": 30,
			"scaling-limit": 1.1,
			"scaling-in-limit": 2,
			"predictive_autoscaling": false,
			"scaling-buffer-size": 3,
			"min_cluster_nodes": 4,
			"max_cluster_nodes": 32,
			"gpu_poll_interval": 5,
			"num-replicas": 3,
			"max-subscribed-ratio": 7,
			"execution-time-sampling-window": 10,
			"migration-time-sampling-window": 10,
			"scheduler-http-port": 8078,
			"common_options": {
			"gpus-per-host": 8,
			"deployment_mode": "docker-compose",
			"using-wsl": true,
			"docker_network_name": "distributed_cluster_default",
			"prometheus_interval": 15,
			"prometheus_port": -1,
			"num_resend_attempts": 1,
			"acks_enabled": false,
			"scheduling-policy": "static",
			"hdfs-namenode-endpoint": "host.docker.internal:10000",
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
	"notebook-image-name": "scusemua/jupyter",
	"notebook-image-tag": "latest",
	"distributed-cluster-service-port": 8079,
	"remote-docker-event-aggregator-port": 5821,
	"initial-cluster-size": -1,
	"initial-connection-period": 0
},
	"port": 8080,
	"provisioner_port": 8081,
	"jaeger_addr": "",
	"consul_addr": ""
}`
)

// dockerSchedulerTestHostMapper implements the scheduling.HostMapper interface and exists for use in the
// Docker Swarm Scheduler unit tests.
type dockerSchedulerTestHostMapper struct{}

// GetHostsOfKernel returns the Host instances on which the replicas of the specified kernel are scheduled.
func (m *dockerSchedulerTestHostMapper) GetHostsOfKernel(kernelId string) ([]scheduling.Host, error) {
	return nil, nil
}

func addHost(idx int, hostSpec types.Spec, disableHost bool, cluster scheduling.Cluster, mockCtrl *gomock.Controller) (scheduling.Host, *mock_proto.MockLocalGatewayClient, *distNbTesting.ResourceSpoofer, error) {
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

var _ = Describe("Docker Swarm Scheduler Tests", func() {
	var (
		mockCtrl        *gomock.Controller
		dockerScheduler *scheduler.DockerScheduler
		dockerCluster   scheduling.Cluster
		placer          scheduling.Placer
		hostMapper      scheduler.HostMapper
	)

	config.LogLevel = logger.LOG_LEVEL_ALL

	hostSpec := types.NewDecimalSpec(8000, 64000, 8, 32)

	var opts *domain.ClusterGatewayOptions
	err := json.Unmarshal([]byte(optsAsJson), &opts)
	if err != nil {
		panic(err)
	}

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())

		dockerCluster = cluster.NewDockerSwarmCluster(hostSpec, hostMapper, nil, nil, &opts.ClusterDaemonOptions.SchedulerOptions)
		Expect(dockerCluster).ToNot(BeNil())

		placer = dockerCluster.Placer()
		Expect(placer).ToNot(BeNil())

		genericScheduler := dockerCluster.Scheduler()
		Expect(genericScheduler).ToNot(BeNil())

		var ok bool
		dockerScheduler, ok = genericScheduler.(*scheduler.DockerScheduler)
		Expect(ok).To(BeTrue())
		Expect(dockerScheduler).ToNot(BeNil())

		hostMapper = &dockerSchedulerTestHostMapper{}
	})

	AfterEach(func() {
		mockCtrl.Finish()
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
			Expect(placer).ToNot(BeNil())
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

		It("Will correctly return whatever viable hosts it finds, even if it cannot find all of them, via the TryGetCandidateHosts method", func() {
			validateVariablesNonNil()

			largerHostSpec := types.NewDecimalSpec(8000, 64000, 64, 32)

			// Create a new, larger host.
			i := len(hosts)
			bigHost1, _, _, err := addHost(i, largerHostSpec, false, dockerCluster, mockCtrl)
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

			candidateHosts := make([]scheduling.Host, 0)
			candidateHosts = dockerScheduler.TryGetCandidateHosts(candidateHosts, bigKernelSpec)
			Expect(candidateHosts).ToNot(BeNil())
			Expect(len(candidateHosts)).To(Equal(1))
			Expect(candidateHosts[0]).To(Equal(bigHost1))

			Expect(bigHost1.NumReservations()).To(Equal(1))
			Expect(bigHost1.PendingResources().Equals(bigKernelSpec.DecimalSpecFromKernelSpec())).To(BeTrue())

			i = len(hosts)
			bigHost2, _, _, err := addHost(i, largerHostSpec, false, dockerCluster, mockCtrl)
			Expect(err).To(BeNil())
			Expect(bigHost2).ToNot(BeNil())

			hosts[i] = bigHost2

			candidateHosts = dockerScheduler.TryGetCandidateHosts(candidateHosts, bigKernelSpec)
			Expect(candidateHosts).ToNot(BeNil())
			Expect(len(candidateHosts)).To(Equal(2))
			Expect(candidateHosts[0]).To(Equal(bigHost1))
			Expect(candidateHosts[1]).To(Equal(bigHost2))

			Expect(bigHost1.NumReservations()).To(Equal(1))
			Expect(bigHost1.PendingResources().Equals(bigKernelSpec.DecimalSpecFromKernelSpec())).To(BeTrue())

			Expect(bigHost2.NumReservations()).To(Equal(1))
			Expect(bigHost2.PendingResources().Equals(bigKernelSpec.DecimalSpecFromKernelSpec())).To(BeTrue())

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
			Expect(len(candidateHosts)).To(Equal(3))

			combinedSpec := smallKernelSpec.DecimalSpecFromKernelSpec().Add(bigKernelSpec.DecimalSpecFromKernelSpec())

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

		It("Will try to scale out if necessary", func() {
			initialSize := len(hosts)

			// First, add two new hosts, so that there are 5 hosts.
			for i := initialSize; i < initialSize+2; i++ {
				host, localGatewayClient, resourceSpoofer, err := addHost(i, hostSpec, false, dockerCluster, mockCtrl)
				Expect(err).To(BeNil())
				Expect(host).ToNot(BeNil())
				Expect(localGatewayClient).ToNot(BeNil())
				Expect(resourceSpoofer).ToNot(BeNil())

				hosts[i] = host
				localGatewayClients[i] = localGatewayClient
				resourceSpoofers[i] = resourceSpoofer
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

			hosts[5] = host
			localGatewayClients[5] = localGatewayClient
			resourceSpoofers[5] = resourceSpoofer

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
					WithResourceUtilization(resource.NewUtilization(1.0, 2000, []float64{1.0, 1.0}, 4)).
					Build()

				dockerCluster.AddSession(kernelId, session)
				dockerCluster.Scheduler().UpdateRatio(true)

				Expect(dockerCluster.Sessions().Len()).To(Equal(i + 1))

				for j := 0; j < 3; j++ {
					host := hosts[j]

					fmt.Printf("Cluster subscribed ratio: %f, Host %s (ID=%s) subscription ratio: %f, oversubscription factor: %s\n",
						dockerCluster.SubscriptionRatio(), host.GetNodeName(), host.GetID(), host.SubscribedRatio(), host.OversubscriptionFactor().StringFixed(4))

					err := host.AddToPendingResources(decimalSpec)
					Expect(err).To(BeNil())

					dockerCluster.Scheduler().UpdateRatio(true)

					fmt.Printf("Cluster subscribed ratio: %f, Host %s (ID=%s) subscription ratio: %f, oversubscription factor: %s\n\n",
						dockerCluster.SubscriptionRatio(), host.GetNodeName(), host.GetID(), host.SubscribedRatio(), host.OversubscriptionFactor().StringFixed(4))
				}
			}

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

			By("Correctly returning only 2 candidate hosts due to the other hosts becoming too oversubscribed")

			candidateHosts := make([]scheduling.Host, 0)
			candidateHosts = dockerScheduler.TryGetCandidateHosts(candidateHosts, kernelSpec)
			Expect(candidateHosts).ToNot(BeNil())
			Expect(len(candidateHosts)).To(Equal(2))

			By("Releasing the reservations on the 2 candidate hosts without any errors")

			for _, host := range candidateHosts {
				Expect(host.NumReservations()).To(Equal(1))
				err = host.ReleaseReservation(kernelSpec)
				Expect(err).To(BeNil())
			}

			Expect(dockerCluster.Len()).To(Equal(5))
			Expect(dockerCluster.NumScalingOperationsAttempted()).To(Equal(0))
			Expect(dockerCluster.NumScaleOutOperationsAttempted()).To(Equal(0))
			Expect(dockerCluster.NumScaleOutOperationsSucceeded()).To(Equal(0))

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
			Expect(placer).ToNot(BeNil())
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
