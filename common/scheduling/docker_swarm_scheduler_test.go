package scheduling_test

import (
	"encoding/json"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/mock_proto"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	distNbTesting "github.com/zhangjyr/distributed-notebook/common/testing"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/gateway/domain"
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
	"remote-docker-event-aggregator-port": 5821
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
func (m *dockerSchedulerTestHostMapper) GetHostsOfKernel(kernelId string) ([]*scheduling.Host, error) {
	return nil, nil
}

func addHost(idx int, hostSpec types.Spec, cluster scheduling.Cluster, mockCtrl *gomock.Controller) (*scheduling.Host, *mock_proto.MockLocalGatewayClient, *distNbTesting.ResourceSpoofer, error) {
	hostId := uuid.NewString()
	nodeName := fmt.Sprintf("TestNode%d", idx)
	resourceSpoofer := distNbTesting.NewResourceSpoofer(nodeName, hostId, hostSpec)
	host, localGatewayClient, err := distNbTesting.NewHostWithSpoofedGRPC(mockCtrl, cluster, hostId, nodeName, resourceSpoofer)

	cluster.NewHostAddedOrConnected(host)

	return host, localGatewayClient, resourceSpoofer, err
}

var _ = Describe("Docker Swarm Scheduler Tests", func() {
	var (
		mockCtrl   *gomock.Controller
		scheduler  *scheduling.DockerScheduler
		cluster    scheduling.ClusterInternal
		placer     scheduling.Placer
		hostMapper scheduling.HostMapper
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

		cluster = scheduling.NewDockerSwarmCluster(hostSpec, hostMapper, nil, &opts.ClusterSchedulerOptions)
		Expect(cluster).ToNot(BeNil())

		placer = cluster.Placer()
		Expect(placer).ToNot(BeNil())

		genericScheduler := cluster.ClusterScheduler()
		Expect(genericScheduler).ToNot(BeNil())

		var ok bool
		scheduler, ok = genericScheduler.(*scheduling.DockerScheduler)
		Expect(ok).To(BeTrue())
		Expect(scheduler).ToNot(BeNil())

		hostMapper = &dockerSchedulerTestHostMapper{}
	})

	AfterEach(func() {
		mockCtrl.Finish()
	})

	Context("Will handle basic scheduling operations correctly", func() {
		var numHosts int
		var hosts map[int]*scheduling.Host
		var localGatewayClients map[int]*mock_proto.MockLocalGatewayClient
		var resourceSpoofers map[int]*distNbTesting.ResourceSpoofer

		BeforeEach(func() {
			hosts = make(map[int]*scheduling.Host)
			localGatewayClients = make(map[int]*mock_proto.MockLocalGatewayClient)
			resourceSpoofers = make(map[int]*distNbTesting.ResourceSpoofer)
			numHosts = 3

			for i := 0; i < numHosts; i++ {
				host, localGatewayClient, resourceSpoofer, err := addHost(i, hostSpec, cluster, mockCtrl)
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
			Expect(scheduler).ToNot(BeNil())
			Expect(placer).ToNot(BeNil())
			Expect(cluster).ToNot(BeNil())
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

			candidateHosts, err := scheduler.GetCandidateHosts(context.Background(), kernelSpec)
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

			candidateHosts, err := scheduler.GetCandidateHosts(context.Background(), kernelSpec)
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
			bigHost1, _, _, err := addHost(i, largerHostSpec, cluster, mockCtrl)
			Expect(err).To(BeNil())
			Expect(bigHost1).ToNot(BeNil())

			hosts[i] = bigHost1

			Expect(cluster.Len()).To(Equal(4))

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

			candidateHosts := make([]*scheduling.Host, 0)
			candidateHosts = scheduler.TryGetCandidateHosts(candidateHosts, bigKernelSpec)
			Expect(candidateHosts).ToNot(BeNil())
			Expect(len(candidateHosts)).To(Equal(1))
			Expect(candidateHosts[0]).To(Equal(bigHost1))

			Expect(bigHost1.NumReservations()).To(Equal(1))
			Expect(bigHost1.PendingResources().Equals(bigKernelSpec.DecimalSpecFromKernelSpec())).To(BeTrue())

			i = len(hosts)
			bigHost2, _, _, err := addHost(i, largerHostSpec, cluster, mockCtrl)
			Expect(err).To(BeNil())
			Expect(bigHost2).ToNot(BeNil())

			hosts[i] = bigHost2

			candidateHosts = scheduler.TryGetCandidateHosts(candidateHosts, bigKernelSpec)
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
			cluster.RemoveHost(hosts[0].ID)
			cluster.RemoveHost(hosts[1].ID)

			Expect(cluster.Len()).To(Equal(3))

			candidateHosts, err = scheduler.GetCandidateHosts(context.Background(), smallKernelSpec)
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
	})
})
