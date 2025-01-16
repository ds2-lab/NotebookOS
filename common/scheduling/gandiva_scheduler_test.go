package scheduling_test

import (
	"encoding/json"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/scheduling/cluster"
	"github.com/scusemua/distributed-notebook/common/scheduling/mock_scheduler"
	"github.com/scusemua/distributed-notebook/common/scheduling/placer"
	"github.com/scusemua/distributed-notebook/common/scheduling/policy"
	"github.com/scusemua/distributed-notebook/common/scheduling/scheduler"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/gateway/domain"
	"go.uber.org/mock/gomock"
)

var (
	gandivaSchedulerTestOpts = `{
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
			"deployment_mode": "docker-swarm",
			"using-wsl": true,
			"docker_network_name": "distributed_cluster_default",
			"prometheus_interval": 15,
			"prometheus_port": -1,
			"num_resend_attempts": 1,
			"acks_enabled": false,
			"scheduling-policy": "gandiva",
			"idle-session-reclamation-policy": "none",
			"remote-storage-endpoint": "host.docker.internal:10000",
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

var _ = Describe("Gandiva Scheduler Tests", func() {
	var (
		mockCtrl *gomock.Controller

		mockedHostMapper         *mock_scheduler.MockHostMapper
		mockedKernelProvider     *mock_scheduler.MockKernelProvider
		mockedNotificationBroker *mock_scheduler.MockNotificationBroker

		dockerSwarmCluster *cluster.DockerSwarmCluster
		randomPlacer       *placer.RandomPlacer
		gandivaScheduler   *scheduler.GandivaScheduler

		opts *domain.ClusterGatewayOptions

		hostSpec *types.DecimalSpec
	)

	BeforeEach(func() {
		err := json.Unmarshal([]byte(optsAsJson), &opts)
		if err != nil {
			panic(err)
		}

		mockCtrl = gomock.NewController(GinkgoT())
		mockedHostMapper = mock_scheduler.NewMockHostMapper(mockCtrl)
		mockedKernelProvider = mock_scheduler.NewMockKernelProvider(mockCtrl)
		mockedNotificationBroker = mock_scheduler.NewMockNotificationBroker(mockCtrl)

		hostSpec = types.NewDecimalSpec(64000, 128000, 8, 40)

		schedulingPolicy, err := policy.GetSchedulingPolicy(&opts.SchedulerOptions)
		Expect(err).To(BeNil())
		Expect(schedulingPolicy).ToNot(BeNil())
		Expect(schedulingPolicy.NumReplicas()).To(Equal(1))
		Expect(schedulingPolicy.Name()).To(Equal("Gandiva"))

		gandivaScheduler, err = scheduler.NewGandivaScheduler(dockerSwarmCluster, randomPlacer, mockedHostMapper,
			hostSpec, mockedKernelProvider, mockedNotificationBroker, schedulingPolicy, &opts.SchedulerOptions)
		Expect(err).To(BeNil())
	})

	It("Will work", func() {
		Expect(gandivaScheduler).ToNot(BeNil())
	})
})
