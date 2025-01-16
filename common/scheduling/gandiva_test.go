package scheduling_test

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	jupyter "github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/cluster"
	"github.com/scusemua/distributed-notebook/common/scheduling/mock_scheduler"
	"github.com/scusemua/distributed-notebook/common/scheduling/placer"
	"github.com/scusemua/distributed-notebook/common/scheduling/policy"
	"github.com/scusemua/distributed-notebook/common/scheduling/scheduler"
	"github.com/scusemua/distributed-notebook/common/statistics"
	"github.com/scusemua/distributed-notebook/common/testing"
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

var _ = Describe("Gandiva Cluster/Scheduler Tests", func() {
	var (
		mockCtrl *gomock.Controller

		mockedHostMapper     *mock_scheduler.MockHostMapper
		mockedKernelProvider *mock_scheduler.MockKernelProvider

		dockerSwarmCluster *cluster.DockerSwarmCluster
		randomPlacer       *placer.RandomPlacer
		gandivaScheduler   *scheduler.GandivaScheduler
		schedulingPolicy   scheduling.Policy

		opts *domain.ClusterGatewayOptions

		hostSpec *types.DecimalSpec
	)

	//commitResources := func(host scheduling.Host, resources *types.DecimalSpec) {
	//	err := host.AddToCommittedResources(resources)
	//	Expect(err).To(BeNil())
	//
	//	err = host.SubtractFromIdleResources(resources)
	//	Expect(err).To(BeNil())
	//
	//	fmt.Printf("Committed the following resources to Host %s (ID=%s): %v\n\n",
	//		host.GetNodeName(), host.GetID(), resources.String())
	//
	//	err = dockerSwarmCluster.UpdateIndex(host)
	//	Expect(err).To(BeNil())
	//}

	releaseResources := func(host scheduling.Host, resources *types.DecimalSpec) {
		err := host.SubtractFromCommittedResources(resources)
		Expect(err).To(BeNil())

		err = host.AddToIdleResources(resources)
		Expect(err).To(BeNil())

		fmt.Printf("\nReleased the following resources from Host %s (ID=%s): %v\n",
			host.GetNodeName(), host.GetID(), resources.String())
		fmt.Printf("Host %s now has the following idle resources: %v\n",
			host.GetNodeName(), host.IdleResources().String())
		fmt.Printf("Host %s now has the following committed resources: %v\n\n",
			host.GetNodeName(), host.CommittedResources().String())

		err = dockerSwarmCluster.UpdateIndex(host)
		Expect(err).To(BeNil())
	}

	createHost := func(idx int) (scheduling.Host, *testing.ResourceSpoofer) {
		hostId := uuid.NewString()
		hostName := fmt.Sprintf("TestHost-%d", idx)
		resourceSpoofer := testing.NewResourceSpoofer(hostName, hostId, hostSpec)
		host, _, err := testing.NewHostWithSpoofedGRPC(mockCtrl, dockerSwarmCluster, hostId, hostName, resourceSpoofer)
		Expect(err).To(BeNil())
		Expect(host).ToNot(BeNil())

		err = dockerSwarmCluster.NewHostAddedOrConnected(host)
		Expect(err).To(BeNil())

		return host, resourceSpoofer
	}

	createKernelSpec := func(spec types.Spec) *proto.KernelSpec {
		kernelId := uuid.NewString()
		kernelKey := uuid.NewString()
		resourceSpec := proto.NewResourceSpec(int32(spec.CPU()), float32(spec.MemoryMB()),
			int32(spec.GPU()), float32(spec.VRAM()))
		return &proto.KernelSpec{
			Id:              kernelId,
			Session:         kernelId,
			Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
			SignatureScheme: jupyter.JupyterSignatureScheme,
			Key:             kernelKey,
			ResourceSpec:    resourceSpec,
		}
	}

	BeforeEach(func() {
		err := json.Unmarshal([]byte(gandivaSchedulerTestOpts), &opts)
		if err != nil {
			panic(err)
		}

		mockCtrl = gomock.NewController(GinkgoT())
		mockedHostMapper = mock_scheduler.NewMockHostMapper(mockCtrl)
		mockedKernelProvider = mock_scheduler.NewMockKernelProvider(mockCtrl)

		hostSpec = types.NewDecimalSpec(64000, 128000, float64(opts.SchedulerOptions.GpusPerHost), 40)

		schedulingPolicy, err = policy.GetSchedulingPolicy(&opts.SchedulerOptions)
		Expect(err).To(BeNil())
		Expect(schedulingPolicy).ToNot(BeNil())
		Expect(schedulingPolicy.NumReplicas()).To(Equal(1))
		Expect(schedulingPolicy.Name()).To(Equal("Gandiva"))

		randomPlacer, err = placer.NewRandomPlacer(nil, 1, schedulingPolicy)
		Expect(err).To(BeNil())

		dockerSwarmCluster = cluster.NewDockerSwarmCluster(hostSpec, randomPlacer, mockedHostMapper,
			mockedKernelProvider, nil, nil, schedulingPolicy,
			func(f func(stats *statistics.ClusterStatistics)) {}, &opts.SchedulerOptions)

		var ok bool
		gandivaScheduler, ok = dockerSwarmCluster.Scheduler().(*scheduler.GandivaScheduler)
		Expect(ok).To(BeTrue())
		Expect(gandivaScheduler).ToNot(BeNil())
	})

	It("Will be instantiated correctly", func() {
		Expect(gandivaScheduler).ToNot(BeNil())

		Expect(gandivaScheduler.Instance()).To(Equal(gandivaScheduler))

		var i int32
		for i = 0; i < 9; i++ {
			Expect(gandivaScheduler.NumHostsInPool(i)).To(Equal(0))

			hostPool := gandivaScheduler.GetHostPool(i)
			Expect(hostPool).ToNot(BeNil())
			Expect(hostPool.NumGpus).To(Equal(i))

			placer := hostPool.Placer
			Expect(placer).ToNot(BeNil())
			Expect(placer.Size()).To(Equal(0))
			Expect(placer.Len()).To(Equal(0))
		}
	})

	It("Will correctly update the positions of host in the index when their resources change", func() {
		_, _ = createHost(1)
		Expect(gandivaScheduler.NumUnpooledHosts()).To(Equal(1))
		Expect(dockerSwarmCluster.Len()).To(Equal(1))

		_, _ = createHost(2)
		Expect(gandivaScheduler.NumUnpooledHosts()).To(Equal(2))
		Expect(dockerSwarmCluster.Len()).To(Equal(2))

		kernelResourceSpec := types.NewDecimalSpec(128, 128, 5, 2)
		kernel1Spec := createKernelSpec(kernelResourceSpec)
		kernel2Spec := createKernelSpec(kernelResourceSpec)

		candidateHosts := gandivaScheduler.FindCandidateHosts(1, kernel1Spec)
		Expect(len(candidateHosts)).To(Equal(1))
		GinkgoWriter.Printf("Candidate host name: \"%s\"\n", candidateHosts[0].GetNodeName())
		//Expect(candidateHosts[0]).To(Equal(host1))

		candidateHosts = gandivaScheduler.FindCandidateHosts(1, kernel2Spec)
		Expect(len(candidateHosts)).To(Equal(1))
		GinkgoWriter.Printf("Candidate host name: \"%s\"\n", candidateHosts[0].GetNodeName())
		//Expect(candidateHosts[0]).To(Equal(host2))
	})

	It("Will not return any candidate hosts when there are no hosts in the cluster", func() {
		kernelId := uuid.NewString()
		kernelKey := uuid.NewString()
		resourceSpec := proto.NewResourceSpec(1250, 2000, 4, 4)

		kernelSpec := &proto.KernelSpec{
			Id:              kernelId,
			Session:         kernelId,
			Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
			SignatureScheme: jupyter.JupyterSignatureScheme,
			Key:             kernelKey,
			ResourceSpec:    resourceSpec,
		}

		candidateHosts := gandivaScheduler.FindCandidateHosts(1, kernelSpec)
		Expect(len(candidateHosts)).To(Equal(0))
	})

	It("Will return a candidate host in response to a request", func() {
		host1, _ := createHost(1)

		Expect(gandivaScheduler.NumUnpooledHosts()).To(Equal(1))

		kernel1ResourceSpec := types.NewDecimalSpec(128, 128, 2, 2)
		kernel1Spec := createKernelSpec(kernel1ResourceSpec)

		By("Returning the only available host when finding a candidate")

		candidateHosts := gandivaScheduler.FindCandidateHosts(1, kernel1Spec)
		Expect(len(candidateHosts)).To(Equal(1))
		Expect(candidateHosts[0]).To(Equal(host1))

		Expect(gandivaScheduler.NumUnpooledHosts()).To(Equal(0))

		Expect(gandivaScheduler.NumHostsInPool(0)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(1)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(2)).To(Equal(1))
		Expect(gandivaScheduler.NumHostsInPool(3)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(4)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(5)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(6)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(7)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(8)).To(Equal(0))

		hostPool := gandivaScheduler.GetHostPool(int32(2))
		Expect(hostPool).ToNot(BeNil())
		Expect(hostPool.NumGpus).To(Equal(int32(2)))
		Expect(hostPool.Placer.Len()).To(Equal(1))
		Expect(hostPool.Placer.Size()).To(Equal(1))

		By("Returning the only available host again when finding a candidate a second time")

		kernel2ResourceSpec := types.NewDecimalSpec(128, 128, 2, 2)
		kernel2Spec := createKernelSpec(kernel2ResourceSpec)

		candidateHosts = gandivaScheduler.FindCandidateHosts(1, kernel2Spec)
		Expect(len(candidateHosts)).To(Equal(1))
		Expect(candidateHosts[0]).To(Equal(host1))

		Expect(gandivaScheduler.NumUnpooledHosts()).To(Equal(0))

		Expect(gandivaScheduler.NumHostsInPool(0)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(1)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(2)).To(Equal(1))
		Expect(gandivaScheduler.NumHostsInPool(3)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(4)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(5)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(6)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(7)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(8)).To(Equal(0))

		hostPool = gandivaScheduler.GetHostPool(int32(2))
		Expect(hostPool).ToNot(BeNil())
		Expect(hostPool.NumGpus).To(Equal(int32(2)))
		Expect(hostPool.Placer.Len()).To(Equal(1))
		Expect(hostPool.Placer.Size()).To(Equal(1))
	})

	It("Will return the least-loaded candidate host in response to a request", func() {
		host1, _ := createHost(1)
		Expect(gandivaScheduler.NumUnpooledHosts()).To(Equal(1))
		Expect(dockerSwarmCluster.Len()).To(Equal(1))

		host2, _ := createHost(2)
		Expect(gandivaScheduler.NumUnpooledHosts()).To(Equal(2))
		Expect(dockerSwarmCluster.Len()).To(Equal(2))

		resourceSpec := proto.NewResourceSpec(1250, 2000, 5, 4)
		kernel1Id := uuid.NewString()
		kernel1Key := uuid.NewString()
		kernel1Spec := &proto.KernelSpec{
			Id:              kernel1Id,
			Session:         kernel1Id,
			Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
			SignatureScheme: jupyter.JupyterSignatureScheme,
			Key:             kernel1Key,
			ResourceSpec:    resourceSpec,
		}

		By("Returning an available host when finding a candidate")

		candidateHosts := gandivaScheduler.FindCandidateHosts(1, kernel1Spec)
		Expect(len(candidateHosts)).To(Equal(1))
		GinkgoWriter.Printf("Candidate host name: \"%s\"\n", candidateHosts[0].GetNodeName())
		Expect(candidateHosts[0]).To(Equal(host1))

		Expect(gandivaScheduler.NumUnpooledHosts()).To(Equal(1))

		Expect(gandivaScheduler.NumHostsInPool(0)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(1)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(2)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(3)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(4)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(5)).To(Equal(1))
		Expect(gandivaScheduler.NumHostsInPool(6)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(7)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(8)).To(Equal(0))

		hostPool := gandivaScheduler.GetHostPool(int32(5))
		Expect(hostPool).ToNot(BeNil())
		Expect(hostPool.NumGpus).To(Equal(int32(5)))
		Expect(hostPool.Placer.Len()).To(Equal(1))
		Expect(hostPool.Placer.Size()).To(Equal(1))

		By("Returning the other available host again when finding a candidate a second time")

		kernel2Id := uuid.NewString()
		kernel2Key := uuid.NewString()
		kernel2Spec := &proto.KernelSpec{
			Id:              kernel2Id,
			Session:         kernel2Id,
			Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
			SignatureScheme: jupyter.JupyterSignatureScheme,
			Key:             kernel2Key,
			ResourceSpec:    resourceSpec,
		}

		candidateHosts = gandivaScheduler.FindCandidateHosts(1, kernel2Spec)
		Expect(len(candidateHosts)).To(Equal(1))
		GinkgoWriter.Printf("Candidate host name: \"%s\"\n", candidateHosts[0].GetNodeName())
		Expect(candidateHosts[0]).To(Equal(host2))

		Expect(gandivaScheduler.NumUnpooledHosts()).To(Equal(0))

		Expect(gandivaScheduler.NumHostsInPool(0)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(1)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(2)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(3)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(4)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(5)).To(Equal(2))
		Expect(gandivaScheduler.NumHostsInPool(6)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(7)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(8)).To(Equal(0))

		hostPool = gandivaScheduler.GetHostPool(int32(5))
		Expect(hostPool).ToNot(BeNil())
		Expect(hostPool.NumGpus).To(Equal(int32(5)))
		Expect(hostPool.Placer.Len()).To(Equal(2))
		Expect(hostPool.Placer.Size()).To(Equal(2))

		By("Returning the correct host after further resource adjustments have occurred")

		// Artificially increase the resources available on Host #1.
		releaseResources(host1, types.NewDecimalSpec(128, 128, 2, 2))

		kernel3Id := uuid.NewString()
		kernel3Key := uuid.NewString()
		kernel3Spec := &proto.KernelSpec{
			Id:              kernel3Id,
			Session:         kernel3Id,
			Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
			SignatureScheme: jupyter.JupyterSignatureScheme,
			Key:             kernel3Key,
			ResourceSpec:    resourceSpec,
		}

		candidateHosts = gandivaScheduler.FindCandidateHosts(1, kernel3Spec)
		Expect(len(candidateHosts)).To(Equal(1))
		GinkgoWriter.Printf("Candidate host name: \"%s\"\n", candidateHosts[0].GetNodeName())
		Expect(candidateHosts[0]).To(Equal(host1))

		Expect(gandivaScheduler.NumUnpooledHosts()).To(Equal(0))

		Expect(gandivaScheduler.NumHostsInPool(0)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(1)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(2)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(3)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(4)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(5)).To(Equal(2))
		Expect(gandivaScheduler.NumHostsInPool(6)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(7)).To(Equal(0))
		Expect(gandivaScheduler.NumHostsInPool(8)).To(Equal(0))

		hostPool = gandivaScheduler.GetHostPool(int32(5))
		Expect(hostPool).ToNot(BeNil())
		Expect(hostPool.NumGpus).To(Equal(int32(5)))
		Expect(hostPool.Placer.Len()).To(Equal(2))
		Expect(hostPool.Placer.Size()).To(Equal(2))
	})
})
