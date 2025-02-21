package placer_test

import (
	"encoding/json"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	jupyter "github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/cluster"
	"github.com/scusemua/distributed-notebook/common/scheduling/mock_scheduler"
	"github.com/scusemua/distributed-notebook/common/scheduling/placer"
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
			"scaling-interval": 15,
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
		"notebook-image-name": "scusemua/jupyter-gpu",
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

var _ = Describe("Gandiva Placer Tests", func() {
	var (
		mockCtrl *gomock.Controller

		mockedHostMapper     *mock_scheduler.MockHostMapper
		mockedKernelProvider *mock_scheduler.MockKernelProvider

		dockerCluster    *cluster.DockerCluster
		dockerScheduler  *scheduler.DockerScheduler
		schedulingPolicy scheduling.Policy

		opts *domain.ClusterGatewayOptions

		hostSpec *types.DecimalSpec
	)

	clusterProvider := func() scheduling.Cluster {
		return dockerCluster
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

		schedulingPolicy, err = scheduler.GetSchedulingPolicy(&opts.SchedulerOptions, clusterProvider)
		Expect(err).To(BeNil())
		Expect(schedulingPolicy).ToNot(BeNil())
		Expect(schedulingPolicy.NumReplicas()).To(Equal(1))
		Expect(schedulingPolicy.Name()).To(Equal("Gandiva"))

		clusterPlacer, err := schedulingPolicy.GetNewPlacer(nil)
		Expect(err).To(BeNil())
		Expect(clusterPlacer).ToNot(BeNil())

		gandivaPlacer, ok := clusterPlacer.(*placer.GandivaPlacer)
		Expect(ok).To(BeTrue())
		Expect(gandivaPlacer).ToNot(BeNil())

		dockerCluster = cluster.NewDockerCluster(hostSpec, gandivaPlacer, mockedHostMapper,
			mockedKernelProvider, nil, nil, schedulingPolicy.(scheduler.SchedulingPolicy), // TODO: Fix these messy types
			func(f func(stats *metrics.ClusterStatistics)) {}, &opts.SchedulerOptions)

		dockerScheduler, ok = dockerCluster.Scheduler().(*scheduler.DockerScheduler)
		Expect(ok).To(BeTrue())
		Expect(dockerScheduler).ToNot(BeNil())
	})

	It("Will be instantiated correctly", func() {
		Expect(dockerScheduler).ToNot(BeNil())
		Expect(dockerScheduler.Instance()).To(Equal(dockerScheduler))

		clusterPlacer := dockerScheduler.Placer()
		Expect(clusterPlacer).ToNot(BeNil())

		gandivaPlacer, ok := clusterPlacer.(*placer.GandivaPlacer)
		Expect(ok).To(BeTrue())
		Expect(gandivaPlacer).ToNot(BeNil())

		var i int32
		for i = 0; i < 9; i++ {
			Expect(gandivaPlacer.NumHostsInPool(i)).To(Equal(0))

			hostPool, loaded := gandivaPlacer.GetHostPool(i)
			Expect(loaded).To(BeTrue())
			Expect(hostPool).ToNot(BeNil())
			Expect(hostPool.PoolNumber).To(Equal(i))
			Expect(hostPool.Len()).To(Equal(0))
			Expect(hostPool.Size()).To(Equal(0))

			placer := hostPool.Pool
			Expect(placer).ToNot(BeNil())
			Expect(placer.Len()).To(Equal(0))
		}
	})

	It("Will correctly update the positions of host in the index when their resources change", func() {
		clusterPlacer := dockerScheduler.Placer()
		Expect(clusterPlacer).ToNot(BeNil())

		gandivaPlacer, ok := clusterPlacer.(*placer.GandivaPlacer)
		Expect(ok).To(BeTrue())
		Expect(gandivaPlacer).ToNot(BeNil())

		_, _ = createHost(1, dockerCluster, mockCtrl, hostSpec)
		Expect(gandivaPlacer.NumFreeHosts()).To(Equal(1))
		Expect(dockerCluster.Len()).To(Equal(1))

		_, _ = createHost(2, dockerCluster, mockCtrl, hostSpec)
		Expect(gandivaPlacer.NumFreeHosts()).To(Equal(2))
		Expect(dockerCluster.Len()).To(Equal(2))

		kernelResourceSpec := types.NewDecimalSpec(128, 128, 5, 2)
		kernel1Spec := createKernelSpec(kernelResourceSpec)
		kernel2Spec := createKernelSpec(kernelResourceSpec)

		candidateHosts, err := dockerScheduler.SearchForCandidateHosts(1, kernel1Spec, false)
		Expect(err).To(BeNil())
		Expect(len(candidateHosts)).To(Equal(1))
		GinkgoWriter.Printf("Candidate host name: \"%s\"\n", candidateHosts[0].GetNodeName())
		//Expect(candidateHosts[0]).To(Equal(host1))

		candidateHosts, err = dockerScheduler.SearchForCandidateHosts(1, kernel2Spec, false)
		Expect(err).To(BeNil())
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

		candidateHosts, err := dockerScheduler.SearchForCandidateHosts(1, kernelSpec, false)
		Expect(err).To(BeNil())
		Expect(len(candidateHosts)).To(Equal(0))
	})

	It("Will return a candidate host in response to a request", func() {
		clusterPlacer := dockerScheduler.Placer()
		Expect(clusterPlacer).ToNot(BeNil())

		gandivaPlacer, ok := clusterPlacer.(*placer.GandivaPlacer)
		Expect(ok).To(BeTrue())
		Expect(gandivaPlacer).ToNot(BeNil())

		host1, _ := createHost(1, dockerCluster, mockCtrl, hostSpec)

		Expect(gandivaPlacer.NumFreeHosts()).To(Equal(1))

		kernel1ResourceSpec := types.NewDecimalSpec(128, 128, 2, 2)
		kernel1Spec := createKernelSpec(kernel1ResourceSpec)

		By("Returning the only available host when finding a candidate")

		candidateHosts, err := dockerScheduler.SearchForCandidateHosts(1, kernel1Spec, false)
		Expect(err).To(BeNil())
		Expect(len(candidateHosts)).To(Equal(1))
		Expect(candidateHosts[0]).To(Equal(host1))

		Expect(gandivaPlacer.NumFreeHosts()).To(Equal(0))

		Expect(gandivaPlacer.NumHostsInPool(0)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(1)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(2)).To(Equal(1))
		Expect(gandivaPlacer.NumHostsInPool(3)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(4)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(5)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(6)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(7)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(8)).To(Equal(0))

		hostPool, loaded := gandivaPlacer.GetHostPool(2)
		Expect(loaded).To(BeTrue())
		Expect(hostPool).ToNot(BeNil())
		Expect(hostPool.PoolNumber).To(Equal(int32(2)))
		Expect(hostPool.Len()).To(Equal(1))
		Expect(hostPool.Size()).To(Equal(1))

		By("Returning the only available host again when finding a candidate a second time")

		kernel2ResourceSpec := types.NewDecimalSpec(128, 128, 2, 2)
		kernel2Spec := createKernelSpec(kernel2ResourceSpec)

		candidateHosts, err = dockerScheduler.SearchForCandidateHosts(1, kernel2Spec, false)
		Expect(err).To(BeNil())
		Expect(len(candidateHosts)).To(Equal(1))
		Expect(candidateHosts[0]).To(Equal(host1))

		Expect(gandivaPlacer.NumFreeHosts()).To(Equal(0))

		Expect(gandivaPlacer.NumHostsInPool(0)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(1)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(2)).To(Equal(1))
		Expect(gandivaPlacer.NumHostsInPool(3)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(4)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(5)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(6)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(7)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(8)).To(Equal(0))

		hostPool, loaded = gandivaPlacer.GetHostPool(2)
		Expect(loaded).To(BeTrue())
		Expect(hostPool).ToNot(BeNil())
		Expect(hostPool.PoolNumber).To(Equal(int32(2)))
		Expect(hostPool.Len()).To(Equal(1))
		Expect(hostPool.Size()).To(Equal(1))
	})

	It("Will return the least-loaded candidate host in response to a request", func() {
		clusterPlacer := dockerScheduler.Placer()
		Expect(clusterPlacer).ToNot(BeNil())

		gandivaPlacer, ok := clusterPlacer.(*placer.GandivaPlacer)
		Expect(ok).To(BeTrue())
		Expect(gandivaPlacer).ToNot(BeNil())

		host1, _ := createHost(1, dockerCluster, mockCtrl, hostSpec)
		Expect(gandivaPlacer.NumFreeHosts()).To(Equal(1))
		Expect(dockerCluster.Len()).To(Equal(1))

		host2, _ := createHost(2, dockerCluster, mockCtrl, hostSpec)
		Expect(gandivaPlacer.NumFreeHosts()).To(Equal(2))
		Expect(dockerCluster.Len()).To(Equal(2))

		Expect(gandivaPlacer.Len()).To(Equal(2))
		Expect(gandivaPlacer.GetIndex().Len()).To(Equal(2))

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

		candidateHosts, err := dockerScheduler.SearchForCandidateHosts(1, kernel1Spec, false)
		Expect(err).To(BeNil())
		Expect(len(candidateHosts)).To(Equal(1))
		GinkgoWriter.Printf("Candidate host name: \"%s\"\n", candidateHosts[0].GetNodeName())
		Expect(candidateHosts[0]).To(Equal(host1))
		Expect(candidateHosts[0].CommittedResources().Equals(kernel1Spec.ResourceSpec)).To(BeTrue())

		Expect(gandivaPlacer.NumFreeHosts()).To(Equal(1))

		Expect(gandivaPlacer.NumHostsInPool(0)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(1)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(2)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(3)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(4)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(5)).To(Equal(1))
		Expect(gandivaPlacer.NumHostsInPool(6)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(7)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(8)).To(Equal(0))

		hostPool, loaded := gandivaPlacer.GetHostPool(5)
		Expect(loaded).To(BeTrue())
		Expect(hostPool).ToNot(BeNil())
		Expect(hostPool.PoolNumber).To(Equal(int32(5)))
		Expect(hostPool.Len()).To(Equal(1))
		Expect(hostPool.Size()).To(Equal(1))

		Expect(gandivaPlacer.Len()).To(Equal(2))
		Expect(gandivaPlacer.GetIndex().Len()).To(Equal(2))

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

		candidateHosts, err = dockerScheduler.SearchForCandidateHosts(1, kernel2Spec, false)
		Expect(err).To(BeNil())
		Expect(len(candidateHosts)).To(Equal(1))
		GinkgoWriter.Printf("Candidate host name: \"%s\"\n", candidateHosts[0].GetNodeName())
		Expect(candidateHosts[0]).To(Equal(host2))

		Expect(gandivaPlacer.NumFreeHosts()).To(Equal(0))
		Expect(gandivaPlacer.Len()).To(Equal(2))
		Expect(gandivaPlacer.GetIndex().Len()).To(Equal(2))

		Expect(gandivaPlacer.NumHostsInPool(0)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(1)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(2)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(3)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(4)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(5)).To(Equal(2))
		Expect(gandivaPlacer.NumHostsInPool(6)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(7)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(8)).To(Equal(0))

		hostPool, loaded = gandivaPlacer.GetHostPool(5)
		Expect(loaded).To(BeTrue())
		Expect(hostPool).ToNot(BeNil())
		Expect(hostPool.PoolNumber).To(Equal(int32(5)))
		Expect(hostPool.Len()).To(Equal(2))
		Expect(hostPool.Size()).To(Equal(2))

		By("Returning the correct host after further resource adjustments have occurred")

		// Artificially increase the resources available on host #1.
		releaseResources(host1, types.NewDecimalSpec(128, 128, 2, 2), dockerCluster, []int{8, 9})

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

		candidateHosts, err = dockerScheduler.SearchForCandidateHosts(1, kernel3Spec, false)
		Expect(err).To(BeNil())
		Expect(len(candidateHosts)).To(Equal(1))
		GinkgoWriter.Printf("Candidate host name: \"%s\"\n", candidateHosts[0].GetNodeName())
		Expect(candidateHosts[0]).To(Equal(host1))

		Expect(gandivaPlacer.NumFreeHosts()).To(Equal(0))

		Expect(gandivaPlacer.NumHostsInPool(0)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(1)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(2)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(3)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(4)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(5)).To(Equal(2))
		Expect(gandivaPlacer.NumHostsInPool(6)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(7)).To(Equal(0))
		Expect(gandivaPlacer.NumHostsInPool(8)).To(Equal(0))

		hostPool, loaded = gandivaPlacer.GetHostPool(5)
		Expect(loaded).To(BeTrue())
		Expect(hostPool).ToNot(BeNil())
		Expect(hostPool.PoolNumber).To(Equal(int32(5)))
		Expect(hostPool.Len()).To(Equal(2))
		Expect(hostPool.Size()).To(Equal(2))
	})

	It("Will correctly allocate jobs with different GPU requirements to different pools", func() {
		clusterPlacer := dockerScheduler.Placer()
		Expect(clusterPlacer).ToNot(BeNil())

		gandivaPlacer, ok := clusterPlacer.(*placer.GandivaPlacer)
		Expect(ok).To(BeTrue())
		Expect(gandivaPlacer).ToNot(BeNil())

		numHosts := 8
		hosts := make([]scheduling.Host, 0, numHosts)

		expectedHostPoolSizes := []int{0, 0, 0, 0, 0, 0, 0, 0, 0}
		for i := 0; i < numHosts; i++ {
			host, _ := createHost(1, dockerCluster, mockCtrl, hostSpec)
			hosts = append(hosts, host)
		}

		Expect(gandivaPlacer.NumFreeHosts()).To(Equal(numHosts))

		for numGpus, expectedHostPoolSize := range expectedHostPoolSizes {
			Expect(gandivaPlacer.HasHostPool(int32(numGpus))).To(BeTrue())
			Expect(gandivaPlacer.NumHostsInPool(int32(numGpus))).To(Equal(expectedHostPoolSize))
		}

		numGpusPerSession := []float64{1, 2, 4, 8, 4, 2, 1, 8, 2, 2, 2}
		numSessions := len(numGpusPerSession)
		kernelSpecs := make([]*proto.KernelSpec, 0, numSessions)

		for i := 0; i < numSessions; i++ {
			numGpus := numGpusPerSession[i]
			vramGb := numGpus * 2

			kernelResourceSpec := types.NewDecimalSpec(128, 128, numGpus, vramGb)
			kernelSpec := createKernelSpec(kernelResourceSpec)

			kernelSpecs = append(kernelSpecs, kernelSpec)
		}

		//
		//
		By("Adding a host to the 1-GPU host pool")

		sessionIndex := 0
		kernelSpec := kernelSpecs[sessionIndex]
		candidateHosts, err := dockerScheduler.SearchForCandidateHosts(1, kernelSpec, false)
		Expect(err).To(BeNil())
		Expect(len(candidateHosts)).To(Equal(1))

		candidateHost := candidateHosts[0]
		Expect(candidateHost).To(Equal(hosts[0]))

		Expect(gandivaPlacer.NumFreeHosts()).To(Equal(numHosts - 1))

		GinkgoWriter.Printf("Committed TransactionResources: %s\n", candidateHost.CommittedResources().String())
		Expect(candidateHost.CommittedResources().Equals(kernelSpec.ResourceSpec)).To(BeTrue())

		expectedHostPoolSizes[1] = 1
		for numGpus, expectedHostPoolSize := range expectedHostPoolSizes {
			Expect(gandivaPlacer.HasHostPool(int32(numGpus))).To(BeTrue())
			Expect(gandivaPlacer.NumHostsInPool(int32(numGpus))).To(Equal(expectedHostPoolSize))
		}

		//
		//
		By("Adding a host to the 2-GPU host pool")

		sessionIndex = 1
		kernelSpec = kernelSpecs[sessionIndex]
		candidateHosts, err = dockerScheduler.SearchForCandidateHosts(1, kernelSpec, false)
		Expect(err).To(BeNil())
		Expect(len(candidateHosts)).To(Equal(1))

		candidateHost = candidateHosts[0]
		GinkgoWriter.Printf("Candidate host: Host %s (ID=%s)\n", candidateHost.GetNodeName(), candidateHost.GetID())
		Expect(candidateHost).To(Equal(hosts[1]))

		Expect(gandivaPlacer.NumFreeHosts()).To(Equal(numHosts - 2))

		GinkgoWriter.Printf("Committed TransactionResources: %s\n", candidateHost.CommittedResources().String())
		Expect(candidateHost.CommittedResources().Equals(kernelSpec.ResourceSpec)).To(BeTrue())

		expectedHostPoolSizes[2] = 1
		for numGpus, expectedHostPoolSize := range expectedHostPoolSizes {
			Expect(gandivaPlacer.HasHostPool(int32(numGpus))).To(BeTrue())
			Expect(gandivaPlacer.NumHostsInPool(int32(numGpus))).To(Equal(expectedHostPoolSize))
		}

		//
		//
		By("Adding a host to the 4-GPU host pool")

		sessionIndex = 2
		kernelSpec = kernelSpecs[sessionIndex]
		candidateHosts, err = dockerScheduler.SearchForCandidateHosts(1, kernelSpec, false)
		Expect(err).To(BeNil())
		Expect(len(candidateHosts)).To(Equal(1))

		candidateHost = candidateHosts[0]
		GinkgoWriter.Printf("Candidate host: Host %s (ID=%s)\n", candidateHost.GetNodeName(), candidateHost.GetID())
		Expect(candidateHost).To(Equal(hosts[2]))

		Expect(gandivaPlacer.NumFreeHosts()).To(Equal(numHosts - 3))

		GinkgoWriter.Printf("Committed TransactionResources: %s\n", candidateHost.CommittedResources().String())
		Expect(candidateHost.CommittedResources().Equals(kernelSpec.ResourceSpec)).To(BeTrue())

		expectedHostPoolSizes[4] = 1
		for numGpus, expectedHostPoolSize := range expectedHostPoolSizes {
			Expect(gandivaPlacer.HasHostPool(int32(numGpus))).To(BeTrue())
			Expect(gandivaPlacer.NumHostsInPool(int32(numGpus))).To(Equal(expectedHostPoolSize))
		}

		//
		//
		By("Adding a host to the 8-GPU host pool")

		sessionIndex = 3
		kernelSpec = kernelSpecs[sessionIndex]
		candidateHosts, err = dockerScheduler.SearchForCandidateHosts(1, kernelSpec, false)
		Expect(err).To(BeNil())
		Expect(len(candidateHosts)).To(Equal(1))

		candidateHost = candidateHosts[0]
		GinkgoWriter.Printf("Candidate host: Host %s (ID=%s)\n", candidateHost.GetNodeName(), candidateHost.GetID())
		Expect(candidateHost).To(Equal(hosts[3]))

		Expect(gandivaPlacer.NumFreeHosts()).To(Equal(numHosts - 4))

		GinkgoWriter.Printf("Committed TransactionResources: %s\n", candidateHost.CommittedResources().String())
		Expect(candidateHost.CommittedResources().Equals(kernelSpec.ResourceSpec)).To(BeTrue())

		expectedHostPoolSizes[8] = 1
		for numGpus, expectedHostPoolSize := range expectedHostPoolSizes {
			Expect(gandivaPlacer.HasHostPool(int32(numGpus))).To(BeTrue())
			Expect(gandivaPlacer.NumHostsInPool(int32(numGpus))).To(Equal(expectedHostPoolSize))
		}

		//
		//
		//
		By("Not adding a host to the 4-GPU host pool as the current host can be used")

		sessionIndex = 4
		kernelSpec = kernelSpecs[sessionIndex]
		candidateHosts, err = dockerScheduler.SearchForCandidateHosts(1, kernelSpec, false)
		Expect(err).To(BeNil())
		Expect(len(candidateHosts)).To(Equal(1))

		candidateHost = candidateHosts[0]
		GinkgoWriter.Printf("Candidate host: Host %s (ID=%s)\n", candidateHost.GetNodeName(), candidateHost.GetID())
		Expect(candidateHost).To(Equal(hosts[2]))

		Expect(gandivaPlacer.NumFreeHosts()).To(Equal(numHosts - 4)) // Same as before

		GinkgoWriter.Printf("Committed TransactionResources: %s\n", candidateHost.CommittedResources().String())
		combinedSpec := kernelSpecs[2].ResourceSpec.ToDecimalSpec().Add(kernelSpec.ResourceSpec.ToDecimalSpec())
		Expect(candidateHost.CommittedResources().Equals(combinedSpec)).To(BeTrue())

		for numGpus, expectedHostPoolSize := range expectedHostPoolSizes {
			Expect(gandivaPlacer.HasHostPool(int32(numGpus))).To(BeTrue())
			Expect(gandivaPlacer.NumHostsInPool(int32(numGpus))).To(Equal(expectedHostPoolSize))
		}

		//
		// 2, 1, 8
		By("Not adding a host to the 2-GPU host pool as the current host can be used")

		sessionIndex = 5
		kernelSpec = kernelSpecs[sessionIndex]
		candidateHosts, err = dockerScheduler.SearchForCandidateHosts(1, kernelSpec, false)
		Expect(err).To(BeNil())
		Expect(len(candidateHosts)).To(Equal(1))

		candidateHost = candidateHosts[0]
		GinkgoWriter.Printf("Candidate host: Host %s (ID=%s)\n", candidateHost.GetNodeName(), candidateHost.GetID())
		Expect(candidateHost).To(Equal(hosts[1]))

		Expect(gandivaPlacer.NumFreeHosts()).To(Equal(numHosts - 4)) // Same as before

		GinkgoWriter.Printf("Committed TransactionResources: %s\n", candidateHost.CommittedResources().String())
		combinedSpec = kernelSpecs[1].ResourceSpec.ToDecimalSpec().Add(kernelSpec.ResourceSpec.ToDecimalSpec())
		Expect(candidateHost.CommittedResources().Equals(combinedSpec)).To(BeTrue())

		for numGpus, expectedHostPoolSize := range expectedHostPoolSizes {
			Expect(gandivaPlacer.HasHostPool(int32(numGpus))).To(BeTrue())
			Expect(gandivaPlacer.NumHostsInPool(int32(numGpus))).To(Equal(expectedHostPoolSize))
		}

		//
		// 1, 8
		By("Not adding a host to the 1-GPU host pool as the current host can be used")

		sessionIndex = 6
		kernelSpec = kernelSpecs[sessionIndex]
		candidateHosts, err = dockerScheduler.SearchForCandidateHosts(1, kernelSpec, false)
		Expect(err).To(BeNil())
		Expect(len(candidateHosts)).To(Equal(1))

		candidateHost = candidateHosts[0]
		GinkgoWriter.Printf("Candidate host: Host %s (ID=%s)\n", candidateHost.GetNodeName(), candidateHost.GetID())
		Expect(candidateHost).To(Equal(hosts[0]))

		Expect(gandivaPlacer.NumFreeHosts()).To(Equal(numHosts - 4)) // Same as before

		GinkgoWriter.Printf("Committed TransactionResources: %s\n", candidateHost.CommittedResources().String())
		combinedSpec = kernelSpecs[0].ResourceSpec.ToDecimalSpec().Add(kernelSpec.ResourceSpec.ToDecimalSpec())
		Expect(candidateHost.CommittedResources().Equals(combinedSpec)).To(BeTrue())

		for numGpus, expectedHostPoolSize := range expectedHostPoolSizes {
			Expect(gandivaPlacer.HasHostPool(int32(numGpus))).To(BeTrue())
			Expect(gandivaPlacer.NumHostsInPool(int32(numGpus))).To(Equal(expectedHostPoolSize))
		}

		//
		// 8
		By("Adding a host to the 8-GPU host pool as the current host cannot be used")

		sessionIndex = 7
		kernelSpec = kernelSpecs[sessionIndex]
		candidateHosts, err = dockerScheduler.SearchForCandidateHosts(1, kernelSpec, false)
		Expect(err).To(BeNil())
		Expect(len(candidateHosts)).To(Equal(1))

		candidateHost = candidateHosts[0]
		GinkgoWriter.Printf("Candidate host: Host %s (ID=%s)\n", candidateHost.GetNodeName(), candidateHost.GetID())
		Expect(candidateHost).To(Equal(hosts[4]))

		Expect(gandivaPlacer.NumFreeHosts()).To(Equal(numHosts - 5)) // One less than before

		GinkgoWriter.Printf("Committed TransactionResources: %s\n", candidateHost.CommittedResources().String())
		Expect(candidateHost.CommittedResources().Equals(kernelSpec.ResourceSpec)).To(BeTrue())

		expectedHostPoolSizes[8] = 2
		for numGpus, expectedHostPoolSize := range expectedHostPoolSizes {
			Expect(gandivaPlacer.HasHostPool(int32(numGpus))).To(BeTrue())
			Expect(gandivaPlacer.NumHostsInPool(int32(numGpus))).To(Equal(expectedHostPoolSize))
		}

		//
		//
		// 2, 2, 2
		By("Not yet adding a host to the 2-GPU host pool as the current host can be used")

		sessionIndex = 8
		kernelSpec = kernelSpecs[sessionIndex]
		candidateHosts, err = dockerScheduler.SearchForCandidateHosts(1, kernelSpec, false)
		Expect(err).To(BeNil())
		Expect(len(candidateHosts)).To(Equal(1))

		candidateHost = candidateHosts[0]
		GinkgoWriter.Printf("Candidate host: Host %s (ID=%s)\n", candidateHost.GetNodeName(), candidateHost.GetID())
		Expect(candidateHost).To(Equal(hosts[1]))

		Expect(gandivaPlacer.NumFreeHosts()).To(Equal(numHosts - 5)) // Same as before

		GinkgoWriter.Printf("Committed TransactionResources: %s\n", candidateHost.CommittedResources().String())
		combinedSpec = kernelSpecs[1].ResourceSpec.ToDecimalSpec().Add(kernelSpecs[5].ResourceSpec.ToDecimalSpec()).Add(kernelSpec.ResourceSpec.ToDecimalSpec())
		Expect(candidateHost.CommittedResources().Equals(combinedSpec)).To(BeTrue())

		for numGpus, expectedHostPoolSize := range expectedHostPoolSizes {
			Expect(gandivaPlacer.HasHostPool(int32(numGpus))).To(BeTrue())
			Expect(gandivaPlacer.NumHostsInPool(int32(numGpus))).To(Equal(expectedHostPoolSize))
		}

		//
		//
		// 2, 2
		By("Not yet adding a host to the 2-GPU host pool as the current host can be used")

		sessionIndex = 9
		kernelSpec = kernelSpecs[sessionIndex]
		candidateHosts, err = dockerScheduler.SearchForCandidateHosts(1, kernelSpec, false)
		Expect(err).To(BeNil())
		Expect(len(candidateHosts)).To(Equal(1))

		candidateHost = candidateHosts[0]
		GinkgoWriter.Printf("Candidate host: Host %s (ID=%s)\n", candidateHost.GetNodeName(), candidateHost.GetID())
		Expect(candidateHost).To(Equal(hosts[1]))

		Expect(gandivaPlacer.NumFreeHosts()).To(Equal(numHosts - 5)) // Same as before

		GinkgoWriter.Printf("Committed TransactionResources: %s\n", candidateHost.CommittedResources().String())
		combinedSpec = kernelSpecs[1].ResourceSpec.ToDecimalSpec().
			Add(kernelSpecs[5].ResourceSpec.ToDecimalSpec()).
			Add(kernelSpecs[8].ResourceSpec.ToDecimalSpec()).
			Add(kernelSpec.ResourceSpec.ToDecimalSpec())
		Expect(candidateHost.CommittedResources().Equals(combinedSpec)).To(BeTrue())

		for numGpus, expectedHostPoolSize := range expectedHostPoolSizes {
			Expect(gandivaPlacer.HasHostPool(int32(numGpus))).To(BeTrue())
			Expect(gandivaPlacer.NumHostsInPool(int32(numGpus))).To(Equal(expectedHostPoolSize))
		}

		//
		//
		// 2
		By("Adding a host to the 2-GPU host pool as the current host cannot be used")

		sessionIndex = 10
		kernelSpec = kernelSpecs[sessionIndex]
		candidateHosts, err = dockerScheduler.SearchForCandidateHosts(1, kernelSpec, false)
		Expect(err).To(BeNil())
		Expect(len(candidateHosts)).To(Equal(1))

		candidateHost = candidateHosts[0]
		GinkgoWriter.Printf("Candidate host: Host %s (ID=%s)\n", candidateHost.GetNodeName(), candidateHost.GetID())
		Expect(candidateHost).To(Equal(hosts[5]))

		Expect(gandivaPlacer.NumFreeHosts()).To(Equal(numHosts - 6)) // One less than before

		GinkgoWriter.Printf("Committed TransactionResources: %s\n", candidateHost.CommittedResources().String())
		Expect(candidateHost.CommittedResources().Equals(kernelSpec.ResourceSpec)).To(BeTrue())

		expectedHostPoolSizes[2] = 2
		for numGpus, expectedHostPoolSize := range expectedHostPoolSizes {
			Expect(gandivaPlacer.HasHostPool(int32(numGpus))).To(BeTrue())
			Expect(gandivaPlacer.NumHostsInPool(int32(numGpus))).To(Equal(expectedHostPoolSize))
		}
	})
})
