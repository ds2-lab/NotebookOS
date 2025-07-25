package prewarm_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	jupyter "github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/mock_scheduling"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/client"
	"github.com/scusemua/distributed-notebook/common/scheduling/cluster"
	"github.com/scusemua/distributed-notebook/common/scheduling/policy"
	"github.com/scusemua/distributed-notebook/common/scheduling/prewarm"
	"github.com/scusemua/distributed-notebook/common/scheduling/scheduler"
	"github.com/scusemua/distributed-notebook/gateway/daemon"
	"github.com/scusemua/distributed-notebook/gateway/domain"
	"go.uber.org/mock/gomock"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var _ = Describe("Base Prewarmer Tests", func() {
	gatewayStaticConfigJson := `{
  "jaeger_addr": "",
  "consul_addr": "",
  "connection_info": {
    "ip": "",
    "transport": "tcp",
    "signature_scheme": "",
    "key": "",
    "control_port": 17201,
    "shell_port": 17202,
    "stdin_port": 17203,
    "hb_port": 17200,
    "iopub_port": 17204,
    "iosub_port": 17205,
    "ack_port": 17206,
    "starting_resource_port": 17207,
    "num_resource_ports": 14096
  },
  "cluster_daemon_options": {
    "local-daemon-service-name": "local-daemon-network",
    "global-daemon-service-name": "daemon-network",
    "kubernetes-namespace": "",
    "notebook-image-name": "scusemua/jupyter-cpu:latest",
    "notebook-image-tag": "latest",
    "cluster_scheduler_options": {
      "common_options": {
        "deployment_mode": "docker-compose",
        "docker_app_name": "",
        "docker_network_name": "distributed_cluster_default",
        "scheduling-policy": "static",
        "idle-session-reclamation-policy": "none",
        "remote-storage-endpoint": "redis:6379",
        "remote-storage": "redis",
        "gpus-per-host": 8,
        "prometheus_interval": 15,
        "prometheus_port": -1,
        "num_resend_attempts": 1,
        "smr-port": 17080,
        "debug_port": 19996,
        "election_timeout_seconds": 3,
        "local_mode": true,
        "use_real_gpus": false,
        "acks_enabled": false,
        "debug_mode": true,
        "simulate_checkpointing_latency": true,
        "disable_prometheus_metrics_publishing": false,
        "simulate_training_using_sleep": false,
        "bind_debugpy_port": false,
        "save_stopped_kernel_containers": false
      },
      "custom_idle_session_reclamation_options": {
        "idle_session_replay_all_cells": false,
        "idle_session_timeout_interval_sec": 0
      },
      "subscribed-ratio-update-interval": 0,
      "scaling-factor": 1.1,
      "scaling-interval": 15,
      "scaling-limit": 1.15,
      "scaling-in-limit": 2,
      "scaling-buffer-size": 3,
      "min_cluster_nodes": 6,
      "max_cluster_nodes": 48,
      "gpu_poll_interval": 5,
      "max-subscribed-ratio": 7,
      "execution-time-sampling-window": 10,
      "migration-time-sampling-window": 10,
      "scheduler-http-port": 18078,
      "mean_scale_out_per_host_sec": 15,
      "std_dev_scale_out_per_host_sec": 2,
      "mean_scale_in_per_host_sec": 10,
      "std_dev_scale_in_per_host_sec": 1,
      "millicpus_per_host": 0,
      "memory_mb_per_host": 0,
      "vram_gb_per_host": 0,
      "predictive_autoscaling": false,
      "assign_kernel_debug_ports": false,
      "prewarming_enabled": true,
      "initial_num_containers_per_host": 1,
      "min_prewarm_containers_per_host": 1,
      "max_prewarm_containers_per_host": 3,
      "prewarming_policy": "no_maintenance"
    },
    "local-daemon-service-port": 18075,
    "global-daemon-service-port": 0,
    "distributed-cluster-service-port": 18079,
    "remote-docker-event-aggregator-port": 15821,
    "initial-cluster-size": 12,
    "initial-connection-period": 60,
    "idle_session_reclamation_interval_sec": 30,
    "submit_execute_requests_one_at_a_time": true,
    "use-stateful-set": false,
    "idle_session_reclamation_enabled": true
  },
  "port": 18080,
  "provisioner_port": 18081,
  "pretty_print_options": true
}`

	var (
		mockCtrl              *gomock.Controller
		schedulingPolicy      scheduling.Policy
		mockCluster           *mock_scheduling.MockCluster
		mockScheduler         *mock_scheduling.MockScheduler
		mockPlacer            *mock_scheduling.MockPlacer
		mockMetricsProvider   *mock_scheduling.MockMetricsProvider
		clusterGatewayOptions *domain.ClusterGatewayOptions
		prewarmer             scheduling.ContainerPrewarmer
	)

	// createAndInitializePrewarmer initializes the existing prewarmer variable defined above.
	createAndInitializePrewarmer := func(initSize, maxSize int) {
		prewarmerConfig := prewarm.NewPrewarmerConfig(initSize, maxSize, 0 /* Default of 5sec will be used */)
		prewarmer = prewarm.NewBaseContainerPrewarmer(mockCluster, prewarmerConfig, mockMetricsProvider)
	}

	Context("Unit Tests", func() {
		clusterProvider := func() scheduling.Cluster {
			return mockCluster
		}

		AfterEach(func() {
			if prewarmer != nil {
				_ = prewarmer.Stop()
			}
		})

		BeforeEach(func() {
			mockCtrl = gomock.NewController(GinkgoT())

			err := json.Unmarshal([]byte(gatewayStaticConfigJson), &clusterGatewayOptions)
			GinkgoWriter.Printf("Error: %v\n", err)
			Expect(err).To(BeNil())

			GinkgoWriter.Printf("clusterGatewayOptions:\n%v\n", clusterGatewayOptions.PrettyString(2))
			Expect(clusterGatewayOptions.SchedulerOptions.PrewarmingEnabled).To(BeTrue())
			Expect(clusterGatewayOptions.SchedulerOptions.PrewarmingPolicy).To(Equal(scheduling.NoMaintenance.String()))

			schedulingPolicy, err = policy.NewStaticPolicy(&clusterGatewayOptions.SchedulerOptions, clusterProvider)
			Expect(err).To(BeNil())
			Expect(schedulingPolicy).ToNot(BeNil())
			Expect(schedulingPolicy.PolicyKey()).To(Equal(scheduling.Static))

			mockCluster = mock_scheduling.NewMockCluster(mockCtrl)
			mockPlacer = mock_scheduling.NewMockPlacer(mockCtrl)
			mockScheduler = mock_scheduling.NewMockScheduler(mockCtrl)
			mockMetricsProvider = mock_scheduling.NewMockMetricsProvider(mockCtrl)

			mockCluster.EXPECT().Scheduler().AnyTimes().Return(mockScheduler)
			mockCluster.EXPECT().MetricsProvider().AnyTimes().Return(mockMetricsProvider)

			mockScheduler.EXPECT().Policy().AnyTimes().Return(schedulingPolicy)
			mockScheduler.EXPECT().Placer().AnyTimes().Return(mockPlacer)
			mockScheduler.EXPECT().ContainerPrewarmer().AnyTimes().Return(prewarmer)
		})

		AfterEach(func() {
			mockCtrl.Finish()
		})

		It("Will correctly creates no pre-warm containers for an empty cluster", func() {
			initialCapacity := 1
			minCapacity := 0
			maxCapacity := 2

			By("Being created or instantiated correctly")

			createAndInitializePrewarmer(initialCapacity, maxCapacity)

			Expect(prewarmer).ToNot(BeNil())
			_, ok := prewarmer.(*prewarm.BaseContainerPrewarmer)
			Expect(ok).To(BeTrue())

			Expect(prewarmer.InitialPrewarmedContainersPerHost()).To(Equal(initialCapacity))
			Expect(prewarmer.MinPrewarmedContainersPerHost()).To(Equal(minCapacity))
			Expect(prewarmer.MaxPrewarmedContainersPerHost()).To(Equal(maxCapacity))

			Expect(prewarmer.Len()).To(Equal(0))

			Expect(prewarmer).ToNot(BeNil())
			_, ok = prewarmer.(*prewarm.BaseContainerPrewarmer)
			Expect(ok).To(BeTrue())

			By("Correctly provisioning the initial round of prewarm containers")

			// Empty cluster.
			mockCluster.EXPECT().RangeOverHosts(gomock.Any()).Times(1)

			created, target := prewarmer.ProvisionInitialPrewarmContainers()

			Expect(created).To(Equal(int32(0)))
			Expect(target).To(Equal(int32(0)))
		})

		It("Will return correct values when querying the size of the container pool", func() {
			numHosts := 1
			initialCapacity := 1
			minCapacity := 0
			maxCapacity := 2

			By("Being created or instantiated correctly")

			createAndInitializePrewarmer(initialCapacity, maxCapacity)

			Expect(prewarmer).ToNot(BeNil())
			_, ok := prewarmer.(*prewarm.BaseContainerPrewarmer)
			Expect(ok).To(BeTrue())

			Expect(prewarmer.InitialPrewarmedContainersPerHost()).To(Equal(initialCapacity))
			Expect(prewarmer.MinPrewarmedContainersPerHost()).To(Equal(minCapacity))
			Expect(prewarmer.MaxPrewarmedContainersPerHost()).To(Equal(maxCapacity))

			Expect(prewarmer.Len()).To(Equal(0))

			By("Correctly provisioning the initial round of prewarm containers")

			Expect(prewarmer.Len()).To(Equal(0))

			hosts, localGatewayClients := createHosts(numHosts, 0, hostSpec, mockCluster, mockCtrl)
			Expect(len(hosts)).To(Equal(numHosts))
			Expect(len(localGatewayClients)).To(Equal(numHosts))

			var blockStartReplicaWg sync.WaitGroup
			blockStartReplicaWg.Add(1)

			var startReplicaCalledWg sync.WaitGroup
			startReplicaCalledWg.Add(1)

			mockCluster.
				EXPECT().
				RangeOverHosts(gomock.Any()).
				Times(1).
				DoAndReturn(func(f func(key string, value scheduling.Host) bool) {
					for idx, host := range hosts {
						connInfo := &proto.KernelConnectionInfo{
							Ip:              fmt.Sprintf("10.0.0.%d", idx+1),
							Transport:       "tcp",
							ControlPort:     9000,
							ShellPort:       9001,
							StdinPort:       9002,
							HbPort:          9003,
							IopubPort:       9004,
							IosubPort:       9005,
							SignatureScheme: jupyter.JupyterSignatureScheme,
							Key:             uuid.NewString(),
						}

						localGatewayClient := localGatewayClients[idx]
						localGatewayClient.
							EXPECT().
							StartKernelReplica(gomock.Any(), gomock.Any(), gomock.Any()).
							Times(initialCapacity).
							DoAndReturn(func(ctx context.Context, in *proto.KernelReplicaSpec, opts ...grpc.CallOption) (*proto.KernelConnectionInfo, error) {
								globalLogger.Info("Creating prewarm container on host %s (ID=%s).",
									host.GetNodeName(), host.GetID())

								startReplicaCalledWg.Done()

								globalLogger.Info("Passed startReplicaCalledWg.Done()")

								blockStartReplicaWg.Wait()

								globalLogger.Info("Passed blockStartReplicaWg.Wait()")

								time.Sleep(time.Millisecond*5 + (time.Millisecond * time.Duration(rand.Intn(10))))
								return connInfo, nil
							})

						f(host.GetID(), host)
					}
				})

			createdChan, targetChan := make(chan int32, 1), make(chan int32, 1)

			By("Returning the correct size values before provisioning any containers")

			Expect(prewarmer.Len()).To(Equal(0))

			for _, host := range hosts {
				curr, provisioning := prewarmer.GetNumPrewarmContainersOnHost(host)
				Expect(curr).To(Equal(0))
				Expect(provisioning).To(Equal(0))
			}

			go func() {
				globalLogger.Info("Provision initial prewarm containers")
				created, target := prewarmer.ProvisionInitialPrewarmContainers()
				globalLogger.Info("Provisioned initial prewarm containers")

				createdChan <- created
				targetChan <- target
			}()

			By("Returning the correct size values while provisioning a container")

			startReplicaCalledWg.Wait()

			Expect(prewarmer.Len()).To(Equal(0))

			for _, host := range hosts {
				curr, provisioning := prewarmer.GetNumPrewarmContainersOnHost(host)
				Expect(curr).To(Equal(0))
				Expect(provisioning).To(Equal(1))
			}

			time.Sleep(time.Millisecond * 250)

			Expect(prewarmer.Len()).To(Equal(0))

			for _, host := range hosts {
				curr, provisioning := prewarmer.GetNumPrewarmContainersOnHost(host)
				Expect(curr).To(Equal(0))
				Expect(provisioning).To(Equal(1))
			}

			blockStartReplicaWg.Done()

			Eventually(func() bool {
				if prewarmer.Len() != (numHosts * initialCapacity) {
					return false
				}

				for _, host := range hosts {
					curr, provisioning := prewarmer.GetNumPrewarmContainersOnHost(host)
					if curr != (numHosts * initialCapacity) {
						return false
					}

					if provisioning != 0 {
						return false
					}
				}

				return true
			}, time.Second*5, time.Millisecond*100).Should(BeTrue())

			var created, target int32
			Eventually(createdChan, time.Second*1, time.Millisecond*250).Should(Receive(&created))
			Eventually(targetChan, time.Second*1, time.Millisecond*250).Should(Receive(&target))

			Expect(created).To(Equal(int32(numHosts * initialCapacity)))
			Expect(target).To(Equal(int32(numHosts * initialCapacity)))

			Expect(prewarmer.Len()).To(Equal(numHosts * initialCapacity))
		})

		It("Will return an error when requesting a prewarm container from a host with no prewarm containers", func() {
			numHosts := 3
			initialCapacity := 0
			maxCapacity := 2

			createAndInitializePrewarmer(initialCapacity, maxCapacity)

			By("Correctly provisioning the initial round of prewarm containers")

			hosts, _ := createHosts(numHosts, 0, hostSpec, mockCluster, mockCtrl)

			created, target := prewarmer.ProvisionInitialPrewarmContainers()

			Expect(created).To(Equal(int32(numHosts * initialCapacity)))
			Expect(target).To(Equal(int32(numHosts * initialCapacity)))
			Expect(prewarmer.Len()).To(Equal(numHosts * initialCapacity))

			// First time, the hosts aren't even "registered" yet
			for _, host := range hosts {
				container, err := prewarmer.RequestPrewarmedContainer(host)
				Expect(container).To(BeNil())
				Expect(err).ToNot(BeNil())
				Expect(errors.Is(err, prewarm.ErrNoPrewarmedContainersAvailable)).To(BeTrue())
			}

			// This time, the hosts are registered, but they still have no containers
			for _, host := range hosts {
				container, err := prewarmer.RequestPrewarmedContainer(host)
				Expect(container).To(BeNil())
				Expect(err).ToNot(BeNil())
				Expect(errors.Is(err, prewarm.ErrNoPrewarmedContainersAvailable)).To(BeTrue())
			}
		})

		It("Will correctly return pre-warm containers when they are available", func() {
			numHosts := 3
			initialCapacity := 3
			maxCapacity := 5

			By("Being created or instantiated correctly")

			createAndInitializePrewarmer(initialCapacity, maxCapacity)

			hosts, localGatewayClients := createHosts(numHosts, 0, hostSpec, mockCluster, mockCtrl)

			mockCluster.
				EXPECT().
				RangeOverHosts(gomock.Any()).
				Times(1).
				DoAndReturn(func(f func(key string, value scheduling.Host) bool) {
					for idx, host := range hosts {
						connInfo := &proto.KernelConnectionInfo{
							Ip:              fmt.Sprintf("10.0.0.%d", idx+1),
							Transport:       "tcp",
							ControlPort:     9000,
							ShellPort:       9001,
							StdinPort:       9002,
							HbPort:          9003,
							IopubPort:       9004,
							IosubPort:       9005,
							SignatureScheme: jupyter.JupyterSignatureScheme,
							Key:             uuid.NewString(),
						}

						localGatewayClient := localGatewayClients[idx]
						localGatewayClient.
							EXPECT().
							StartKernelReplica(gomock.Any(), gomock.Any(), gomock.Any()).
							Times(initialCapacity).
							DoAndReturn(func(ctx context.Context, in *proto.KernelReplicaSpec, opts ...grpc.CallOption) (*proto.KernelConnectionInfo, error) {
								time.Sleep(time.Millisecond*5 + (time.Millisecond * time.Duration(rand.Intn(10))))
								globalLogger.Info("Creating prewarm container on host %s (ID=%s).\n",
									host.GetNodeName(), host.GetID())
								return connInfo, nil
							})

						f(host.GetID(), host)
					}
				})

			created, target := prewarmer.ProvisionInitialPrewarmContainers()

			Expect(created).To(Equal(int32(numHosts * initialCapacity)))
			Expect(target).To(Equal(int32(numHosts * initialCapacity)))

			Expect(prewarmer.Len()).To(Equal(numHosts * initialCapacity))

		})

		It("Will allow for the dynamic request of additional prewarm containers", func() {
			numHosts := 3
			initialCapacity := 3
			maxCapacity := 4

			By("Being created or instantiated correctly")

			createAndInitializePrewarmer(initialCapacity, maxCapacity)

			hosts, localGatewayClients := createHosts(numHosts, 0, hostSpec, mockCluster, mockCtrl)

			mockCluster.
				EXPECT().
				RangeOverHosts(gomock.Any()).
				Times(1).
				DoAndReturn(func(f func(key string, value scheduling.Host) bool) {
					for idx, host := range hosts {
						connInfo := &proto.KernelConnectionInfo{
							Ip:              fmt.Sprintf("10.0.0.%d", idx+1),
							Transport:       "tcp",
							ControlPort:     9000,
							ShellPort:       9001,
							StdinPort:       9002,
							HbPort:          9003,
							IopubPort:       9004,
							IosubPort:       9005,
							SignatureScheme: jupyter.JupyterSignatureScheme,
							Key:             uuid.NewString(),
						}

						localGatewayClient := localGatewayClients[idx]
						localGatewayClient.
							EXPECT().
							StartKernelReplica(gomock.Any(), gomock.Any(), gomock.Any()).
							Times(initialCapacity).
							DoAndReturn(func(ctx context.Context, in *proto.KernelReplicaSpec, opts ...grpc.CallOption) (*proto.KernelConnectionInfo, error) {
								time.Sleep(time.Millisecond*5 + (time.Millisecond * time.Duration(rand.Intn(10))))
								globalLogger.Info("Creating prewarm container on host %s (ID=%s).\n",
									host.GetNodeName(), host.GetID())
								return connInfo, nil
							})

						f(host.GetID(), host)
					}
				})

			created, target := prewarmer.ProvisionInitialPrewarmContainers()

			Expect(created).To(Equal(int32(numHosts * initialCapacity)))
			Expect(target).To(Equal(int32(numHosts * initialCapacity)))

			Expect(prewarmer.Len()).To(Equal(numHosts * initialCapacity))

			By("Returning containers when they are requested (and they are available)")

			currSize := numHosts * initialCapacity
			Expect(prewarmer.Len()).To(Equal(currSize))

			// Request the containers
			containers := make([]scheduling.PrewarmedContainer, 0, len(hosts))
			for _, host := range hosts {
				container, err := prewarmer.RequestPrewarmedContainer(host)
				Expect(container).ToNot(BeNil())
				Expect(err).To(BeNil())
				Expect(container.Host()).To(Equal(host))
				containers = append(containers, container)
				currSize -= 1
			}

			// Mark the containers as used
			for _, container := range containers {
				container.OnPrewarmedContainerUsed()
				Expect(prewarmer.Len()).To(Equal(currSize))
			}

			By("Returning containers even after some have been used")

			// Request the containers
			containers = make([]scheduling.PrewarmedContainer, 0, len(hosts))
			for _, host := range hosts {
				container, err := prewarmer.RequestPrewarmedContainer(host)
				Expect(container).ToNot(BeNil())
				Expect(err).To(BeNil())
				Expect(container.Host()).To(Equal(host))

				currSize -= 1
				containers = append(containers, container)
			}

			Expect(prewarmer.Len()).To(Equal(currSize))

			// Mark the containers as used
			for _, container := range containers {
				container.OnPrewarmedContainerUsed()

				Expect(prewarmer.Len()).To(Equal(currSize))
			}

			By("Returning containers even after some more have been used")

			// Request the containers
			containers = make([]scheduling.PrewarmedContainer, 0, len(hosts))
			for _, host := range hosts {
				container, err := prewarmer.RequestPrewarmedContainer(host)
				Expect(container).ToNot(BeNil())
				Expect(err).To(BeNil())
				Expect(container.Host()).To(Equal(host))

				currSize -= 1
				containers = append(containers, container)
			}

			Expect(prewarmer.Len()).To(Equal(currSize))

			// Mark the containers as used
			for _, container := range containers {
				container.OnPrewarmedContainerUsed()
				Expect(prewarmer.Len()).To(Equal(currSize))
			}

			By("Not returning any more because they've all been used")

			// Request the containers (but there are none left)
			for _, host := range hosts {
				container, err := prewarmer.RequestPrewarmedContainer(host)
				Expect(container).To(BeNil())
				Expect(err).ToNot(BeNil())
				Expect(errors.Is(err, prewarm.ErrNoPrewarmedContainersAvailable)).To(BeTrue())
			}
		})

		Context("Stopping", func() {
			It("Will fail to stop if it is not already running", func() {
				numHosts := 3
				initialCapacity := 1
				maxCapacity := 2

				createAndInitializePrewarmer(initialCapacity, maxCapacity)

				// Set the interval to something small so that we don't have to wait so long.
				prewarmer.(*prewarm.BaseContainerPrewarmer).Config.Interval = time.Millisecond * 25

				By("Correctly provisioning the initial round of prewarm containers")

				hosts, localGatewayClients := createHosts(numHosts, 0, hostSpec, mockCluster, mockCtrl)

				mockCluster.
					EXPECT().
					RangeOverHosts(gomock.Any()).
					Times(1).
					DoAndReturn(func(f func(key string, value scheduling.Host) bool) {
						for idx, host := range hosts {
							connInfo := &proto.KernelConnectionInfo{
								Ip:              fmt.Sprintf("10.0.0.%d", idx+1),
								Transport:       "tcp",
								ControlPort:     9000,
								ShellPort:       9001,
								StdinPort:       9002,
								HbPort:          9003,
								IopubPort:       9004,
								IosubPort:       9005,
								SignatureScheme: jupyter.JupyterSignatureScheme,
								Key:             uuid.NewString(),
							}

							localGatewayClient := localGatewayClients[idx]
							localGatewayClient.
								EXPECT().
								StartKernelReplica(gomock.Any(), gomock.Any(), gomock.Any()).
								Times(1).
								DoAndReturn(func(ctx context.Context, in *proto.KernelReplicaSpec, opts ...grpc.CallOption) (*proto.KernelConnectionInfo, error) {
									time.Sleep(time.Millisecond*5 + (time.Millisecond * time.Duration(rand.Intn(10))))
									return connInfo, nil
								})

							f(host.GetID(), host)
						}
					})

				created, target := prewarmer.ProvisionInitialPrewarmContainers()

				Expect(created).To(Equal(int32(numHosts * initialCapacity)))
				Expect(target).To(Equal(int32(numHosts * initialCapacity)))
				Expect(prewarmer.Len()).To(Equal(numHosts * initialCapacity))

				By("Not being in the 'running' state initially")

				Expect(prewarmer.IsRunning()).To(BeFalse())

				By("Returning an error when instructed to stop while not running")

				err := prewarmer.Stop()
				Expect(err).ToNot(BeNil())
				Expect(errors.Is(err, prewarm.ErrFailedToStop)).To(BeTrue())
				Expect(errors.Is(err, prewarm.ErrNotRunning)).To(BeTrue())
			})

			It("Will stop cleanly and correctly", func() {
				numHosts := 3
				initialCapacity := 1
				maxCapacity := 2

				createAndInitializePrewarmer(initialCapacity, maxCapacity)

				// Set the interval to something small so that we don't have to wait so long.
				prewarmer.(*prewarm.BaseContainerPrewarmer).Config.Interval = time.Millisecond * 25

				By("Correctly provisioning the initial round of prewarm containers")

				hosts, localGatewayClients := createHosts(numHosts, 0, hostSpec, mockCluster, mockCtrl)

				mockCluster.
					EXPECT().
					RangeOverHosts(gomock.Any()).
					Times(1).
					DoAndReturn(func(f func(key string, value scheduling.Host) bool) {
						for idx, host := range hosts {
							connInfo := &proto.KernelConnectionInfo{
								Ip:              fmt.Sprintf("10.0.0.%d", idx+1),
								Transport:       "tcp",
								ControlPort:     9000,
								ShellPort:       9001,
								StdinPort:       9002,
								HbPort:          9003,
								IopubPort:       9004,
								IosubPort:       9005,
								SignatureScheme: jupyter.JupyterSignatureScheme,
								Key:             uuid.NewString(),
							}

							localGatewayClient := localGatewayClients[idx]
							localGatewayClient.
								EXPECT().
								StartKernelReplica(gomock.Any(), gomock.Any(), gomock.Any()).
								Times(1).
								DoAndReturn(func(ctx context.Context, in *proto.KernelReplicaSpec, opts ...grpc.CallOption) (*proto.KernelConnectionInfo, error) {
									time.Sleep(time.Millisecond*5 + (time.Millisecond * time.Duration(rand.Intn(10))))
									return connInfo, nil
								})

							f(host.GetID(), host)
						}
					})

				created, target := prewarmer.ProvisionInitialPrewarmContainers()

				Expect(created).To(Equal(int32(numHosts * initialCapacity)))
				Expect(target).To(Equal(int32(numHosts * initialCapacity)))
				Expect(prewarmer.Len()).To(Equal(numHosts * initialCapacity))

				By("Not being in the 'running' state initially")

				Expect(prewarmer.IsRunning()).To(BeFalse())

				By("Entering the 'running' state when instructed to do so")

				go func() {
					defer GinkgoRecover()
					err := prewarmer.Run()
					Expect(err).To(BeNil())
				}()

				Eventually(prewarmer.IsRunning, time.Millisecond*750, time.Millisecond*125).Should(BeTrue())

				Expect(prewarmer.IsRunning()).To(BeTrue())

				By("Correctly stopping when instructed to do so")

				err := prewarmer.Stop()
				Expect(err).To(BeNil())

				Expect(prewarmer.IsRunning()).To(BeFalse())

				By("Entering the 'running' state when instructed to do so again")

				go func() {
					defer GinkgoRecover()
					err := prewarmer.Run()
					Expect(err).To(BeNil())
				}()

				Eventually(prewarmer.IsRunning, time.Millisecond*750, time.Millisecond*125).Should(BeTrue())

				Expect(prewarmer.IsRunning()).To(BeTrue())

				By("Correctly stopping when instructed to do so again")

				err = prewarmer.Stop()
				Expect(err).To(BeNil())

				Expect(prewarmer.IsRunning()).To(BeFalse())
			})
		})

		Context("Initial Capacity", func() {
			It("Will correctly initialize the pool with 1 pre-warmed container per host", func() {
				numHosts := 3
				initialCapacity := 1
				minCapacity := 0
				maxCapacity := 2

				By("Being created or instantiated correctly")

				createAndInitializePrewarmer(initialCapacity, maxCapacity)

				Expect(prewarmer).ToNot(BeNil())
				_, ok := prewarmer.(*prewarm.BaseContainerPrewarmer)
				Expect(ok).To(BeTrue())

				Expect(prewarmer.InitialPrewarmedContainersPerHost()).To(Equal(initialCapacity))
				Expect(prewarmer.MinPrewarmedContainersPerHost()).To(Equal(minCapacity))
				Expect(prewarmer.MaxPrewarmedContainersPerHost()).To(Equal(maxCapacity))

				Expect(prewarmer.Len()).To(Equal(0))

				By("Correctly provisioning the initial round of prewarm containers")

				hosts, localGatewayClients := createHosts(numHosts, 0, hostSpec, mockCluster, mockCtrl)
				Expect(len(hosts)).To(Equal(numHosts))
				Expect(len(localGatewayClients)).To(Equal(numHosts))

				mockCluster.
					EXPECT().
					RangeOverHosts(gomock.Any()).
					Times(1).
					DoAndReturn(func(f func(key string, value scheduling.Host) bool) {
						for idx, host := range hosts {
							connInfo := &proto.KernelConnectionInfo{
								Ip:              fmt.Sprintf("10.0.0.%d", idx+1),
								Transport:       "tcp",
								ControlPort:     9000,
								ShellPort:       9001,
								StdinPort:       9002,
								HbPort:          9003,
								IopubPort:       9004,
								IosubPort:       9005,
								SignatureScheme: jupyter.JupyterSignatureScheme,
								Key:             uuid.NewString(),
							}

							localGatewayClient := localGatewayClients[idx]
							localGatewayClient.
								EXPECT().
								StartKernelReplica(gomock.Any(), gomock.Any(), gomock.Any()).
								Times(1).
								DoAndReturn(func(ctx context.Context, in *proto.KernelReplicaSpec, opts ...grpc.CallOption) (*proto.KernelConnectionInfo, error) {
									time.Sleep(time.Millisecond*5 + (time.Millisecond * time.Duration(rand.Intn(10))))
									return connInfo, nil
								})

							f(host.GetID(), host)
						}
					})

				created, target := prewarmer.ProvisionInitialPrewarmContainers()

				Expect(created).To(Equal(int32(numHosts * initialCapacity)))
				Expect(target).To(Equal(int32(numHosts * initialCapacity)))

				Expect(prewarmer.Len()).To(Equal(numHosts * initialCapacity))
			})

			It("Will correctly initialize the pool with 3 pre-warmed containers per host", func() {
				numHosts := 3
				initialCapacity := 3
				minCapacity := 0
				maxCapacity := 4

				By("Being created or instantiated correctly")

				createAndInitializePrewarmer(initialCapacity, maxCapacity)

				Expect(prewarmer).ToNot(BeNil())
				_, ok := prewarmer.(*prewarm.BaseContainerPrewarmer)
				Expect(ok).To(BeTrue())

				Expect(prewarmer.InitialPrewarmedContainersPerHost()).To(Equal(initialCapacity))
				Expect(prewarmer.MinPrewarmedContainersPerHost()).To(Equal(minCapacity))
				Expect(prewarmer.MaxPrewarmedContainersPerHost()).To(Equal(maxCapacity))

				Expect(prewarmer.Len()).To(Equal(0))

				hosts, localGatewayClients := createHosts(numHosts, 0, hostSpec, mockCluster, mockCtrl)
				Expect(len(hosts)).To(Equal(numHosts))
				Expect(len(localGatewayClients)).To(Equal(numHosts))

				mockCluster.
					EXPECT().
					RangeOverHosts(gomock.Any()).
					Times(1).
					DoAndReturn(func(f func(key string, value scheduling.Host) bool) {
						for idx, host := range hosts {
							connInfo := &proto.KernelConnectionInfo{
								Ip:              fmt.Sprintf("10.0.0.%d", idx+1),
								Transport:       "tcp",
								ControlPort:     9000,
								ShellPort:       9001,
								StdinPort:       9002,
								HbPort:          9003,
								IopubPort:       9004,
								IosubPort:       9005,
								SignatureScheme: jupyter.JupyterSignatureScheme,
								Key:             uuid.NewString(),
							}

							localGatewayClient := localGatewayClients[idx]
							localGatewayClient.
								EXPECT().
								StartKernelReplica(gomock.Any(), gomock.Any(), gomock.Any()).
								Times(initialCapacity).
								DoAndReturn(func(ctx context.Context, in *proto.KernelReplicaSpec, opts ...grpc.CallOption) (*proto.KernelConnectionInfo, error) {
									GinkgoWriter.Printf("Creating prewarm container on host %s (ID=%s).\n",
										host.GetNodeName(), host.GetID())

									time.Sleep(time.Millisecond*5 + (time.Millisecond * time.Duration(rand.Intn(10))))
									return connInfo, nil
								})

							f(host.GetID(), host)
						}
					})

				created, target := prewarmer.ProvisionInitialPrewarmContainers()

				Expect(created).To(Equal(int32(numHosts * initialCapacity)))
				Expect(target).To(Equal(int32(numHosts * initialCapacity)))

				Expect(prewarmer.Len()).To(Equal(numHosts * initialCapacity))
			})
		})
	})

	Context("E2E Static Scheduling", func() {
		var (
			dockerCluster  scheduling.Cluster
			clusterPlacer  scheduling.Placer
			clusterGateway *daemon.ClusterGatewayImpl
		)

		clusterProvider := func() scheduling.Cluster {
			return dockerCluster
		}

		BeforeEach(func() {
			err := json.Unmarshal([]byte(gatewayStaticConfigJson), &clusterGatewayOptions)
			GinkgoWriter.Printf("Error: %v\n", err)
			Expect(err).To(BeNil())

			clusterGatewayOptions.LoggerOptions = config.LoggerOptions{
				Verbose: debugLoggingEnabled,
				Debug:   debugLoggingEnabled,
			}
			clusterGatewayOptions.Verbose = debugLoggingEnabled
			clusterGatewayOptions.Debug = debugLoggingEnabled

			Expect(clusterGatewayOptions.SchedulerOptions.PrewarmingEnabled).To(BeTrue())
			Expect(clusterGatewayOptions.SchedulerOptions.PrewarmingPolicy).To(Equal(scheduling.NoMaintenance.String()))

			clusterGateway = daemon.New(&clusterGatewayOptions.ConnectionInfo, &clusterGatewayOptions.ClusterDaemonOptions, func(srv daemon.ClusterGateway) {
				globalLogger.Info("Initializing internalCluster Daemon with options: %s", clusterGatewayOptions.ClusterDaemonOptions.String())
				srv.SetClusterOptions(&clusterGatewayOptions.SchedulerOptions)
				srv.SetDistributedClientProvider(&client.DistributedKernelClientProvider{})
			})

			schedulingPolicy, err = scheduler.GetSchedulingPolicy(&clusterGatewayOptions.SchedulerOptions, clusterProvider)
			Expect(err).To(BeNil())
			Expect(schedulingPolicy).ToNot(BeNil())

			clusterPlacer, err = schedulingPolicy.GetNewPlacer(clusterGateway.MetricsProvider)
			Expect(err).To(BeNil())
			Expect(clusterPlacer).ToNot(BeNil())

			dockerCluster, err = cluster.NewBuilder(cluster.DockerCompose).
				WithKubeClient(nil).
				WithHostSpec(hostSpec).
				WithPlacer(clusterPlacer).
				WithSchedulingPolicy(schedulingPolicy).
				WithHostMapper(clusterGateway).
				WithKernelProvider(clusterGateway).
				WithClusterMetricsProvider(clusterGateway.MetricsProvider).
				WithNotificationBroker(clusterGateway).
				WithStatisticsUpdateProvider(clusterGateway.UpdateClusterStatistics).
				WithOptions(&clusterGatewayOptions.SchedulerOptions).
				BuildCluster()

			Expect(err).To(BeNil())
			Expect(dockerCluster).ToNot(BeNil())

			prewarmer = clusterGateway.Scheduler().ContainerPrewarmer()
			Expect(prewarmer).ToNot(BeNil())

			_, ok := prewarmer.(*prewarm.BaseContainerPrewarmer)
			Expect(ok).To(BeTrue())
		})

		It("Will correctly maintain the minimum capacity", func() {

		})
	})
})
