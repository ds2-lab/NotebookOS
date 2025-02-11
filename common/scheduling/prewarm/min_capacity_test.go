package prewarm_test

import (
	"encoding/json"
	"fmt"
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
	"math/rand"
	"sync"
	"time"
)

var _ = Describe("MinCapacity Prewarmer Tests", func() {
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
	createAndInitializePrewarmer := func(initSize, minSize, maxSize int) {
		if maxSize < minSize {
			panic("Max size is less than min size.")
		}

		prewarmerConfig := &prewarm.MinCapacityPrewarmerConfig{
			PrewarmerConfig:               prewarm.NewPrewarmerConfig(initSize, maxSize, 0 /* Default of 5sec will be used */),
			MinPrewarmedContainersPerHost: minSize,
		}

		prewarmer = prewarm.NewMinCapacityPrewarmer(mockCluster, prewarmerConfig, mockMetricsProvider)
	}

	Context("Unit Tests", func() {

		BeforeEach(func() {
			mockCtrl = gomock.NewController(GinkgoT())

			err := json.Unmarshal([]byte(gatewayStaticConfigJson), &clusterGatewayOptions)
			GinkgoWriter.Printf("Error: %v\n", err)
			Expect(err).To(BeNil())

			Expect(clusterGatewayOptions.SchedulerOptions.PrewarmingEnabled).To(BeTrue())
			Expect(clusterGatewayOptions.SchedulerOptions.PrewarmingPolicy).To(Equal(scheduling.MaintainMinCapacity.String()))

			schedulingPolicy, err = policy.NewStaticPolicy(&clusterGatewayOptions.SchedulerOptions)
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
			minCapacity := 1
			maxCapacity := 2

			By("Being created or instantiated correctly")

			createAndInitializePrewarmer(initialCapacity, minCapacity, maxCapacity)

			Expect(prewarmer).ToNot(BeNil())
			_, ok := prewarmer.(*prewarm.MinCapacityPrewarmer)
			Expect(ok).To(BeTrue())

			Expect(prewarmer.InitialPrewarmedContainersPerHost()).To(Equal(initialCapacity))
			Expect(prewarmer.MinPrewarmedContainersPerHost()).To(Equal(minCapacity))
			Expect(prewarmer.MaxPrewarmedContainersPerHost()).To(Equal(maxCapacity))

			Expect(prewarmer.Len()).To(Equal(0))

			Expect(prewarmer).ToNot(BeNil())
			_, ok = prewarmer.(*prewarm.MinCapacityPrewarmer)
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
			minCapacity := 1
			maxCapacity := 2

			By("Being created or instantiated correctly")

			createAndInitializePrewarmer(initialCapacity, minCapacity, maxCapacity)

			Expect(prewarmer).ToNot(BeNil())
			_, ok := prewarmer.(*prewarm.MinCapacityPrewarmer)
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
								GinkgoWriter.Printf("Creating prewarm container on host %s (ID=%s).\n",
									host.GetNodeName(), host.GetID())

								startReplicaCalledWg.Done()

								blockStartReplicaWg.Wait()

								time.Sleep(time.Millisecond*5 + time.Duration(rand.Intn(10)))
								return connInfo, nil
							})

						f(host.GetID(), host)
					}
				})

			createdChan, targetChan := make(chan int32, 1), make(chan int32, 1)

			By("Returning the correct size values before provisioning any containers")

			Expect(prewarmer.Len()).To(Equal(0))

			for _, host := range hosts {
				curr, provisioning := prewarmer.HostLen(host)
				Expect(curr).To(Equal(0))
				Expect(provisioning).To(Equal(0))
			}

			go func() {
				created, target := prewarmer.ProvisionInitialPrewarmContainers()

				createdChan <- created
				targetChan <- target
			}()

			By("Returning the correct size values while provisioning a container")

			startReplicaCalledWg.Wait()

			Expect(prewarmer.Len()).To(Equal(0))

			for _, host := range hosts {
				curr, provisioning := prewarmer.HostLen(host)
				Expect(curr).To(Equal(0))
				Expect(provisioning).To(Equal(1))
			}

			time.Sleep(time.Millisecond * 250)

			Expect(prewarmer.Len()).To(Equal(0))

			for _, host := range hosts {
				curr, provisioning := prewarmer.HostLen(host)
				Expect(curr).To(Equal(0))
				Expect(provisioning).To(Equal(1))
			}

			blockStartReplicaWg.Done()

			Eventually(func() bool {
				if prewarmer.Len() != (numHosts * initialCapacity) {
					return false
				}

				for _, host := range hosts {
					curr, provisioning := prewarmer.HostLen(host)
					if curr != (numHosts * initialCapacity) {
						return false
					}

					if provisioning != 0 {
						return false
					}
				}

				return true
			}, time.Second*5, time.Millisecond*100).Should(BeTrue())

			created := <-createdChan
			target := <-targetChan

			Expect(created).To(Equal(int32(numHosts * initialCapacity)))
			Expect(target).To(Equal(int32(numHosts * initialCapacity)))

			Expect(prewarmer.Len()).To(Equal(numHosts * initialCapacity))
		})

		It("Will correctly maintain the size of the warm container pool", func() {
			numHosts := 2
			initialCapacity := 1
			minCapacity := 2
			maxCapacity := 4

			mockCluster.EXPECT().Len().AnyTimes().Return(numHosts)
			createAndInitializePrewarmer(initialCapacity, minCapacity, maxCapacity)

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
								time.Sleep(time.Millisecond*5 + time.Duration(rand.Intn(10)))
								return connInfo, nil
							})

						f(host.GetID(), host)
					}
				})

			created, target := prewarmer.ProvisionInitialPrewarmContainers()

			Expect(created).To(Equal(int32(numHosts * initialCapacity)))
			Expect(target).To(Equal(int32(numHosts * initialCapacity)))

			Expect(prewarmer.Len()).To(Equal(initialCapacity * numHosts))

			By("Provisioning additional pre-warm containers according to the MinCapacity policy")

			minCapacityPrewarmer, ok := prewarmer.(*prewarm.MinCapacityPrewarmer)
			Expect(ok).To(BeTrue())
			Expect(minCapacityPrewarmer).ToNot(BeNil())

			guardChan := make(chan struct{})
			minCapacityPrewarmer.GuardChannel = guardChan

			var preRunWg, postRunWg sync.WaitGroup

			// Set up mocked calls for next iteration of the Run method.
			prepareNextRunIter := func() {
				postRunWg.Add(numHosts)
				preRunWg.Add(numHosts)

				mockCluster.
					EXPECT().
					RangeOverHosts(gomock.Any()).
					Times(1).
					DoAndReturn(func(f func(key string, value scheduling.Host) bool) {
						for _, host := range hosts {
							preRunWg.Done()
							f(host.GetID(), host)
							postRunWg.Done()
						}
					})
			}

			// Used to block the calls to StartKernelReplica until we allow them through.
			var startKernelWg sync.WaitGroup

			prepareNextRunIter()
			startKernelWg.Add(1)

			// Prepare calls.
			for i := 0; i < numHosts; i++ {
				localGatewayClients[i].
					EXPECT().
					StartKernelReplica(gomock.Any(), gomock.Any(), gomock.Any()).
					Times(1).
					DoAndReturn(
						func(ctx context.Context, in *proto.KernelReplicaSpec, opts ...grpc.CallOption) (*proto.KernelConnectionInfo, error) {
							time.Sleep(time.Millisecond*5 + time.Duration(rand.Intn(10)))
							n := 128

							startKernelWg.Wait()

							return &proto.KernelConnectionInfo{
								Ip:              fmt.Sprintf("10.%d.%d.%d", i, rand.Intn(n), rand.Intn(n)),
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
						})
			}

			go func() {
				defer GinkgoRecover()
				err := prewarmer.Run()
				Expect(err).To(BeNil())
			}()

			// Wait for prewarmer to begin running.
			Eventually(prewarmer.IsRunning, time.Millisecond*750, time.Millisecond*125).Should(BeTrue())

			// Wait for prewarmer to call StartKernelReplica on each prov.
			preRunWg.Wait()

			// This should occur immediately, essentially.
			Eventually(func() bool {
				return prewarmer.TotalNumProvisioning() > 0
			}, time.Millisecond*750, time.Millisecond*250).Should(BeTrue())

			// This should occur immediately, essentially.
			Eventually(func() bool {
				for i := 0; i < numHosts; i++ {
					curr, prov := prewarmer.HostLen(hosts[i])

					if curr != 1 {
						return false
					}

					if prov != 1 {
						return false
					}
				}

				return true
			}, time.Millisecond*500, time.Millisecond*125).Should(BeTrue())

			// Let the calls to StartKernelReplica through.
			startKernelWg.Done()

			// Wait for calls to StartKernelReplica and whatnot to finish.
			postRunWg.Wait()

			// This should occur more or less immediately.
			Eventually(func() bool {
				return prewarmer.Len() == 4
			}, time.Millisecond*750, time.Millisecond*125).Should(BeTrue())

			// Done provisioning.
			for _, host := range hosts {
				curr, prov := prewarmer.HostLen(host)
				Expect(curr).To(Equal(2))
				Expect(prov).To(Equal(0))
			}

			//////////////////////////////////////////////
			//////////////////////////////////////////////
			// Request 1 pre-warm container from Host 1 //
			//////////////////////////////////////////////
			//////////////////////////////////////////////

			// Request container from Host #1.
			container, err := prewarmer.RequestPrewarmedContainer(hosts[1])
			Expect(err).To(BeNil())
			Expect(container).ToNot(BeNil())
			Expect(container.Host()).To(Equal(hosts[1]))

			container.OnPrewarmedContainerUsed()
			Expect(prewarmer.Len()).To(Equal(3))
			curr, prov := prewarmer.HostLen(hosts[1])
			Expect(prov).To(Equal(0))
			Expect(curr).To(Equal(1))

			prepareNextRunIter()
			startKernelWg.Add(1)
			localGatewayClients[1].
				EXPECT().
				StartKernelReplica(gomock.Any(), gomock.Any(), gomock.Any()).
				Times(1).
				DoAndReturn(
					func(ctx context.Context, in *proto.KernelReplicaSpec, opts ...grpc.CallOption) (*proto.KernelConnectionInfo, error) {
						time.Sleep(time.Millisecond*5 + time.Duration(rand.Intn(10)))
						n := 128

						startKernelWg.Wait()

						return &proto.KernelConnectionInfo{
							Ip:              fmt.Sprintf("10.1.%d.%d", rand.Intn(n), rand.Intn(n)),
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
					})

			guardChan <- struct{}{}

			preRunWg.Wait()

			// This should occur immediately, essentially.
			Eventually(func() bool {
				return prewarmer.TotalNumProvisioning() > 0
			}, time.Millisecond*750, time.Millisecond*250).Should(BeTrue())

			// This should occur immediately, essentially.
			Eventually(func() bool {
				curr, prov := prewarmer.HostLen(hosts[1])

				if curr != 1 {
					return false
				}

				if prov != 1 {
					return false
				}

				return true
			}, time.Millisecond*500, time.Millisecond*125).Should(BeTrue())

			// Let the calls to StartKernelReplica through.
			startKernelWg.Done()

			// Wait for calls to StartKernelReplica and whatnot to finish.
			postRunWg.Wait()

			// This should occur more or less immediately.
			Eventually(func() bool {
				return prewarmer.Len() == 4
			}, time.Millisecond*750, time.Millisecond*125).Should(BeTrue())

			// Done provisioning.
			for _, host := range hosts {
				curr, prov := prewarmer.HostLen(host)
				Expect(curr).To(Equal(2))
				Expect(prov).To(Equal(0))
			}

			///////////////////////////////////////////////
			///////////////////////////////////////////////
			// Request 2 pre-warm containers from Host 0 //
			///////////////////////////////////////////////
			///////////////////////////////////////////////

			// Request 2 containers from Host #1.
			for i := 0; i < 2; i++ {
				container, err = prewarmer.RequestPrewarmedContainer(hosts[0])
				Expect(err).To(BeNil())
				Expect(container).ToNot(BeNil())
				Expect(container.Host()).To(Equal(hosts[0]))

				container.OnPrewarmedContainerUsed()
				Expect(prewarmer.Len()).To(Equal(4 - (i + 1)))
				curr, prov = prewarmer.HostLen(hosts[0])
				Expect(prov).To(Equal(0))
				Expect(curr).To(Equal(2 - (i + 1)))
			}

			prepareNextRunIter()
			startKernelWg.Add(1)
			localGatewayClients[0].
				EXPECT().
				StartKernelReplica(gomock.Any(), gomock.Any(), gomock.Any()).
				Times(2).
				DoAndReturn(
					func(ctx context.Context, in *proto.KernelReplicaSpec, opts ...grpc.CallOption) (*proto.KernelConnectionInfo, error) {
						time.Sleep(time.Millisecond*5 + time.Duration(rand.Intn(10)))
						n := 128

						startKernelWg.Wait()

						return &proto.KernelConnectionInfo{
							Ip:              fmt.Sprintf("10.0.%d.%d", rand.Intn(n), rand.Intn(n)),
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
					})

			guardChan <- struct{}{}

			preRunWg.Wait()

			// This should occur immediately, essentially.
			Eventually(func() bool {
				return prewarmer.TotalNumProvisioning() == 2
			}, time.Millisecond*750, time.Millisecond*250).Should(BeTrue())

			// This should occur immediately, essentially.
			Eventually(func() bool {
				curr, prov := prewarmer.HostLen(hosts[0])

				if curr != 0 {
					return false
				}

				if prov != 2 {
					return false
				}

				return true
			}, time.Millisecond*500, time.Millisecond*125).Should(BeTrue())

			// Let the calls to StartKernelReplica through.
			startKernelWg.Done()

			// Wait for calls to StartKernelReplica and whatnot to finish.
			postRunWg.Wait()

			// This should occur more or less immediately.
			Eventually(func() bool {
				return prewarmer.Len() == 4
			}, time.Millisecond*750, time.Millisecond*125).Should(BeTrue())

			// Done provisioning.
			for _, host := range hosts {
				curr, prov := prewarmer.HostLen(host)
				Expect(curr).To(Equal(2))
				Expect(prov).To(Equal(0))
			}
		})

		Context("Initial Capacity", func() {
			It("Will correctly initialize the pool with 1 pre-warmed container per host", func() {
				numHosts := 3
				initialCapacity := 1
				minCapacity := 1
				maxCapacity := 2

				By("Being created or instantiated correctly")

				createAndInitializePrewarmer(initialCapacity, minCapacity, maxCapacity)

				Expect(prewarmer).ToNot(BeNil())
				_, ok := prewarmer.(*prewarm.MinCapacityPrewarmer)
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
									time.Sleep(time.Millisecond*5 + time.Duration(rand.Intn(10)))
									return connInfo, nil
								})

							f(host.GetID(), host)
						}
					})

				created, target := prewarmer.ProvisionInitialPrewarmContainers()

				Expect(created).To(Equal(int32(numHosts * initialCapacity)))
				Expect(target).To(Equal(int32(numHosts * initialCapacity)))

				Expect(prewarmer.Len()).To(Equal(initialCapacity * numHosts))
			})

			It("Will correctly initialize the pool with 3 pre-warmed containers per host", func() {
				numHosts := 3
				initialCapacity := 3
				minCapacity := 1
				maxCapacity := 4

				By("Being created or instantiated correctly")

				createAndInitializePrewarmer(initialCapacity, minCapacity, maxCapacity)

				Expect(prewarmer).ToNot(BeNil())
				_, ok := prewarmer.(*prewarm.MinCapacityPrewarmer)
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

									time.Sleep(time.Millisecond*5 + time.Duration(rand.Intn(10)))
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
			Expect(clusterGatewayOptions.SchedulerOptions.PrewarmingPolicy).To(Equal(scheduling.MaintainMinCapacity.String()))

			clusterGateway = daemon.New(&clusterGatewayOptions.ConnectionInfo, &clusterGatewayOptions.ClusterDaemonOptions, func(srv daemon.ClusterGateway) {
				globalLogger.Info("Initializing internalCluster Daemon with options: %s", clusterGatewayOptions.ClusterDaemonOptions.String())
				srv.SetClusterOptions(&clusterGatewayOptions.SchedulerOptions)
				srv.SetDistributedClientProvider(&client.DistributedKernelClientProvider{})
			})

			schedulingPolicy, err = scheduler.GetSchedulingPolicy(&clusterGatewayOptions.SchedulerOptions)
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

			_, ok := prewarmer.(*prewarm.MinCapacityPrewarmer)
			Expect(ok).To(BeTrue())
		})

		It("Will correctly maintain the minimum capacity", func() {

		})
	})
})
