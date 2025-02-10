package prewarm_test

import (
	"encoding/json"
	"github.com/Scusemua/go-utils/config"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/client"
	"github.com/scusemua/distributed-notebook/common/scheduling/cluster"
	"github.com/scusemua/distributed-notebook/common/scheduling/prewarm"
	"github.com/scusemua/distributed-notebook/common/scheduling/scheduler"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/gateway/daemon"
	"github.com/scusemua/distributed-notebook/gateway/domain"
)

var _ = Describe("MinCapacity Prewarmer Tests", func() {
	Context("Static Scheduling", func() {
		var (
			prewarmer             scheduling.ContainerPrewarmer
			dockerCluster         scheduling.Cluster
			clusterPlacer         scheduling.Placer
			schedulingPolicy      scheduling.Policy
			clusterGateway        *daemon.ClusterGatewayImpl
			clusterGatewayOptions *domain.ClusterGatewayOptions
		)

		gatewayConfigJson := `{"jaeger_addr":"","consul_addr":"","connection_info":{"ip":"","transport":"tcp","signature_scheme":"","key":"","control_port":17201,"shell_port":17202,"stdin_port":17203,"hb_port":17200,"iopub_port":17204,"iosub_port":17205,"ack_port":17206,"starting_resource_port":17207,"num_resource_ports":14096},"cluster_daemon_options":{"local-daemon-service-name":"local-daemon-network","global-daemon-service-name":"daemon-network","kubernetes-namespace":"","notebook-image-name":"scusemua/jupyter-cpu:latest","notebook-image-tag":"latest","cluster_scheduler_options":{"common_options":{"deployment_mode":"docker-compose","docker_app_name":"","docker_network_name":"distributed_cluster_default","scheduling-policy":"static","idle-session-reclamation-policy":"none","remote-storage-endpoint":"redis:6379","remote-storage":"redis","gpus-per-host":8,"prometheus_interval":15,"prometheus_port":-1,"num_resend_attempts":1,"smr-port":17080,"debug_port":19996,"election_timeout_seconds":3,"local_mode":true,"use_real_gpus":false,"acks_enabled":false,"debug_mode":true,"simulate_checkpointing_latency":true,"disable_prometheus_metrics_publishing":false,"simulate_training_using_sleep":false,"bind_debugpy_port":false,"save_stopped_kernel_containers":false},"custom_idle_session_reclamation_options":{"idle_session_replay_all_cells":false,"idle_session_timeout_interval_sec":0},"subscribed-ratio-update-interval":0,"scaling-factor":1.1,"scaling-interval":15,"scaling-limit":1.15,"scaling-in-limit":2,"scaling-buffer-size":3,"min_cluster_nodes":6,"max_cluster_nodes":48,"gpu_poll_interval":5,"max-subscribed-ratio":7,"execution-time-sampling-window":10,"migration-time-sampling-window":10,"scheduler-http-port":18078,"mean_scale_out_per_host_sec":15,"std_dev_scale_out_per_host_sec":2,"mean_scale_in_per_host_sec":10,"std_dev_scale_in_per_host_sec":1,"millicpus_per_host":0,"memory_mb_per_host":0,"vram_gb_per_host":0,"predictive_autoscaling":false,"assign_kernel_debug_ports":false},"local-daemon-service-port":18075,"global-daemon-service-port":0,"distributed-cluster-service-port":18079,"remote-docker-event-aggregator-port":15821,"initial-cluster-size":12,"initial-connection-period":60,"idle_session_reclamation_interval_sec":30,"submit_execute_requests_one_at_a_time":true,"use-stateful-set":false,"idle_session_reclamation_enabled":true,"prewarming_enabled":true,"initial_num_containers_per_host":1,"min_prewarm_containers_per_host":1,"max_prewarm_containers_per_host":3,"prewarming_policy":"maintain_minimum_capacity"},"port":18080,"provisioner_port":18081,"pretty_print_options":true}`

		hostSpec := types.NewDecimalSpec(128000, 256000, 8, 40)

		BeforeEach(func() {
			var err error

			err = json.Unmarshal([]byte(gatewayConfigJson), &clusterGatewayOptions)
			GinkgoWriter.Printf("Error: %v\n", err)
			Expect(err).To(BeNil())

			clusterGatewayOptions.LoggerOptions = config.LoggerOptions{
				Verbose: debugLoggingEnabled,
				Debug:   debugLoggingEnabled,
			}
			clusterGatewayOptions.Verbose = debugLoggingEnabled
			clusterGatewayOptions.Debug = debugLoggingEnabled

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
