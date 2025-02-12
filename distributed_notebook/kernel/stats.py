class ExecutionStats(object):
    """
    Encapsulates some metrics related to code execution and whatnot.
    """

    def __init__(
            self,
            cuda_init_microseconds: float = 0,
            download_runtime_dependencies_microseconds: float = 0,
            download_model_microseconds: float = 0,
            download_training_data_microseconds: float = 0,
            upload_model_and_training_data_microseconds: float = 0,
            execution_time_microseconds: float = 0,
            leader_election_microseconds: float = 0,
            copy_data_from_cpu_to_gpu_microseconds: float = 0,
            copy_data_from_gpu_to_cpu_microseconds: float = 0,
            replay_time_microseconds: float = 0,
            execution_start_unix_millis: float = 0,
            execution_end_unix_millis: float = 0,
            sync_start_unix_millis: float = 0,
            sync_end_unix_millis: float = 0,
            download_training_data_start_unix_millis: float = 0,
            download_training_data_end_unix_millis: float = 0,
            download_model_start_unix_millis: float = 0,
            download_model_end_unix_millis: float = 0,
            upload_model_start_unix_millis: float = 0,
            upload_model_end_unix_millis: float = 0,
            tokenize_dataset_microseconds: float = 0,
            tokenize_training_data_start_unix_millis: float = 0,
            tokenize_training_data_end_unix_millis: float = 0,
            won_election: bool = False,  # always true for non-static/non-dynamic scheduling policies
    ):
        """
        Order:
        - init cuda
        - download dependencies, dataset, model/model parameters, etc.
        - leader election
        - copy data from cpu to gpu
        - code execution
        - copy data from gpu to cpu
        - synchronization
            - remote remote_storage writes
        """
        self.cuda_init_microseconds: float = cuda_init_microseconds
        self.download_runtime_dependencies_microseconds: float = download_runtime_dependencies_microseconds
        self.download_model_microseconds: float = download_model_microseconds
        self.download_training_data_microseconds: float = download_training_data_microseconds
        self.tokenize_dataset_microseconds: float = tokenize_dataset_microseconds
        self.upload_model_and_training_data_microseconds: float = upload_model_and_training_data_microseconds
        self.execution_start_unix_millis: float = execution_start_unix_millis
        self.execution_end_unix_millis: float = execution_end_unix_millis
        self.execution_time_microseconds: float = execution_time_microseconds
        self.replay_time_microseconds: float = replay_time_microseconds
        self.leader_election_microseconds: float = leader_election_microseconds
        self.copy_data_from_cpu_to_gpu_microseconds: float = copy_data_from_cpu_to_gpu_microseconds
        self.copy_data_from_gpu_to_cpu_microseconds: float = copy_data_from_gpu_to_cpu_microseconds
        self.won_election: bool = won_election
        self.sync_start_unix_millis: float = sync_start_unix_millis
        self.sync_end_unix_millis: float = sync_end_unix_millis
        self.sync_duration_millis: float = sync_end_unix_millis - sync_start_unix_millis

        self.download_training_data_start_unix_millis: float = download_training_data_start_unix_millis
        self.download_training_data_end_unix_millis: float = download_training_data_end_unix_millis

        self.tokenize_training_data_start_unix_millis: float = tokenize_training_data_start_unix_millis
        self.tokenize_training_data_end_unix_millis: float = tokenize_training_data_end_unix_millis

        self.download_model_start_unix_millis: float = download_model_start_unix_millis
        self.download_model_end_unix_millis: float = download_model_end_unix_millis

        self.upload_model_start_unix_millis: float = upload_model_start_unix_millis
        self.upload_model_end_unix_millis: float = upload_model_end_unix_millis