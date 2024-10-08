import time

class SimulatedCheckpointer(object):
    def __init__(
            self,
            name: str = "",
            download_rate: float = 15e9,
            upload_rate: float = 1e9,
            download_variance_percent: float = 0.05,
            upload_variance_percent: float = 0.05,
            failure_chance_percentage: float = 0.0
    ):
        """
        Create a new SimulatedCheckpointer object.

        Args:
            name: Name of the storage service being simulated.
            download_rate: Download rate in Gb/sec.
            upload_rate: Upload rate in Gb/sec.
            download_variance_percent: The maximum amount by which the download rate can vary/deviate from its set value during a simulated I/O operation.
            upload_variance_percent: The maximum amount by which the upload rate can vary/deviate from its set value during a simulated I/O operation.
            failure_chance_percentage: The likelihood that an error (which results in the failure of a single I/O operation) occurs during any single I/O operation.
        """
        # Name of the storage service being simulated.
        self.name: str = name

        # Download rate in Gb/sec.
        self.download_rate:float = download_rate

        # Upload rate in Gb/sec.
        self.upload_rate:float = upload_rate

        # The maximum amount by which the download rate can vary/deviate
        # from its set value during a simulated I/O operation.
        self.download_variance_percent: float = download_variance_percent

        # The maximum amount by which the upload rate can vary/deviate
        # from its set value during a simulated I/O operation.
        self.upload_variance_percent: float = upload_variance_percent

        # The likelihood that an error (which results in the failure of a single I/O operation)
        # occurs during any single I/O operation.
        self.failure_chance_percentage: float = failure_chance_percentage

        self.total_num_io_ops: int = 0
        self.total_num_read_ops: int = 0
        self.total_num_write_ops: int = 0

        self.num_successful_read_ops: int = 0
        self.num_successful_write_ops: int = 0

        self.total_num_failures: int = 0
        self.num_failed_read_ops: int = 0
        self.num_failed_write_ops: int = 0

        self.read_latencies: list[float] = []
        self.write_latencies: list[float] = []

    async def async_upload_data(self, size_bytes: float = 0):
        """
        Asynchronously simulate a write I/O operation.

        Args:
            size_bytes: the size of the data to be written in bytes.
        """
        start_time: float = time.time()

        stop_time: float = time.time()
        latency_ms: float = (stop_time - start_time) * 1.0e3

        self.total_num_io_ops += 1
        self.total_num_write_ops += 1
        self.num_successful_write_ops += 1
        self.write_latencies.append(latency_ms)

    async def async_download_data(self, size_bytes: float = 0):
        """
        Asynchronously simulate a write I/O operation.

        Args:
            size_bytes: the size of the data to be written in bytes.
        """
        start_time: float = time.time()

        stop_time: float = time.time()
        latency_ms: float = (stop_time - start_time) * 1.0e3

        self.total_num_io_ops += 1
        self.total_num_read_ops += 1
        self.num_successful_read_ops += 1
        self.read_latencies.append(latency_ms)

    def upload_data(self, size_bytes: float = 0):
        """
        Simulate a write I/O operation.

        Args:
            size_bytes: the size of the data to be written in bytes.
        """
        start_time: float = time.time()

        stop_time: float = time.time()
        latency_ms: float = (stop_time - start_time) * 1.0e3

        self.total_num_io_ops += 1
        self.total_num_write_ops += 1
        self.num_successful_write_ops += 1
        self.write_latencies.append(latency_ms)

    def download_data(self, size_bytes: float = 0):
        """
        Simulate a write I/O operation.

        Args:
            size_bytes: the size of the data to be written in bytes.
        """
        start_time: float = time.time()

        stop_time: float = time.time()
        latency_ms: float = (stop_time - start_time) * 1.0e3

        self.total_num_io_ops += 1
        self.total_num_read_ops += 1
        self.num_successful_read_ops += 1
        self.read_latencies.append(latency_ms)

    read_data = download_data # alias for download_data
    write_data = upload_data # alias for upload_data
    async_read_data = async_download_data # alias for async_download_data
    async_write_data = async_upload_data # alias for async_upload_data