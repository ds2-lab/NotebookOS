import logging
import random
import time
import math

from ...logging.color_formatter import ColoredLogFormatter

def get_new_rate(base_rate: int, percent_deviation: float)->int:
    """
    Compute and return a new upload or download rate calculated using the base upload/download rate and
    the amount by which the rate may vary as a percentage of the base rate.

    Args:
        base_rate: the base upload or download rate in bytes/second.
        percent_deviation: the amount by which the rate may vary as a percentage of the base rate.

    Returns:
        A new upload or download rate calculated using the base upload/download rate and the amount
        by which the rate may vary as a percentage of the base rate.
    """
    if percent_deviation < 0:
        raise ValueError(f"Invalid % deviation specified: {percent_deviation}. Value must be >= 0.")

    if percent_deviation == 0:
        return base_rate

    maximum_rate_adjustment: int = math.floor(percent_deviation * base_rate)
    rate_adjustment: int = random.randint(0, maximum_rate_adjustment)

    # 50/50 that the rate is increased or decreased.
    if random.random() > 0.5:
        return base_rate + rate_adjustment
    else:
        return base_rate - rate_adjustment

class SimulatedCheckpointer(object):
    def __init__(
            self,
            name: str = "",
            download_rate: int = 200_000_000, # 200MB/sec
            upload_rate: int = 1_000_000, # 1MB/sec
            download_variance_percent: float = 0.05,
            upload_variance_percent: float = 0.05,
            read_failure_chance_percentage: float = 0.0,
            write_failure_chance_percentage: float = 0.0
    ):
        """
        Create a new SimulatedCheckpointer object.

        Args:
            name: Name of the storage service being simulated.
            download_rate: Download rate in bytes/sec.
            upload_rate: Upload rate in bytes/sec.
            download_variance_percent: The maximum amount by which the download rate can vary/deviate from its set value during a simulated I/O operation.
            upload_variance_percent: The maximum amount by which the upload rate can vary/deviate from its set value during a simulated I/O operation.
            read_failure_chance_percentage: The likelihood as a percentage (value between 0 and 1) that an error occurs during any single read operation.
            write_failure_chance_percentage: The likelihood as a percentage (value between 0 and 1) that an error occurs during any single write operation.
        """
        if download_rate < 0:
            raise ValueError(f"Invalid download rate specified: {download_rate} bytes/sec. Value must be positive.")

        if upload_rate < 0:
            raise ValueError(f"Invalid upload rate specified: {upload_rate} bytes/sec. Value must be positive.")

        if download_variance_percent < 0:
            raise ValueError(
                f"Invalid download variance percentage specified: {download_variance_percent}. Value must be positive.")

        if upload_variance_percent < 0:
            raise ValueError(
                f"Invalid upload variance percentage specified: {upload_variance_percent}. Value must be positive.")

        if read_failure_chance_percentage < 0 or read_failure_chance_percentage > 1:
            raise ValueError(
                f"Invalid read failure percentage specified: {read_failure_chance_percentage}. Value must be positive and between 0 and 1.")

        if write_failure_chance_percentage < 0 or write_failure_chance_percentage > 1:
            raise ValueError(
                f"Invalid write failure percentage specified: {write_failure_chance_percentage}. Value must be positive and between 0 and 1.")

        # Name of the storage service being simulated.
        self.name: str = name

        # Download rate in bytes/sec.
        self.download_rate: float = download_rate

        # Upload rate in bytes/sec.
        self.upload_rate: int = upload_rate

        # The maximum amount by which the download rate can vary/deviate
        # from its set value during a simulated I/O operation.
        self.download_variance_percent: float = download_variance_percent

        # The maximum amount by which the upload rate can vary/deviate
        # from its set value during a simulated I/O operation.
        self.upload_variance_percent: float = upload_variance_percent

        # The likelihood as a percentage (value between 0 and 1) that an error occurs during any single read operation.
        self.read_failure_chance_percentage: float = read_failure_chance_percentage

        # The likelihood as a percentage (value between 0 and 1) that an error occurs during any single write operation.
        self.write_failure_chance_percentage: float = write_failure_chance_percentage

        self.logger: logging.Logger = logging.getLogger(f"SimulatedCheckpointer[{self.name}] ")
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False
        ch = logging.StreamHandler()
        ch.setFormatter(ColoredLogFormatter())
        self.logger.addHandler(ch)

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
alc

        Raises a TimeoutError if the simulated I/O operation fails.
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

        Raises a TimeoutError if the simulated I/O operation fails.
        """
        start_time: float = time.time()

        stop_time: float = time.time()
        latency_ms: float = (stop_time - start_time) * 1.0e3

        self.total_num_io_ops += 1
        self.total_num_read_ops += 1
        self.num_successful_read_ops += 1
        self.read_latencies.append(latency_ms)

    def __simulate_network_io_operation(
            self,
            size_bytes: int = 0,
            base_rate: int = 1_000_000,
            rate_variance_percent: float = 0.0,
            failure_percentage_chance: float = 0.0,
            operation_name: str = ""
    )->tuple[bool, float]:
        """
        Simulate a network read/write operation.

        Args:
            size_bytes: the size of the data to be read/written.
            base_rate: the base upload/download rate.
            rate_variance_percent: the amount by which the transfer rate can vary as a percentage of the base rate.
            failure_percentage_chance: the likelihood that this operation should fail.
            operation_name: "upload" or "download", depending on the type of operation that is being simulated.

        Returns:
            a tuple of bool, float where the first element is a flag indicating whether the simulated I/O operation
            was successful (True) or failed (False), and the second element is the amount of time that the operation
            was estimated to take at the beginning, based on its initial transfer rate.
        """
        current_rate: int = get_new_rate(base_rate, rate_variance_percent)
        initial_estimated_time_required_ms: float = (size_bytes / current_rate) * 1.0e3
        estimated_time_required_ms: float = initial_estimated_time_required_ms

        if size_bytes == 0:
            roll: float = random.random()
            failure_occurred: bool = roll < failure_percentage_chance

            if failure_occurred:
                return False, 0 # Size of 0 bytes, so instantaneous.

            return True, 0 # Size of 0 bytes, so instantaneous.

        if size_bytes > 1.0e6:
            transfer_size: float = round(size_bytes / 1.0e6, 3)
            rate_formatted: float = round(current_rate / 1.0e6, 3)
            transfer_units = "MB"
        else:
            transfer_size: int = size_bytes
            rate_formatted: float = current_rate
            transfer_units = "bytes"

        self.logger.info(f"Simulated network {operation_name} of size {transfer_size} {transfer_units} "
                          f"targeting {self.name} is estimated to take {round(estimated_time_required_ms, 2)} ms to "
                          f"complete at current data transfer rate of {rate_formatted} {transfer_units}/sec.")

        # If our transfer rate is permitted to vary, then we'll update the rate every second.
        # If the estimated transfer time is less than one second, then the rate will not vary.
        if rate_variance_percent > 0 and estimated_time_required_ms > 1.0e3:
            bytes_remaining: int = size_bytes
            sleep_interval_sec: float = 1

            # Keep sleeping in ~1sec increments until we've transferred everything.
            while bytes_remaining > 0:
                time.sleep(sleep_interval_sec)

                # Since current_rate is bytes/second, and we just simulated I/O for 1 second,
                # the number of bytes transferred is just equal to current_rate.
                bytes_remaining -= current_rate

                # If we've transferred everything, then we're done and can exit the loop.
                if bytes_remaining <= 0:
                    break

                current_rate = get_new_rate(base_rate, rate_variance_percent)

                if current_rate > base_rate:
                    rate_difference: float = round((current_rate - base_rate) / 1.0e6, 3)
                    self.logger.debug(f"Got new {operation_name} rate: "
                                      f"{current_rate/1.0e6} MB/sec (+{rate_difference} MB/sec).")
                else:
                    rate_difference: float = round((base_rate - current_rate) / 1.0e6, 3)
                    self.logger.debug(f"Got new {operation_name} rate: "
                                      f"{current_rate/1.0e6} MB/sec (-{rate_difference} MB/sec).")

                if bytes_remaining > 1.0e6:
                    remaining_transfer_size: float = round(bytes_remaining / 1.0e6, 3)
                    units = "MB"
                else:
                    remaining_transfer_size: int = bytes_remaining
                    units = "bytes"

                estimated_time_required_sec: float = bytes_remaining / current_rate
                self.logger.info(f"Remaining data: {remaining_transfer_size} {units}. "
                                  f"Estimated time remaining: {round(estimated_time_required_sec * 1.0e3, 4)} ms.\n")

                # We'll sleep for at most 1 second.
                # If there's less than 1 second of I/O time remaining based on the current rate, then we'll
                # sleep for however long is required.
                sleep_interval_sec = min(1.0, estimated_time_required_sec)
        else:
            time.sleep(estimated_time_required_ms)

        return True, initial_estimated_time_required_ms

    def upload_data(self, size_bytes: int = 0):
        """
        Simulate a write I/O operation.

        Args:
            size_bytes: the size of the data to be written in bytes.

        Raises a TimeoutError if the simulated I/O operation fails.
        """
        if size_bytes < 0:
            raise ValueError(f"Invalid upload size specified: {size_bytes} bytes. value must be positive.")

        start_time: float = time.time()
        success, estimated_time_required_ms = self.__simulate_network_io_operation(
            size_bytes = size_bytes,
            base_rate = self.upload_rate,
            rate_variance_percent = self.upload_variance_percent,
            failure_percentage_chance = self.write_failure_chance_percentage,
            operation_name = "upload"
        )

        if not success:
            # Note: this call will raise a TimeoutError.
            self.__write_operation_failed((time.time() - start_time) * 1.0e3)
            return

        latency_ms: float = round((time.time() - start_time) * 1.0e3, 4)

        self.__write_operation_succeeded(latency_ms)

        if size_bytes > 1.0e6:
            transfer_size: float = round(size_bytes / 1.0e6, 3)
            units = "MB"
        else:
            transfer_size: int = size_bytes
            units = "bytes"

        difference: float = round(latency_ms - estimated_time_required_ms, 3)
        if difference > 0:
            self.logger.info(f"Simulated network write of size {transfer_size} {units} targeting {self.name} completed "
                             f"after {latency_ms} ms. Network write took {abs(difference)} ms longer than expected.")
        else:
            self.logger.info(f"Simulated network write of size {transfer_size} {units} targeting {self.name} completed "
                              f"after {latency_ms} ms. Network write took {abs(difference)} ms shorter than expected.")

    def download_data(self, size_bytes: int = 0):
        """
        Simulate a write I/O operation.

        Args:
            size_bytes: the size of the data to be written in bytes.

        Raises a TimeoutError if the simulated I/O operation fails.
        """
        if size_bytes < 0:
            raise ValueError(f"Invalid download size specified: {size_bytes} bytes. value must be positive.")

        start_time: float = time.time()
        success, estimated_time_required_ms = self.__simulate_network_io_operation(
            size_bytes = size_bytes,
            base_rate = self.upload_rate,
            rate_variance_percent = self.upload_variance_percent,
            failure_percentage_chance = self.write_failure_chance_percentage,
            operation_name = "download"
        )

        if not success:
            # Note: this call will raise a TimeoutError.
            self.__read_operation_failed((time.time() - start_time) * 1.0e3)
            return

        latency_ms: float = (time.time() - start_time) * 1.0e3

        self.__read_operation_succeeded(latency_ms)

        if size_bytes > 1.0e6:
            transfer_size: float = round(size_bytes / 1.0e6, 3)
            units = "MB"
        else:
            transfer_size: int = size_bytes
            units = "bytes"

        difference: float = round(latency_ms - estimated_time_required_ms, 3)
        if difference > 0:
            self.logger.info(f"Simulated network read of size {transfer_size} {units} targeting {self.name} completed "
                             f"after {latency_ms} ms. Network write took {abs(difference)} ms longer than expected.")
        else:
            self.logger.info(f"Simulated network read of size {transfer_size} {units} targeting {self.name} completed "
                             f"after {latency_ms} ms. Network write took {abs(difference)} ms shorter than expected.")

    def __write_operation_succeeded(self, latency_ms: float):
        self.total_num_io_ops += 1
        self.total_num_write_ops += 1
        self.num_successful_write_ops += 1
        self.write_latencies.append(latency_ms)

    def __write_operation_failed(self, latency_ms: float):
        self.total_num_failures += 1
        self.num_failed_write_ops += 1
        raise TimeoutError(f"Simulated network write operation failed after {latency_ms} ms. "
                           f"Likelihood: {self.read_failure_chance_percentage * 100.0}%.")

    def __read_operation_succeeded(self, latency_ms: float):
        self.total_num_io_ops += 1
        self.total_num_read_ops += 1
        self.num_successful_read_ops += 1
        self.read_latencies.append(latency_ms)

    def __read_operation_failed(self, latency_ms: float, roll: float):
        self.total_num_failures += 1
        self.num_failed_read_ops += 1
        raise TimeoutError(f"Simulated network read operation failed after {latency_ms} ms. "
                           f"Roll: {roll}. Likelihood: {self.read_failure_chance_percentage}.")

    read_data = download_data  # alias for download_data
    write_data = upload_data  # alias for upload_data
    async_read_data = async_download_data  # alias for async_download_data
    async_write_data = async_upload_data  # alias for async_upload_data

def main():
    aws_s3 = SimulatedCheckpointer(
        name = "AWS S3",
        download_rate = 50_000_000,
        upload_rate = 25_000_000,
        download_variance_percent = 0.05,
        upload_variance_percent = 0.05,
        read_failure_chance_percentage = 0.0,
        write_failure_chance_percentage = 0.0
    )

    aws_s3.download_data(512_000_000)
    print("\n\n")
    aws_s3.upload_data(128_000_000)

if __name__ == "__main__":
    main()