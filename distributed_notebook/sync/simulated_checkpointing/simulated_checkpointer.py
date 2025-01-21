import asyncio
import logging
import random
import sys
import time
import math

from typing import Any

from ...logs.color_formatter import ColoredLogFormatter

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


def format_rate(rate_bytes: float = 0) -> str:
    """
    Given a rate whose units are bytes/second, return a string representation of the given rate, such that
    the string displays the largest units for which the rate is >= 1 unit/sec.

    The largest supported units are petabytes (PB) while the smallest are bytes.

    Examples:
    1_000_000_000 --> 1 GB/sec
        1_000_000 --> 1 MB/sec
          100_000 --> 100 KB/sec
           10_000 --> 10 KB/sec
            1_000 --> 1 KB/sec
              100 --> 100 bytes/sec
    """
    if rate_bytes > 1e9:
        return "%.2f GB/sec" % (rate_bytes / 1.0e9)
    if rate_bytes > 1e6:
        return "%.2f MB/sec" % (rate_bytes / 1.0e6)
    if rate_bytes > 1e3:
        return "%.2f KB/sec" % (rate_bytes / 1.0e3)
    return "%.2f bytes/sec" % rate_bytes


def format_size(size_bytes: float = 0) -> str:
    """
    Given a size in bytes, return a string representation of the size with the largest units
    for which the size >= 1 unit.

    The largest supported units are petabytes (PB) while the smallest are bytes.

    Examples:
    1_000_000_000 --> 1 GB
        1_000_000 --> 1 MB
          100_000 --> 100 KB
           10_000 --> 10 KB
            1_000 --> 1 KB
              100 --> 100 bytes
    """
    if size_bytes > 1e12:
        return "%.2f PB" % (size_bytes/1.0e12)
    if size_bytes > 1e9:
        return "%.2f GB" % (size_bytes/1.0e9)
    if size_bytes > 1e6:
        return "%.2f MB" % (size_bytes/1.0e6)
    if size_bytes > 1e3:
        return "%.2f KB" % (size_bytes/1.0e3)
    return "%.2f bytes" % size_bytes

def get_estimated_io_time_seconds(size_bytes: float = 0, rate: float = 0):
    if size_bytes < 0:
        raise ValueError(f"invalid IO size: {size_bytes} bytes. IO size must be >= 0.")

    if rate <= 0:
        raise ValueError(f"invalid IO rate: {rate} bytes/second. IO rate must be > 0.")

    return size_bytes / rate


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
        self.download_rate: int = download_rate

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

        self.log: logging.Logger = logging.getLogger(f"SimulatedCheckpointer[{self.name}] ")
        self.log.handlers.clear()
        self.log.setLevel(logging.DEBUG)
        self.log.propagate = True
        ch = logging.StreamHandler()
        ch.setFormatter(ColoredLogFormatter())
        self.log.addHandler(ch)

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

        # Note:
        # We take advantage of the fact that the last_io_timestamp field is initialized to -1 in particular in the
        # DistributedKernel::get_most_recently_used_remote_storage method. So, if we change the initial value of
        # last_io_timestamp, then we'll need to update the DistributedKernel::get_most_recently_used_remote_storage
        # method.

        # This is set each time we simulate an upload/write or download/read.
        self.last_io_timestamp: float = -1
        # This is set each time we simulate a download/read.
        self.last_read_timestamp: float = -1
        # This is set each time we simulate an upload/write.
        self.last_write_timestamp: float = -1

    @property
    def upload_rate_formatted(self) -> str:
        return format_rate(self.upload_rate)

    @property
    def download_rate_formatted(self) -> str:
        return format_rate(self.download_rate)

    def __str__(self):
        return f"{self.name} [DownloadRate={self.download_rate_formatted},UploadRate={self.upload_rate_formatted}]"

    def __repr__(self):
        return self.__str__()

    def __getstate__(self) -> dict[str, Any]:
        state: dict[str, Any] = self.__dict__.copy()
        del state["log"]

        return state

    def __setstate__(self, state: dict[str, Any]):
        self.__dict__.update(state)

        try:
            getattr(self, "log")
        except AttributeError:
            self.log: logging.Logger = logging.getLogger(f"SimulatedCheckpointer[{self.name}] ")
            self.log.handlers.clear()
            self.log.setLevel(logging.DEBUG)
            self.log.propagate = False
            ch = logging.StreamHandler()
            ch.setFormatter(ColoredLogFormatter())
            self.log.addHandler(ch)

    def get_estimated_write_time_seconds(self, write_size_bytes: float = 0, rate: float = -1) -> float:
        if write_size_bytes < 0:
            raise ValueError(f"invalid write size: {write_size_bytes} bytes. write size must be >= 0.")

        if rate < 0:
            rate = get_new_rate(self.upload_rate, self.upload_variance_percent)

        return get_estimated_io_time_seconds(size_bytes = write_size_bytes, rate = rate)

    def get_estimated_read_time_seconds(self, read_size_bytes: float = 0, rate: float = -1) -> float:
        if read_size_bytes < 0:
            raise ValueError(f"invalid read size: {read_size_bytes} bytes. read size must be >= 0.")

        if rate < 0:
            rate = get_new_rate(self.download_rate, self.download_variance_percent)

        return get_estimated_io_time_seconds(size_bytes = read_size_bytes, rate = rate)

    async def __simulate_network_io_operation(
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
        initial_estimated_time_required_seconds: float = get_estimated_io_time_seconds(size_bytes = size_bytes, rate = current_rate)
        initial_estimated_time_required_ms: float = initial_estimated_time_required_seconds * 1.0e3
        estimated_time_required_ms: float = initial_estimated_time_required_ms

        if size_bytes == 0:
            roll: float = random.random()
            failure_occurred: bool = roll < failure_percentage_chance

            if failure_occurred:
                self.log.warning(f"Simulated {operation_name} of 0 bytes failed immediately.")
                return False, 0 # Size of 0 bytes, so instantaneous.

            self.log.debug(f"Simulated {operation_name} of 0 bytes succeeded immediately.")
            return True, 0 # Size of 0 bytes, so instantaneous.

        if size_bytes > 1.0e6:
            transfer_size: float = round(size_bytes / 1.0e6, 3)
            rate_formatted: float = round(current_rate / 1.0e6, 3)
            transfer_units = "MB"
        else:
            transfer_size: int = size_bytes
            rate_formatted: float = current_rate
            transfer_units = "bytes"

        self.log.info(f"Simulated network {operation_name} of size {transfer_size} {transfer_units} "
                          f"targeting {self.name} is estimated to take {round(estimated_time_required_ms, 2)} ms to "
                          f"complete at current data transfer rate of {rate_formatted} {transfer_units}/sec.")

        sys.stdout.flush()
        sys.stderr.flush()

        # If our transfer rate is permitted to vary, then we'll update the rate every second.
        # If the estimated transfer time is less than one second, then the rate will not vary.
        if rate_variance_percent > 0 and estimated_time_required_ms > 1.0e3:
            bytes_remaining: int = size_bytes
            sleep_interval_sec: float = 1

            # Keep sleeping in ~1sec increments until we've transferred everything.
            while bytes_remaining > 0:
                # self.log.debug("Sleeping for 1 second as remaining data transfer is expected to take longer than that.")
                await asyncio.sleep(sleep_interval_sec)

                # Since current_rate is bytes/second, and we just simulated I/O for 1 second,
                # the number of bytes transferred is just equal to current_rate.
                bytes_remaining -= current_rate

                # If we've transferred everything, then we're done and can exit the loop.
                if bytes_remaining <= 0:
                    break

                current_rate = get_new_rate(base_rate, rate_variance_percent)

                if current_rate > base_rate:
                    rate_difference: float = round((current_rate - base_rate) / 1.0e6, 3)
                    self.log.debug(f"Got new {operation_name} rate: "
                                      f"{current_rate/1.0e6} MB/sec (+{rate_difference} MB/sec).")
                else:
                    rate_difference: float = round((base_rate - current_rate) / 1.0e6, 3)
                    self.log.debug(f"Got new {operation_name} rate: "
                                      f"{current_rate/1.0e6} MB/sec (-{rate_difference} MB/sec).")

                if bytes_remaining > 1.0e6:
                    remaining_transfer_size: float = round(bytes_remaining / 1.0e6, 3)
                    units = "MB"
                else:
                    remaining_transfer_size: int = bytes_remaining
                    units = "bytes"

                estimated_time_required_sec: float = get_estimated_io_time_seconds(size_bytes = bytes_remaining, rate = current_rate)
                self.log.info(f"Remaining data: {remaining_transfer_size} {units}. "
                                  f"Estimated time remaining: {round(estimated_time_required_sec * 1.0e3, 4)} ms.\n")

                # We'll sleep for at most 1 second.
                # If there's less than 1 second of I/O time remaining based on the current rate, then we'll
                # sleep for however long is required.
                sleep_interval_sec = min(1.0, estimated_time_required_sec)
                sys.stdout.flush()
                sys.stderr.flush()
        else:
            self.log.debug(f"Sleeping for {estimated_time_required_ms} ms to simulate network {operation_name} "
                              f"of size {size_bytes / 1.0e6:,} MB.")
            await asyncio.sleep(initial_estimated_time_required_seconds)

        return True, initial_estimated_time_required_ms

    async def upload_data(self, size_bytes: int = 0):
        """
        Simulate a write I/O operation.

        Args:
            size_bytes: the size of the data to be written in bytes.

        Raises a TimeoutError if the simulated I/O operation fails.
        """
        if size_bytes < 0:
            raise ValueError(f"Invalid upload size specified: {size_bytes} bytes. value must be positive.")

        start_time: float = time.time()
        self.last_write_timestamp = start_time
        self.last_io_timestamp = start_time
        success, estimated_time_required_ms = await self.__simulate_network_io_operation(
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
            self.log.info(f"Simulated network write of size {transfer_size} {units} targeting {self.name} completed "
                             f"after {latency_ms} ms. Network write took {abs(difference)} ms longer than expected.")
        else:
            self.log.info(f"Simulated network write of size {transfer_size} {units} targeting {self.name} completed "
                              f"after {latency_ms} ms. Network write took {abs(difference)} ms shorter than expected.")

    async def download_data(self, size_bytes: int = 0):
        """
        Simulate a write I/O operation.

        Args:
            size_bytes: the size of the data to be written in bytes.

        Raises a TimeoutError if the simulated I/O operation fails.
        """
        if size_bytes < 0:
            raise ValueError(f"Invalid download size specified: {size_bytes} bytes. value must be positive.")

        start_time: float = time.time()
        self.last_read_timestamp = start_time
        self.last_io_timestamp = start_time
        success, estimated_time_required_ms = await self.__simulate_network_io_operation(
            size_bytes = size_bytes,
            base_rate = self.download_rate,
            rate_variance_percent = self.upload_variance_percent,
            failure_percentage_chance = self.write_failure_chance_percentage,
            operation_name = "download"
        )

        if not success:
            self.log.warning(f"Simulated read/download of {size_bytes / 1.0e6:,} MB failed.")
            # Note: this call will raise a TimeoutError.
            self.__read_operation_failed((time.time() - start_time) * 1.0e3)
            return

        latency_ms: float = (time.time() - start_time) * 1.0e3

        self.log.debug(f"Simulated read/download of {size_bytes / 1.0e6:,} MB succeeded.")
        self.__read_operation_succeeded(latency_ms)

        if size_bytes > 1.0e6:
            transfer_size: float = round(size_bytes / 1.0e6, 3)
            units = "MB"
        else:
            transfer_size: int = size_bytes
            units = "bytes"

        difference: float = round(latency_ms - estimated_time_required_ms, 3)
        if difference > 0:
            self.log.info(f"Simulated network read of size {transfer_size} {units} targeting {self.name} completed "
                             f"after {latency_ms} ms. Network write took {abs(difference)} ms longer than expected.")
        else:
            self.log.info(f"Simulated network read of size {transfer_size} {units} targeting {self.name} completed "
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

async def test_simulated_checkpointer():
    aws_s3 = SimulatedCheckpointer(
        name = "AWS S3",
        download_rate = 50_000_000,
        upload_rate = 25_000_000,
        download_variance_percent = 0.05,
        upload_variance_percent = 0.05,
        read_failure_chance_percentage = 0.0,
        write_failure_chance_percentage = 0.0
    )

    aws_fsx = SimulatedCheckpointer(
        name = "AWS FSx",
        download_rate = 256_000_000,
        upload_rate = 64_000_000,
        download_variance_percent = 0.05,
        upload_variance_percent = 0.05,
        read_failure_chance_percentage = 0.0,
        write_failure_chance_percentage = 0.0
    )

    size:int = 512_000_000
    print(f"\n\n\n\n[AWS FSx] DOWNLOAD | SIZE={size} bytes ({size/1.0e6} MB)\n")
    await aws_fsx.download_data(size)

    size:int = 10_000_000_000
    print(f"\n\n\n\n[AWS FSx] DOWNLOAD | SIZE={size} bytes ({size/1.0e6} MB)\n")
    await aws_fsx.download_data(size)

    size:int = 128_000_000
    print(f"\n\n\n\n[AWS FSx] UPLOAD | SIZE={size} bytes ({size/1.0e6} MB)\n")
    await aws_fsx.upload_data(size)

    size:int = 256_000_000
    print(f"\n\n\n\n[AWS S3] DOWNLOAD | SIZE={size} bytes ({size/1.0e6} MB)\n")
    await aws_s3.download_data(size)

    size:int = 64_000_000
    print(f"\n\n\n\n[AWS S3] UPLOAD | SIZE={size} bytes ({size/1.0e6} MB)\n")
    await aws_s3.upload_data(size)

# if __name__ == "__main__":
#     test_simulated_checkpointer()