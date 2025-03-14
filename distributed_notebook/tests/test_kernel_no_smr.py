# This file just has more unit tests, as `test_kernel.py` was getting pretty big.
import shutil
import asyncio
import json
import logging
import os
import socket
import traceback
from asyncio import AbstractEventLoop
from multiprocessing import Process
from typing import List, Dict, Any

import faulthandler
import sys
import time

from distributed_notebook.kernel import DistributedKernel
from distributed_notebook.logs import ColoredLogFormatter

# Create a StreamHandler and set the custom formatter
root_handler = logging.StreamHandler()
root_handler.setFormatter(ColoredLogFormatter())

# Get the root logger and add the handler
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
root_logger.addHandler(root_handler)


def main():
    faulthandler.enable()

    # server_address = ("127.0.0.1", 6379)
    # server = TcpFakeServer(server_address, server_type="redis")
    # t = Thread(target=server.serve_forever, daemon=True)
    # t.start()

    local_daemon_registration_server_port: int = 12000

    smr_ports: Dict[int, int] = {
        1: 8080,
        2: 8081,
        3: 8082
    }

    kernel_id: str = "0f81fcea-441b-4fd0-bce3-e37b91f51533"

    os.environ["KERNEL_ID"] = kernel_id
    os.environ["SESSION_ID"] = kernel_id
    os.environ["LOCAL_DAEMON_SERVICE_PORT"] = str(local_daemon_registration_server_port)
    os.environ["PROMETHEUS_METRICS_PORT"] = str(-1)

    conn_info: Dict[str, str | int] = {
        "ip": "0.0.0.0",
        "transport": "tcp",
        "signature_scheme": "hmac-sha256",
        "key": "",
        "control_port": 9001,
        "shell_port": 9002,
        "stdin_port": 9003,
        "hb_port": 9000,
        "iopub_port": 9004,
        "iosub_port": 9005,
        "ack_port": 0,
        "num_resource_ports": 0,
        "prometheus_port": -1,
    }

    kernel_dir: str = os.path.join("/tmp", f"kernel-{kernel_id}-{1}")
    storage_base: str = os.path.join(kernel_dir, "kernel_storage")

    config: Dict[str, Any] = {
        "DistributedKernel": {
            "storage_base": storage_base,
            "remote_storage_hostname": "distributed-notebook-storage",
            "remote_storage": "s3",
            "local_daemon_addr": f"127.0.0.1",
            "deployment_mode": "docker-compose",
            "workload_id": "",
            "smr_nodes": ["127.0.0.1:8080"],
            "smr_port": 8080,
            "smr_node_id": 1,
            "spec_cpus": 100,
            "spec_mem_mb": 1250,
            "spec_gpus": 1,
            "spec_vram_gb": 1,
            "election_timeout_seconds": 3,
            "smr_join": False,
            "should_register_with_local_daemon": True,
            "simulate_checkpointing_latency": True,
            "simulate_write_after_execute": False,
            "simulate_write_after_execute_on_critical_path": False,
            "smr_enabled": False,
            "simulate_training_using_sleep": False,
            "retrieve_datasets_from_s3": False,
            "datasets_s3_bucket": "",
            "prewarm_container": False,
            "created_for_migration": False,
            "local_tcp_server_port": -1,
            "prometheus_port": -1,
        }
    }

    persistent_id = f"/tmp/{kernel_id}"

    # registration_process: Process = Process(target=kernel_registration_server,
    #                                         args=(local_daemon_registration_server_port, persistent_id))
    # registration_process.start()
    #
    # kernel_process(1, kernel_id, conn_info, config)


def kernel_registration_server(port: int, persistent_id: str):
    server: KernelRegistrationServer = KernelRegistrationServer(port, persistent_id, )
    server.run()


class KernelRegistrationServer(object):
    smr_ports: Dict[int, int] = {
        1: 8080,
        2: 8081,
        3: 8082
    }

    def __init__(self, port: int, persistent_id: str):
        self.port: int = port
        self.persistent_id: str = persistent_id

        # Create the custom formatter
        colored_log_formatter: ColoredLogFormatter = ColoredLogFormatter()

        self.next_id: int = 1

        self.loop: AbstractEventLoop = asyncio.get_event_loop()

        # Create a StreamHandler and set the custom formatter
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(colored_log_formatter)
        handler.setLevel(logging.DEBUG)

        # Get the root logger and add the handler
        self.log = logging.getLogger(f"KernelRegistrationServer")
        self.log.addHandler(handler)
        self.log.setLevel(logging.DEBUG)

    def register_kernel(self, conn):
        self.log.info(f"Registering kernel replica.")
        while True:
            data = conn.recv(1024)
            if not data:
                break

            self.log.info(f"Received data: {data}")

            replicas: Dict[int, str] = {
                1: "localhost:8080"
            }

            responsePayload: Dict[str, Any] = {
                "smr_node_id": self.next_id,
                "hostname": "localhost",
                "replicas": replicas,
                "debug_port": -1,
                "status": "ok",
                "grpc_port": -1,
                "message_acknowledgements_enabled": False,
                "smr_port": self.smr_ports[self.next_id],
                "persistent_id": self.persistent_id,
            }

            responseBytes = json.dumps(responsePayload).encode()

            conn.sendall(responseBytes)

            self.next_id += 1

            return

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # Bind the socket to the address
            s.bind(("localhost", self.port))
            # Enable the server to accept connections
            s.listen()
            self.log.info(f"Server listening on {"localhost"}:{self.port}")
            while True:
                # Accept a connection
                conn, addr = s.accept()
                with conn:
                    self.log.info(f'Connected by {addr}')
                    self.register_kernel(conn)


def kernel_process(replica_id: int, kernel_id: str, conn_info: Dict[str, str | int], config: Dict[str, Any]):
    # Create the custom formatter
    colored_log_formatter: ColoredLogFormatter = ColoredLogFormatter()

    # Create a StreamHandler and set the custom formatter
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(colored_log_formatter)
    handler.setLevel(logging.DEBUG)

    # Get the root logger and add the handler
    process_logger = logging.getLogger(f"KernelProcess{replica_id}")
    process_logger.addHandler(handler)
    process_logger.setLevel(logging.DEBUG)

    process_logger.info(f"Started.")

    profile_subdirectories: List[str] = ["db", "log", "pid", "security", "startup"]

    tmp_profile_dir: str = os.path.join("/tmp", f"kernel-{kernel_id}-{replica_id}")
    os.makedirs(tmp_profile_dir, exist_ok=True)

    storage_base: str = os.path.join(tmp_profile_dir, "kernel_storage")
    os.makedirs(storage_base, exist_ok=True)

    for profile_subdirectory in profile_subdirectories:
        path = os.path.join(tmp_profile_dir, profile_subdirectory)
        os.makedirs(path, exist_ok=True)

    connection_file_path: str = os.path.join(tmp_profile_dir, f"connection-kernel-{kernel_id}-{replica_id}.json")
    config_file_path: str = os.path.join(tmp_profile_dir, f"config-kernel-{kernel_id}-{replica_id}.json")

    with open(connection_file_path, "w") as f:
        json.dump(conn_info, f)

    with open(config_file_path, "w") as f:
        json.dump(config, f)

    argv: List[str] = [sys.executable, "-m", "distributed_notebook.kernel", "-f", connection_file_path, "--config",
                       config_file_path, "--debug",
                       "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"]
    sys.argv = argv

    os.environ["JUPYTER_CONFIG_DIR"] = tmp_profile_dir
    os.environ["JUPYTER_CONFIG_PATH"] = config_file_path

    from ipykernel.kernelapp import IPKernelApp

    # try:
    #     IPKernelApp.launch_instance(kernel_class=DistributedKernel)
    # except Exception as ex:
    #     process_logger.error(f"Error while launching DistributedKernel: {ex}")
    #     process_logger.error(traceback.format_exc())
    #     return

    # shutil.rmtree(tmp_profile_dir)

    process_logger.info(f"Exiting.")

    # time.sleep(1)

    return


if __name__ == "__main__":
    main()
