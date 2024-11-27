import pprint
import signal
from typing import Any, Dict, List, Optional, Union

import grpc
from grpc import aio
from grpc.aio import AioRpcError
from jupyter_client.connect import KernelConnectionInfo
from jupyter_client.provisioning.provisioner_base import KernelProvisionerBase
from traitlets.config import Unicode

from .kernel_creation_error import KernelCreationError
from ..gateway import gateway_pb2
from ..gateway.gateway_pb2_grpc import LocalGatewayStub


class GatewayProvisioner(KernelProvisionerBase):
    # The properties read from the config of the kernel spec: "metadata.kernel_provisioner.config"
    gateway: Union[str, Unicode] = Unicode(None, allow_none=True)

    num_kernels_creating: int = 1

    # Local properties
    gatewayChannel: aio.Channel = None
    gatewayStub: LocalGatewayStub
    launched = False
    autoclose = True

    # Our version of kernel_id
    _kernel_id: Union[str, Unicode] = Unicode(None, allow_none=True)

    # Allow up to 5 minutes for the kernel to shut down gracefully, as it may be offloading lots of data before exiting.
    _kernel_shutdown_wait_time: float = 300.0

    @property
    def has_process(self) -> bool:
        """
        Returns true if this provisioner is currently managing a process.
        This property is asserted to be True immediately following a call to
        the provisioner's :meth:`launch_kernel` method.
        """
        return self.launched

    async def poll(self) -> Optional[int]:
        """
        Checks if kernel process is still running.

        If running, None is returned, otherwise the process's integer-valued exit code is returned.
        This method is called from :meth:`KernelManager.is_alive`.
        """
        try:
            if self.launched:
                kernelId = gateway_pb2.KernelId(id=self._kernel_id)
                self.log.info(
                    f"Checking status of kernel {self._kernel_id} ({kernelId})")
                status = await self._get_stub().GetKernelStatus(kernelId)

                if status.status < 0:
                    return None

                self.launched = False
                self.log.info(
                    f"Kernel stopped on polling kernel {self._kernel_id}. Kernel status: {status.status}.")
                return status.status
            else:
                return 0

        except grpc.RpcError as e:
            self._try_close()
            raise RuntimeError(f"Failed to get kernel status: {e}")

    async def wait(self) -> Optional[int]:
        """
        Waits for kernel process to terminate.
        This method is called from `KernelManager.finish_shutdown()` and
        `KernelManager.kill_kernel()` when terminating a kernel gracefully or
        immediately, respectively.
        """
        try:
            if self.launched:
                kernelId = gateway_pb2.KernelId(id=self._kernel_id)
                status = await self._get_stub().WaitKernel(kernelId)

                self.launched = False
                self.log.info(f"Stopped kernel {self._kernel_id}")
                return status.status
            else:
                return 0

        except grpc.RpcError as e:
            self._try_close()
            raise RuntimeError(f"Failed to get wait kernel: {e}")

    async def send_signal(self, signum: int) -> None:
        """
        Sends signal identified by signum to the process group of the kernel. 
        (This usually includes the kernel and any subprocesses spawned by the kernel.)
        
        This method is called from `KernelManager.signal_kernel()` to send the kernel process a signal.
        """
        if signum == 0:
            await self.poll()
            return
        elif signum == signal.SIGKILL:
            self.log.warn("Received SIGKILL. Un-aliving the kernel now.")
            return await self.kill()
        elif signum == signal.SIGTERM:
            # Shutdown requested, delay and wait for restart flag.
            self.log.warn("Received SIGINT. Kernel shutdown requested.")
            return
        elif signum == signal.SIGINT:
            self.log.warn("Received SIGINT. Kernel interruption requested.")
            return await super().send_signal(signum)
        else:
            self.log.warn("Received signal number %d." % signum)
            return await super().send_signal(signum)

    async def kill(self, restart: bool = False) -> None:
        """
        Kill the kernel process.
        This is typically accomplished via a SIGKILL signal, which cannot be caught.
        This method is called from `KernelManager.kill_kernel()` when terminating
        a kernel immediately.
        restart is True if this operation will precede a subsequent launch_kernel request.
        """
        try:
            if self.launched:
                self.log.info(
                    f"Killing kernel {self._kernel_id}, will restart: {restart} ...")
                kernelId = gateway_pb2.KernelId(
                    id=self._kernel_id, restart=restart)
                await self._get_stub().KillKernel(kernelId)
                self.launched = False
                self.log.info(f"Killed kernel {self._kernel_id}")
            else:
                self.log.debug(f"Cannot kill kernel {self._kernel_id} as it has not yet been launched.")
        except grpc.RpcError as e:
            self._try_close()
            raise RuntimeError(f"Failed to kill kernel: {e}")

    async def terminate(self, restart: bool = False) -> None:
        """
        Terminates the kernel process.
        This is typically accomplished via a SIGTERM signal, which can be caught, allowing
        the kernel provisioner to perform possible cleanup of resources.  This method is
        called indirectly from `KernelManager.finish_shutdown()` during a kernel's
        graceful termination.
        restart is True if this operation precedes a start launch_kernel request.
        """
        try:
            if self.launched:
                self.log.info(
                    f"Stopping kernel {self._kernel_id}, will restart: {restart} ...")
                kernelId = gateway_pb2.KernelId(
                    id=self._kernel_id, restart=restart)
                await self._get_stub().StopKernel(kernelId)
            else:
                self.log.debug(f"Cannot terminate kernel {self._kernel_id} as it has not yet been launched.")
        except grpc.RpcError as e:
            self._try_close()
            raise RuntimeError(f"Failed to terminate kernel: {e}")

    async def launch_kernel(self, cmd: List[str], **kwargs: Any) -> KernelConnectionInfo:
        """
        Launch the kernel process and return its connection information.
        This method is called from `KernelManager.launch_kernel()` during the
        kernel manager's start kernel sequence.
        """
        assert self.parent is not None
        self.num_kernels_creating += 1
        self.log.info("launch_kernel[self.parent.session.session: %s, num_kernels_creating: %d]" % (
            str(self.parent.session.session), self.num_kernels_creating))

        if "resource_spec" in kwargs:
            resource_spec: dict[str, float | int] = kwargs["resource_spec"]
            self.log.debug("Received resource spec for kernel %s: %s" % (self.kernel_id, str(kwargs["resource_spec"])))
        else:
            resource_spec: dict[str, float | int] = {"cpu": 0, "gpu": 0, "memory": 0}
            self.log.error("Did not receive a resource spec for kernel %s." % self.kernel_id)

        spec = gateway_pb2.ResourceSpec(
            cpu=resource_spec.get("cpu", 0),
            gpu=resource_spec.get("gpu", 0),
            memory=resource_spec.get("memory", 0)
        )

        try:
            spec = gateway_pb2.KernelSpec(
                id=self._kernel_id,
                session=self.parent.session.session,
                argv=cmd,
                signatureScheme=self.parent.session.signature_scheme,
                key=self.parent.session.key,
                resourceSpec=spec,
                workloadId = kwargs.get("workload_id", ""))

            self.log.debug(f"Launching kernel {self.kernel_id} with spec: {str(spec)}")

            connectionInfo = await self._get_stub().StartKernel(spec)
            self.launched = True

            self.log.info(
                f"Launched kernel {self.kernel_id}: {connectionInfo}")

            conn_info = dict(
                key=connectionInfo.key,
                ip=connectionInfo.ip,
                control_port=connectionInfo.controlPort,
                shell_port=connectionInfo.shellPort,
                stdin_port=connectionInfo.stdinPort,
                hb_port=connectionInfo.hbPort,
                iosub_port=connectionInfo.iosubPort,
                ack_port=connectionInfo.ackPort,
                iopub_port=connectionInfo.iopubPort,
                transport=connectionInfo.transport,
                signature_scheme=connectionInfo.signatureScheme,
            )

            pprinter = pprint.PrettyPrinter()
            conn_info_formatted = pprinter.pformat(conn_info)
            self.log.info(f"Returning connection info:\n{conn_info_formatted}")

            if type(conn_info["key"]) is str:
                conn_info["key"] = conn_info["key"].encode()

            self.num_kernels_creating -= 1
            return conn_info
        except AioRpcError as e:
            grpc_status_code: grpc.StatusCode = e.code()
            debug_error_str: str = e.debug_error_string()
            details: Optional[str] = e.details()

            self.log.error(f"Failed to launch kernel '{self._kernel_id}'")
            self.log.error(f"gRPC Error Code: {grpc_status_code}")
            self.log.error(f"Debug String from Server: {debug_error_str}")

            if details is not None:
                self.log.error(f"Error details: {details}")

            await self._try_close()
            self.num_kernels_creating -= 1

            # raise RuntimeError(f"gRPC Error {error_code}: {debug_error_str}")
            raise KernelCreationError(
                f"failed to create kernel {self._kernel_id}",
                grpc_status_code=grpc_status_code,
                grpc_debug_error_str=debug_error_str,
                grpc_details=details
            )
        except grpc.RpcError as e:
            self.log.error(f"Failed to launch kernel \"{self._kernel_id}\" because of grpc.RpcError: {e}")

            await self._try_close()
            self.num_kernels_creating -= 1

            # Re-raise the exception, but wrap it in a runtime error this time.
            raise RuntimeError(str(e))
        except Exception as e:
            self.log.error(f"Failed to launch kernel \"{self._kernel_id}\" due to "
                           f"exception of type {type(e).__name__}: {e}")

            await self._try_close()
            self.num_kernels_creating -= 1

            raise RuntimeError(f"Failed to launch kernel due to exception of type {type(e).__name__}: {e}")

    async def cleanup(self, restart: bool = False) -> None:
        """
        Cleanup any resources allocated on behalf of the kernel provisioner.
        This method is called from `KernelManager.cleanup_resources()` as part of
        its shutdown kernel sequence.
        restart is True if this operation precedes a start launch_kernel request.
        """
        pass

    async def shutdown_requested(self, restart: bool = False) -> None:
        """
        Allows the provisioner to determine if the kernel's shutdown has been requested.
        This method is called from `KernelManager.request_shutdown()` as part of
        its shutdown sequence.
        This method is optional and is primarily used in scenarios where the provisioner
        may need to perform other operations in preparation for a kernel's shutdown.
        """
        # Commented out.
        #
        # I don't think we should initiate the shutdown here?
        #
        # The KernelProvisionerBase::shutdown_requested method is called by the KernelManager
        # when it sends the 'shutdown_request' ZMQ message. As a result, there can be cases
        # where two 'shutdown_requests' are sent -- one directly to the kernel, and another
        # through the RPC chain from provisioner --> cluster gateway --> local daemon --> "shutdown_request" 
        # message to the kernel.
        #
        # Previously, this was OK. But now that we ACK messages, it results in the second "shutdown_request"
        # never being ACK'd, which is just adding unnecessary overhead, and complicating debugging.
        #
        # await self.terminate(restart=restart)
        self.log.debug("shutdown_requested called. Kernel will be shutting down.")
        return

    async def pre_launch(self, **kwargs: Any) -> Dict[str, Any]:
        """
        Perform any steps in preparation for kernel process launch.
        This includes applying additional substitutions to the kernel launch command
        and environment. It also includes preparation of launch parameters.
        NOTE: Subclass implementations are advised to call this method as it applies
        environment variable substitutions from the local environment and calls the
        provisioner's :meth:`_finalize_env()` method to allow each provisioner the
        ability to cleanup the environment variables that will be used by the kernel.
        This method is called from `KernelManager.pre_start_kernel()` as part of its
        start kernel sequence.
        Returns the (potentially updated) keyword arguments that are passed to
        :meth:`launch_kernel()`.
        """
        self._kernel_id = self.kernel_id

        self.log.debug(
            "Pre-launching kernel. self.kernel_id=%s, self._kernel_id=%s" % (str(self.kernel_id), str(self._kernel_id)))

        if "resource_spec" in kwargs:
            self.log.debug("Received resource spec for kernel %s: %s" % (self.kernel_id, str(kwargs["resource_spec"])))
        else:
            self.log.error("Did not receive a resource spec for kernel %s." % self.kernel_id)

        # cmd is a must key to return.
        return await super().pre_launch(cmd=self.kernel_spec.argv, **kwargs)

    async def post_launch(self, **kwargs: Any) -> None:
        """
        Perform any steps following the kernel process launch.
        This method is called from `KernelManager.post_start_kernel()` as part of its
        start kernel sequence.
        """
        self.log.info(f"post_launch called for kernel {self.kernel_id}")
        pass

    async def get_provisioner_info(self) -> Dict[str, Any]:
        """
        Captures the base information necessary for persistence relative to this instance.
        This enables applications that subclass `KernelManager` to persist a kernel provisioner's
        relevant information to accomplish functionality like disaster recovery or high availability
        by calling this method via the kernel manager's `provisioner` attribute.
        NOTE: The superclass method must always be called first to ensure proper serialization.
        """
        provisioner_info = await super().get_provisioner_info()
        provisioner_info['gateway'] = self.gateway
        self.log.debug("Getting provisioner info: %s" % str(provisioner_info))
        return provisioner_info

    async def load_provisioner_info(self, provisioner_info: Dict) -> None:
        """
        Loads the base information necessary for persistence relative to this instance.
        The inverse of `get_provisioner_info()`, this enables applications that subclass
        `KernelManager` to re-establish communication with a provisioner that is managing
        a (presumably) remote kernel from an entirely different process that the original
        provisioner.
        NOTE: The superclass method must always be called first to ensure proper deserialization.
        """
        self.log.debug("Loading provisioner info: %s" % str(provisioner_info))
        self.gateway = provisioner_info['gateway']

    def get_shutdown_wait_time(self, recommended: Optional[float] = 5.0) -> float:
        """
        Returns the time allowed for a complete shutdown. 

        This method is called from `KernelManager.finish_shutdown()` during the graceful
        phase of its kernel shutdown sequence.

        The recommended value will typically be what is configured in the kernel manager.
        """
        if recommended is None or recommended < self._kernel_shutdown_wait_time:
            recommended = self._kernel_shutdown_wait_time

            self.log.debug(f"{type(self).__name__} shutdown wait time adjusted to {recommended} seconds.")

        return recommended

    def _get_stub(self) -> LocalGatewayStub:
        if self.gatewayChannel is None:
            self.log.debug(
                "Creating GatewayChannel now. Gateway: \"%s\"" % self.gateway)
            self.gatewayChannel: aio.Channel = aio.insecure_channel(self.gateway)
            self.gatewayStub = LocalGatewayStub(self.gatewayChannel)

        return self.gatewayStub

    async def _try_close(self) -> None:
        if self.autoclose and self.gatewayChannel is not None:
            # asyncio.get_event_loop().run_until_complete(self.gatewayChannel.close())
            await self.gatewayChannel.close()
            self.gatewayChannel = None
