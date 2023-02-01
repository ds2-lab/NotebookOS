import signal
from lib2to3.pgen2.token import OP
from jupyter_client.provisioning import KernelProvisionerBase
from jupyter_client.connect import KernelConnectionInfo

from typing import Any, Dict, List, Optional

from traitlets.config import Unicode

import grpc
from ..gateway import gateway_pb2
from ..gateway.gateway_pb2_grpc import LocalGatewayStub


class GatewayProvisioner(KernelProvisionerBase):
  # The properties read from the config of the kernel spec: "metadata.kernel_provisioner.config"
  gateway: str = Unicode(None, allow_none=False)

  # Local properties
  gatewayChannel = None
  gatewayStub: LocalGatewayStub
  launched = False
  autoclose = True

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
      kernelId = gateway_pb2.KernelId(id=self.kernel_id)
      status = self._get_stub().GetKernelStatus(kernelId)

      if status.status < 0:
        return None
      
      self.launched = False
      return status.status

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
      kernelId = gateway_pb2.KernelId(id=self.kernel_id)
      status = self._get_stub().WaitKernel(kernelId)

      if status.status < 0:
        return None
      
      self.launched = False
      return status.status

    except grpc.RpcError as e:
      self._try_close()
      raise RuntimeError(f"Failed to get wait kernel: {e}")

  async def send_signal(self, signum: int) -> None:
    """
    Sends signal identified by signum to the kernel process.
    This method is called from `KernelManager.signal_kernel()` to send the
    kernel process a signal.
    """
    if signum == 0:
      return await self.poll()
    elif signum == signal.SIGKILL:
      return await self.kill()
    elif signum == signal.SIGTERM or signum == signal.SIGINT:
      return await self.terminate()
    else:
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
      kernelId = gateway_pb2.KernelId(id=self.kernel_id)
      self._get_stub().KillKernel(kernelId)
      self.launched = False
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
      kernelId = gateway_pb2.KernelId(id=self.kernel_id)
      self._get_stub().StopKernel(kernelId)
    except grpc.RpcError as e:
      self._try_close()
      raise RuntimeError(f"Failed to kill kernel: {e}")

  async def launch_kernel(self, cmd: List[str], **kwargs: Any) -> KernelConnectionInfo:
    """
    Launch the kernel process and return its connection information.
    This method is called from `KernelManager.launch_kernel()` during the
    kernel manager's start kernel sequence.
    """
    try:
      spec = gateway_pb2.KernelSpec(
        id=self.kernel_id, 
        argv=cmd, 
        signatureScheme=self.parent.session.signature_scheme,
        key=self.parent.session.key)
      connectionInfo = self._get_stub().StartKernel(spec)
      self.launched = True

      return dict(
        key = connectionInfo.key,
        ip = connectionInfo.ip,
        control_port = connectionInfo.controlPort,
        shell_port = connectionInfo.shellPort,
        stdin_port = connectionInfo.stdinPort,
        hb_port = connectionInfo.hbPort,
        iopub_port = connectionInfo.iopubPort,
        transport = connectionInfo.transport,
        signature_scheme = connectionInfo.signatureScheme,
      )
    except grpc.RpcError as e:
      self._try_close()
      raise RuntimeError(f"Failed to launch kernel: {e}")

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
    pass

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

    # cmd is a must key to return.
    return await super().pre_launch(cmd=self.kernel_spec.argv, **kwargs)

  # async def post_launch(self, **kwargs: Any) -> None:
  #   """
  #   Perform any steps following the kernel process launch.
  #   This method is called from `KernelManager.post_start_kernel()` as part of its
  #   start kernel sequence.
  #   """
  #   pass

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
    self.gateway = provisioner_info['gateway']

  def _get_stub(self) -> LocalGatewayStub:
    if self.gatewayChannel == None:
      self.gatewayChannel =grpc.insecure_channel(self.gateway)
      self.gatewayStub = LocalGatewayStub(self.gatewayChannel)

    return self.gatewayStub

  def _try_close(self) -> None:
    if self.autoclose and self.gatewayChannel != None:
      self.gatewayStub = None
      self.gatewayChannel.close()
      self.gatewayChannel = None