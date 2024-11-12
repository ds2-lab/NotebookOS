from jupyter_server.services.kernels.kernelmanager import MappingKernelManager
from jupyter_server.utils import ApiPath
from jupyter_server.services.kernels.kernelmanager import ServerKernelManager
from jupyter_server._tz import isoformat

import uuid

class DistributedKernelManager(MappingKernelManager, ServerKernelManager):
    """Subclass of MappingKernelManager exposing more control over certain aspects of the kernels, such as their IDs.
    """
    def __init__(self, **kwargs):
        """Initialize a kernel manager."""
        super().__init__(**kwargs)
        self.log.info("Created a new instance of DistributedKernelManager.")

        self.num_kernels_starting: int = 0
    
    def new_kernel_id(self, **kwargs) -> str:
        """
        Returns the id to associate with the kernel for this request. 
        
        :param kwargs:
        :return: string-ized version 4 uuid
        """
        return str(uuid.uuid4())

    async def _async_start_kernel(  # type:ignore[override]
            self, *, kernel_id: str | None = None, path: ApiPath | None = None, **kwargs: str
        ):
        self.num_kernels_starting += 1

        self.log.info("_async_start_kernel() called. kernel_id = %s, path = %s, num_kernels_starting = %d" % (kernel_id, str(path), self.num_kernels_starting))
        returned_kernel_id:str = await super()._async_start_kernel(kernel_id = kernel_id, path = path, **kwargs)
        
        # If the caller didn't specify a particular kernel ID, then that's fine.
        # If they did, then the returned kernel ID should necessarily be equal to whatever was passed by the caller.
        assert kernel_id == "" or kernel_id is None or kernel_id == returned_kernel_id
        
        self.log.info(f"Started kernel {returned_kernel_id}. Number of kernels: {len(self._kernels)}.")
        self.log.info(f"Kernels: {str(self._kernels.keys())}")

        self.num_kernels_starting -= 1

        return returned_kernel_id

    start_kernel = _async_start_kernel

    def kernel_model(self, kernel_id):
        """Return a JSON-safe dict representing a kernel

        For use in representing kernels in the JSON APIs.
        """
        self.log.info("Returning kernel model for kernel %s" % kernel_id)
        
        self._check_kernel_id(kernel_id)
        kernel = self._kernels[kernel_id]

        model = {
            "id": kernel_id,
            "kernel_id": kernel_id,
            "name": kernel.kernel_name,
            "execution_state": kernel.execution_state,
            "connections": self._kernel_connections.get(kernel_id, 0),
        }
        if getattr(kernel, "reason", None):
            model["reason"] = kernel.reason
        if getattr(kernel, "last_activity", None):
            model["last_activity"] = isoformat(kernel.last_activity)
        
        return model