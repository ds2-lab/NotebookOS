from jupyter_server.services.kernels.kernelmanager import MappingKernelManager
from jupyter_server.utils import ApiPath, import_item, to_os_path

import traceback 

import uuid

class DistributedKernelManager(MappingKernelManager):
    """Subclass of MultiKernelManager exposing more control over certain aspects of the kernels, such as their IDs.
    """
    def __init__(self, **kwargs):
        """Initialize a kernel manager."""
        super().__init__(**kwargs)
        self.log.info("Created a new instance of DistributedKernelManager.")
    
    def new_kernel_id(self, **kwargs) -> str:
        """
        Returns the id to associate with the kernel for this request. 
        
        :param kwargs:
        :return: string-ized version 4 uuid
        """
        self.log.info("Generating new Kernel ID. KWARGS (%d): %s" % (len(kwargs), str(list(kwargs.keys()))))
        traceback.print_stack()
        kernelId:str = str(uuid.uuid4())
        self.log.info("Generated kernel ID: %s" % kernelId)
        
        return kernelId

    async def _async_start_kernel(  # type:ignore[override]
            self, *, kernel_id: str | None = None, path: ApiPath | None = None, **kwargs: str
        ) -> str:
        self.log.info("_async_start_kernel() called. kernel_id = %s, path = %s, kwargs = %s" % (kernel_id, str(path), str(kwargs)))
        return await super()._async_start_kernel(kernel_id = kernel_id, path = path, **kwargs)