from jupyter_client import MultiKernelManager
import uuid


class DistributedKernelManager(MultiKernelManager):
    def __init__(self, **kwargs):
        """Initialize a kernel manager."""
        super().__init__(**kwargs)

    def new_kernel_id(self, **kwargs) -> str:
        """
        Returns the id to associate with the kernel for this request. Subclasses may override
        this method to substitute other sources of kernel ids.
        :param kwargs:
        :return: string-ized version 4 uuid
        """
        print("Generating new Kernel ID. KWARGS: %s" % str(kwargs))
        return str(uuid.uuid4())
