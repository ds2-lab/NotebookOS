from jupyter_server.services.sessions.sessionmanager import SessionManager, KernelSessionRecord
from jupyter_core.utils import ensure_async
from typing import Any, Dict, List, NewType, Optional, Union, cast

from traitlets import Instance

KernelName = NewType("KernelName", str)
ModelName = NewType("ModelName", str)

class DistributedSessionManager(SessionManager):
    kernel_manager = Instance("distributed_notebook.manager.distributed_kernel_manager.DistributedKernelManager")

    def __init__(self, *args, **kwargs):
        """Initialize a record list."""
        super().__init__(*args, **kwargs)
        self.log.info("Created a new instance of DistributedSessionManager.")

    async def create_session(
        self,
        path: Optional[str] = None,
        name: Optional[ModelName] = None,
        type: Optional[str] = None,
        kernel_name: Optional[KernelName] = None,
        kernel_id: Optional[str] = None,
        session_id: Optional[str] = None,
        resource_spec: Optional[dict[str, int]] = None,
    ) -> Dict[str, Any]:
        """Creates a session and returns its model

        Parameters
        ----------
        name: ModelName(str)
            Usually the model name, like the filename associated with current
            kernel.
        """
        self.log.info("DistributedSessionManager is creating new Session: Path=%s, Name=%s, Type=%s, KernelName=%s, KernelId=%s, SessionId=%s, ResourceSpec=%s" % (path, name, type, kernel_name, kernel_id, session_id, str(resource_spec)))
        if session_id is None:
            session_id = self.new_session_id()
            self.log.info("Generated new SessionID: \"%s\"" % session_id)
        else:
            self.log.info("Using provided SessionID: \"%s\"" % session_id)
        record = KernelSessionRecord(session_id=session_id)
        self._pending_sessions.update(record)
        if kernel_id is not None and kernel_id in self.kernel_manager:
            pass
        else:
            kernel_id = await self.start_kernel_for_session(
                session_id, path, name, type, kernel_name, kernel_id = kernel_id, resource_spec = resource_spec,
            )
        record.kernel_id = kernel_id
        self._pending_sessions.update(record)
        result = await self.save_session(
            session_id, path=path, name=name, type=type, kernel_id=kernel_id
        )
        self._pending_sessions.remove(record)
        return cast(Dict[str, Any], result)

    async def start_kernel_for_session(
        self,
        session_id: str,
        path: Optional[str],
        name: Optional[ModelName],
        type: Optional[str],
        kernel_name: Optional[KernelName],
        kernel_id: Optional[str] = None, 
        resource_spec: Optional[dict[str, int]] = None,
    ) -> str:
        """Start a new kernel for a given session.

        Parameters
        ----------
        session_id : str
            uuid for the session; this method must be given a session_id
        path : str
            the path for the given session - seem to be a session id sometime.
        name : str
            Usually the model name, like the filename associated with current
            kernel.
        type : str
            the type of the session
        kernel_name : str
            the name of the kernel specification to use.  The default kernel name will be used if not provided.
        kernel_id : str
            uuid for the new kernel; if none, then a kernel id will be generated automatically for the new kernel.
        resource_spec : dict[str, int]
            the resource specification for the new session. kernels of this session will be created with these resource limits within Kubernetes.
        """
        # allow contents manager to specify kernels cwd
        kernel_path = await ensure_async(self.contents_manager.get_kernel_path(path=path))

        kernel_env = self.get_kernel_env(path, name)
        returned_kernel_id = await self.kernel_manager.start_kernel(
            path=kernel_path,
            kernel_name=kernel_name,
            env=kernel_env,
            kernel_id=kernel_id,
            resource_spec=resource_spec,
        )
        
        if kernel_id is not None:
            assert(returned_kernel_id == kernel_id)
        else:
            self.log.debug("Kernel %s (generated uuid) was created for Session %s." % (kernel_id, session_id))
        
        return cast(str, kernel_id)