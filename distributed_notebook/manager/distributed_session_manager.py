import json
from typing import Any, Dict, Optional, cast

import grpc
from grpc.aio import AioRpcError
from jupyter_core.utils import ensure_async
from jupyter_server.services.sessions.sessionmanager import SessionManager, KernelSessionRecord, ModelName, KernelName
from tornado import web

from distributed_notebook.provisioner.kernel_creation_error import KernelCreationError


class DistributedSessionManager(SessionManager):
    # kernel_manager = Instance("distributed_notebook.manager.distributed_kernel_manager.DistributedKernelManager")

    def __init__(self, *args, **kwargs):
        """Initialize a record list."""
        super().__init__(*args, **kwargs)
        self.log.info("Created a new instance of DistributedSessionManager.")
        self.log.info("DistributedSessionManager is using a KernelManager of type %s." % str(type(self.kernel_manager)))
        self.log.info("self.kernel_manager.start_kernel: %s" % str(self.kernel_manager.start_kernel))

        self.num_sessions_creating: int = 0

    async def create_session(
            self,
            path: Optional[str] = None,
            name: Optional[ModelName] = None,
            type: Optional[str] = None,
            kernel_name: Optional[KernelName] = None,
            kernel_id: Optional[str] = None,
            session_id: Optional[str] = None,
            resource_spec: Optional[dict[str, float | int]] = None,
            workload_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Creates a session and returns its model

        Parameters
        ----------
        name: ModelName(str)
            Usually the model name, like the filename associated with current
            kernel.
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
        workload_id : str
            the ID of the workload in which the session is contained
        """
        self.num_sessions_creating += 1
        self.log.info(
            "DistributedSessionManager is creating new Session: Path=%s, Name=%s, Type=%s, KernelName=%s, KernelId=%s, SessionId=%s, ResourceSpec=%s, WorkloadId=%s, NumSessionsCreating=%d" % (
            path, name, type, kernel_name, kernel_id, session_id, str(resource_spec), workload_id, self.num_sessions_creating))

        if session_id is None:
            session_id = self.new_session_id()
            self.log.info("Generated new SessionID: \"%s\"" % session_id)
        else:
            self.log.info("Using provided SessionID: \"%s\"" % session_id)

        record = KernelSessionRecord(session_id=session_id)
        self._pending_sessions.update(record)

        if kernel_id is not None and kernel_id in self.kernel_manager:
            self.log.info(
                f"Kernel {kernel_id} already exists. DistributedSessionManager is skipping kernel-creation step.")
            pass
        else:
            kernel_id = await self.start_kernel_for_session(
                session_id, path, name, type, kernel_name, kernel_id=kernel_id, resource_spec=resource_spec, workload_id=workload_id,
            )

        record.kernel_id = kernel_id
        self._pending_sessions.update(record)
        result = await self.save_session(
            session_id, path=path, name=name, type=type, kernel_id=kernel_id
        )
        self._pending_sessions.remove(record)

        self.num_sessions_creating -= 1

        return cast(Dict[str, Any], result)

    async def start_kernel_for_session(
            self,
            session_id: str,
            path: Optional[str],
            name: Optional[ModelName],
            typ: Optional[str],
            kernel_name: Optional[KernelName],
            kernel_id: Optional[str] = None,
            resource_spec: Optional[dict[str, float | int]] = None,
            workload_id: Optional[str] = None,
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
        typ : str
            the type of the session
        kernel_name : str
            the name of the kernel specification to use.  The default kernel name will be used if not provided.
        kernel_id : str
            uuid for the new kernel; if none, then a kernel id will be generated automatically for the new kernel.
        resource_spec : dict[str, int]
            the resource specification for the new session. kernels of this session will be created with these resource limits within Kubernetes.
        workload_id : str
            the ID of the workload in which the session is contained
        """
        self.log.info(
            "DistributedSessionManager is starting a new Kernel for Session %s: Path=%s, Name=%s, Type=%s, KernelName=%s, KernelId=%s, ResourceSpec=%s" % (
            session_id, path, name, typ, kernel_name, kernel_id, str(resource_spec)))

        # Allow contents manager to specify kernels cwd
        kernel_path = await ensure_async(self.contents_manager.get_kernel_path(path=path))
        kernel_env = self.get_kernel_env(path, name)

        try:
            returned_kernel_id = await self.kernel_manager.start_kernel(
                path=kernel_path,
                kernel_name=kernel_name,
                env=kernel_env,
                kernel_id=kernel_id,
                resource_spec=resource_spec,
                workload_id=workload_id,
            )
        except KernelCreationError as e:
            self.log.error(f"Failed to launch kernel '{kernel_id}'")

            if e.grpc_status_code is not None:
                self.log.error(f"gRPC error code: {e.grpc_status_code}")

            if e.grpc_debug_error_str is not None:
                self.log.error(f"Debug string from server: {e.grpc_debug_error_str}")

            if e.grpc_details is not None:
                self.log.error(f"Error details: {e.grpc_details}")

            error_payload: dict[str, Any] = {
                "message": e.error_message,
                "grpc_status_code": e.grpc_status_code,
                "grpc_debug_error_str": e.grpc_debug_error_str,
                "grpc_details": e.grpc_details,
                "kernel_id": kernel_id,
                "kernel_path": kernel_path,
                "kernel_name": kernel_name,
                "resource_spec": resource_spec
            }
            payload = json.dumps(error_payload)

            raise web.HTTPError(500, str(payload))
        except AioRpcError as e:
            error_code: grpc.StatusCode = e.code()
            debug_error_str: str = e.debug_error_string()
            details: Optional[str] = e.details()

            self.log.error(f"Failed to launch kernel '{kernel_id}'")
            self.log.error(f"gRPC Error Code: {error_code}")
            self.log.error(f"Debug String from Server: {debug_error_str}")

            if details is not None:
                self.log.error(f"Error details: {details}")

            error_payload: dict[str, Any] = {
                "grpc_status_code": error_code,
                "grpc_debug_error_str": debug_error_str,
                "grpc_details": details,
                "kernel_id": kernel_id,
                "kernel_path": kernel_path,
                "kernel_name": kernel_name,
                "resource_spec": resource_spec
            }
            payload = json.dumps(error_payload)

            raise web.HTTPError(500, str(payload))
        except RuntimeError as ex:
            self.log.error(f"Exception encountered while starting kernel \"{kernel_id}\": {ex}")
            raise web.HTTPError(500, str(ex)) from ex

        # If the caller didn't specify a particular kernel ID, then that's fine.
        # If they did, then the returned kernel ID should necessarily be equal to whatever was passed by the caller.
        if kernel_id is not None:
            if kernel_id != returned_kernel_id:
                self.log.error(
                    f"Returned kernel ID is \"{returned_kernel_id}\", whereas kernel ID was supposed to be \"{kernel_id}\".")
                raise ValueError(
                    f"Returned kernel ID of \"{returned_kernel_id}\" is not equal to specified kernel ID of \"{kernel_id}\"")
        else:
            kernel_id = returned_kernel_id

        self.log.debug("Kernel %s was created for Session %s." % (kernel_id, session_id))
        self.log.info(f"Started kernel {returned_kernel_id}. Number of kernels: {len(self.kernel_manager._kernels)}.")
        self.log.info(f"Kernels: {str(self.kernel_manager._kernels.keys())}")

        return cast(str, kernel_id)
