from jupyter_server.services.sessions.sessionmanager import SessionManager, KernelSessionRecord

from typing import Any, Dict, List, NewType, Optional, Union, cast

KernelName = NewType("KernelName", str)
ModelName = NewType("ModelName", str)

class DistributedSessionManager(SessionManager):
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
    ) -> Dict[str, Any]:
        """Creates a session and returns its model

        Parameters
        ----------
        name: ModelName(str)
            Usually the model name, like the filename associated with current
            kernel.
        """
        self.log.info("Creating a new Session. Path=%s, Name=%s, Type=%s, KernelName=%s, KernelId=%s" % (path, name, type, kernel_name, kernel_id))
        session_id = self.new_session_id()
        self.log.info("Created SessionID: %s" % session_id)
        record = KernelSessionRecord(session_id=session_id)
        self._pending_sessions.update(record)
        if kernel_id is not None and kernel_id in self.kernel_manager:
            pass
        else:
            kernel_id = await self.start_kernel_for_session(
                session_id, path, name, type, kernel_name
            )
        record.kernel_id = kernel_id
        self._pending_sessions.update(record)
        result = await self.save_session(
            session_id, path=path, name=name, type=type, kernel_id=kernel_id
        )
        self._pending_sessions.remove(record)
        return cast(Dict[str, Any], result)