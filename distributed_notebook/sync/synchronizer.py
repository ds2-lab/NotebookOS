import asyncio
import logging
import sys
import traceback
import types
from typing import Optional

from .ast import SyncAST
from .election import Election
from .errors import SyncError, DiscardMessageError
from .log import Checkpointer, SyncLog, SynchronizedValue, KEY_SYNC_END
from .object import SyncObject, SyncObjectWrapper, SyncObjectMeta
from .referer import SyncReferer
from .syncpointer import DatasetPointer, ModelPointer
from ..datasets.base import Dataset
from ..demo.script.training import model
from ..logging import ColoredLogFormatter
from ..models.model import DeepLearningModel

KEY_SYNC_AST = "_ast_"
CHECKPOINT_AUTO = 1
CHECKPOINT_ON_CHANGE = 2
MIN_CHECKPOINT_LOGS = 10


class SyncModule(object):
    """A dummy module used for Synchronizer for customizing __dict__"""
    __spec__ = None


class Synchronizer:
    _ast: SyncAST
    _module: types.ModuleType
    _async_loop: asyncio.AbstractEventLoop

    def __init__(self, sync_log: SyncLog, module: Optional[types.ModuleType] = None, ns=None, opts=0, node_id: int = -1):
        if module is None and ns is not None:
            self._module = SyncModule()  # type: ignore
            ns.setdefault("__name__", "__main__")
            self._module.__dict__ = ns
        elif module is None:
            self._module = types.ModuleType("__main__", doc="Automatically created module for python environment")
        else:
            self._module = module

        self._node_id:int = node_id

        # Set callbacks for synclog
        sync_log.set_should_checkpoint_callback(self.should_checkpoint_callback)
        sync_log.set_checkpoint_callback(self.checkpoint_callback)

        self._log = logging.getLogger(__class__.__name__)
        self._log.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ColoredLogFormatter())
        self._log.addHandler(ch)

        self._log.debug("Finished setting callbacks for Synclog (within Synchronizer).")

        self._async_loop = asyncio.get_running_loop()

        self._log.debug("Got asyncio io loop")

        self._tags = {}
        self._ast = SyncAST()
        self._log.debug("Created SyncAST")
        self._referer = SyncReferer()
        self._log.debug("Created SyncReferer")
        self._opts = opts
        self._syncing = False  # Avoid checkpoint in the middle of syncing.

        self._synclog: SyncLog = sync_log
        self._log.debug("Finished creating Synchronizer")

    def start(self):
        self._log.debug("Starting Synchronizer")
        self._async_loop = asyncio.get_running_loop()
        self._synclog.start(self.change_handler)

    def close(self):
        pass

    @property
    def module(self):
        return self._module

    @property
    def global_ns(self):
        return self._module.__dict__

    @property
    def execution_count(self) -> int:
        return self._ast.execution_count

    def fast_forward_execution_count(self):
        prev_exec_count: int = self._ast.execution_count
        self._ast.fast_forward_executions()

        self._log.debug(f"Fast-forwarded execution count by 1 (from {prev_exec_count} to {self._ast.execution_count}).")

    def set_execution_count(self, execution_count):
        """
        Set the execution count of the Synchronizer to the given value.

        The provided value must be greater than the current value, or the value will not be set.
        """
        prev_exec_count: int = self._ast.execution_count

        if execution_count < prev_exec_count:
            raise ValueError(f"cannot set execution count to a lower value (current: {execution_count}, specified: {prev_exec_count})")

        self._ast.set_executions(execution_count)
        self._log.debug(f"Fast-forwarded execution count by {execution_count - prev_exec_count} (from {prev_exec_count} to {self._ast.execution_count}).")

    def change_handler(self, val: SynchronizedValue, restoring: bool = False):
        """Change handler"""
        ## TODO: Buffer changes of one execution and apply changes atomically
        if not val.should_end_execution:
            if not self._syncing:
                self._log.debug(">> enter execution syncing...")
                self._syncing = True
        elif val.key == KEY_SYNC_END:
            self._syncing = False
            self._log.debug("<< exit execution syncing [1]")
            return

        try:
            # self._log.debug("Updating: \"{}\", ended: {}".format(val.key, val.should_end_execution))
            # self._log.debug(f"Updating: {val}")
            existed: Optional[SyncObject] = None
            if val.key == KEY_SYNC_AST:
                existed = self._ast
            elif val.key in self._tags:
                existed = self._tags[val.key]

            if existed is None:
                if isinstance(val.data, SyncObject):
                    existed = val.data
                else:
                    existed = SyncObjectWrapper(self._referer)  # Initialize an empty object wrapper.

            # Switch context
            old_main_modules = sys.modules["__main__"]
            sys.modules["__main__"] = self._module

            self._log.debug(f"Restoring SynchronizedValue with key=\"{val.key}\": {val}")
            diff = existed.update(val)
            self._log.debug("{}:{}".format(val.key, type(diff)))

            sys.modules["__main__"] = old_main_modules
            # End of switch context

            if val.key == KEY_SYNC_AST:
                assert isinstance(existed, SyncAST)
                old_exec_count: int = -1
                if self._ast is not None:
                    old_exec_count = self._ast.execution_count
                self._ast = existed

                if self.execution_count != old_exec_count:
                    self._log.debug(f"Execution count changed from {old_exec_count} to {self.execution_count} after synchronizing AST.")

                # Redeclare modules, classes, and functions.
                try:
                    compiled = compile(diff, "sync", "exec")
                except Exception as ex:
                    self._log.error(f"Failed to compile. Diff ({type(diff)}): {diff}. Error: {ex}")
                    raise ex  # Re-raise.

                try:
                    exec(compiled, self.global_ns, self.global_ns)
                except Exception as ex:
                    self._log.error(f"Failed to exec. Compiled ({type(compiled)}): {compiled}. Error: {ex}")
                    raise ex  # Re-raise.
            else:
                assert isinstance(existed, SyncObjectWrapper)
                assert val.key is not None
                self._tags[val.key] = existed
                self.global_ns[val.key] = existed.object

            if val.should_end_execution:
                self._syncing = False
                self._log.debug("<< exit execution syncing [2]")
        except Exception as e:
            # print_trace(limit = 10)
            self._log.error("Exception encountered in change handler for synchronizer: %s" % str(e))
            tb: list[str] = traceback.format_exception(e)
            for frame in tb:
                self._log.error(frame)

        local_election: Election = self.current_election
        if local_election is not None and local_election.term_number < self.execution_count:
            self._log.warning(f"Current local election has term number {local_election.term_number}, "
                              f"but we (now) have execution count of {self.execution_count}. We're out-of-sync...")

    async def propose_lead(self, jupyter_message_id: str, term_number: int) -> int:
        """Propose to lead the next execution.

        Wait the ready of the synchronization and propose to lead a execution.
        Returns the execution count that granted to lead or 0 if denied.
        Note that if the execution_count is 0, the execution is guaranteed to be
        granted, which may cause duplication execution.
        """
        self._log.debug("Synchronizer is proposing to lead term %d" % term_number)
        try:
            # Propose to lead specified term.
            # Term 0 tries to lead the next term whatever and will always success.
            if await self._synclog.try_lead_execution(jupyter_message_id, term_number):
                self._log.debug("We won the election to lead term %d" % term_number)
                # Synchronized, execution_count was updated to last execution.
                self._async_loop = asyncio.get_running_loop()  # Update async_loop.
                return self._synclog.term
        except SyncError as se:
            self._log.warning("SyncError: {}".format(se))
            # print_trace(limit = 10)
            stack: list[str] = traceback.format_exception(se)
            for frame in stack:
                self._log.error(frame)
        except DiscardMessageError as dme:
            self._log.warning(f"Received direction to discard Jupyter Message {jupyter_message_id}, "
                              f"as election for term {term_number} was skipped: {dme}")
            raise dme
        except Exception as e:
            self._log.error("Exception encountered while proposing LEAD: %s" % str(e))
            # print_trace(limit = 10)
            stack: list[str] = traceback.format_exception(e)
            for frame in stack:
                self._log.error(frame)
            raise e

        self._log.debug("We lost the election to lead term %d" % term_number)
        # Failed to lead the term
        return 0

    def created_first_election(self)->bool:
        """
        :return: return a boolean indicating whether we've created the first election yet.
        """
        if self._synclog is None:
            raise ValueError("Cannot check if we've created first election, as the SyncLog is None...")

        return self._synclog.created_first_election

    @property
    def current_election(self)->Election:
        """
        :return: the current election, if one exists.
        """
        if self._synclog is None:
            return None

        return self._synclog.current_election

    def get_known_election_terms(self)->list[int]:
        """
        :return: a list of term numbers for which we have an associated Election object
        """
        return self._synclog.get_known_election_terms()

    def get_election(self, term_number:int):
        """
        :return: return the election with the given term number, or None if no such election exists.
        """
        return self._synclog.get_election(term_number)

    def is_election_finished(self, term_number: int)->bool:
        """
        Checks if the election with the specified term number has completed.

        Raises a ValueError if there is no election with the specified term number or if the sync log is None.
        """
        if self._synclog is None:
            raise ValueError(f"Cannot check status of election {term_number}; SynchronizationLog is None.")

        election:Election = self.get_election(term_number)

        if election is None:
            raise ValueError(f"Could not find election with term number {term_number}")

        voting_done: bool = election.voting_phase_completed_successfully
        code_executed: bool = election.code_execution_completed_successfully
        return (voting_done and code_executed) or election.is_in_failed_state

    async def propose_yield(self, jupyter_message_id: str, term_number: int) -> int:
        """Propose to yield the next execution to another replica.

        Wait the ready of the synchronization and propose to lead a execution.
        Returns the execution count that granted to lead or 0 if denied.
        Note that if the execution_count is 0, the execution is guaranteed to be
        granted, which may cause duplication execution.
        """
        self._log.debug("Synchronizer is proposing to yield term %d" % term_number)
        try:
            if await self._synclog.try_yield_execution(jupyter_message_id, term_number):
                self._log.error("synclog.yield_exection returned true despite the fact that we're yielding...")
                raise ValueError("synclog.yield_exection returned true despite the fact that we're yielding")
        except SyncError as se:
            self._log.warning("SyncError: {}".format(se))
            # print_trace(limit = 10)
            stack: list[str] = traceback.format_exception(se)
            for frame in stack:
                self._log.error(frame)
        except Exception as e:
            self._log.error("Exception encountered while proposing YIELD: %s" % str(e))
            # print_trace(limit = 10)
            stack: list[str] = traceback.format_exception(e)
            for frame in stack:
                self._log.error(frame)
            raise e

        self._log.debug("Successfully yielded the execution to another replica for term %d" % term_number)
        # Failed to lead the term, which is what we want to happen since we're YIELDING.
        return 0

    async def wait_for_election_to_end(self, term_number: int):
        """
        Wait until the leader of the specified election finishes executing the code,
        or until we know that all replicas yielded.

        :param term_number: the term number of the election
        """
        self._log.debug(f"Waiting for leader to finish executing code (or to learn that all replicas yielded) "
                        f"for election term {term_number}.")

        await self._synclog.wait_for_election_to_end(term_number)

    async def notify_execution_complete(self, term_number: int):
        """
        Notify our peer replicas that we have finished executing the code for the specified election.

        :param term_number: the term of the election for which we served as leader and executed
        the user-submitted code.
        """
        self._log.debug(f"Notifying peers that execution of code during election term {term_number} has finished.")

        await self._synclog.notify_execution_complete(term_number)

    async def ready(self, jupyter_message_id: str, term_number: int, lead: bool) -> int:
        """
        Wait for the replicas to synchronize and propose a leader for an election.
        Returns the execution count that granted to lead or 0 if denied.
        Note that if the execution_count is 0, the execution is guaranteed to be
        granted, which may cause duplication execution.

        Pass 'True' for the 'lead' parameter to propose LEAD.
        Pass 'False' for the 'lead' parameter to propose YIELD.
        """
        if term_number < 0:
            return 0

        if lead:
            self._log.debug("Synchronizer::Ready(LEAD): Proposing to lead now.")
            res = await self.propose_lead(jupyter_message_id, term_number)
            self._log.debug("Synchronizer::Ready(LEAD): Done with proposal protocol for lead. Result: %d" % res)
            return res
        else:
            self._log.debug("Synchronizer::Ready(YIELD): Proposing to yield now.")
            res = await self.propose_yield(jupyter_message_id, term_number)
            self._log.debug("Synchronizer::Ready(YIELD): Done with proposal protocol for yield. Result: %d" % res)
            return res

    async def sync(self, execution_ast, source: Optional[str] = None, checkpointer: Optional[Checkpointer] = None)->bool:
        """
        Note: `execution_ast` may be None if the user's code had a syntax error.
        TODO(Ben): See what happens if there are other errors, such as dividing by zero or array out-of-bounds.
        """
        self._log.debug("Synchronizing execution AST: %s" % str(execution_ast))
        sync_log = self._synclog
        checkpointing = checkpointer is not None
        if checkpointing:
            sync_log = checkpointer

        try:
            sync_ast: Optional[SynchronizedValue]
            if checkpointing:
                sync_ast = self._ast.dump(meta=source)
            else:
                sync_ast = self._ast.diff(execution_ast, meta=source)
                assert sync_ast is not None
            # execution_count updated.
            # self._referer.module_id = self._ast.execution_count # TODO: Verify this.

            self._log.debug(f"Syncing execution (checkpointing={checkpointing}). AST: {sync_ast}. Current execution count: {self.execution_count}.")
            keys = self._ast.globals
            meta = SyncObjectMeta(
                batch=(str(sync_ast.election_term) if not checkpointing else "{}c".format(sync_ast.election_term)))
            # TODO: Recalculate the number of expected synchronizations within the execution.
            expected = len(keys)  # globals + the ast
            synced = 0

            self._syncing = True
            self._log.debug(f"Setting sync_ast.term to term of AST: {self._ast.execution_count}")
            sync_ast.set_election_term(self._ast.execution_count)
            sync_ast.set_key(KEY_SYNC_AST)
            if expected == 0:
                sync_ast.set_should_end_execution(expected == 0)
                # Because should_checkpoint_callback will be called during final append call,
                # set the end of _syncing before the final append call.
                self._syncing = False

            sync_ast.set_proposer_id(self._node_id)

            # current_election: Election = self._synclog.current_election
            # if current_election is not None:
            #     sync_ast.set_election_term(current_election.term_number)
            #     sync_ast.set_attempt_number(current_election.current_attempt_number)

            self._log.debug(f"Appending value: {sync_ast}. Checkpointing={checkpointing}.")
            await sync_log.append(sync_ast)
            self._log.debug(f"Successfully appended value: {sync_ast}. Checkpointing={checkpointing}.")
            for key in keys:
                synced = synced + 1
                self._log.debug("Syncing key \"%s\" now." % key)
                await self.sync_key(sync_log, key, self.global_ns[key],
                                    end_execution=synced == expected,
                                    checkpointing=checkpointing, meta=meta)
                self._log.debug("Successfully synchronized key \"%s\"." % key)

            if checkpointing:
                checkpointer.close()
        except SyncError as se:
            tb = traceback.format_exc()
            self._log.error("SyncError: {}".format(se))
            self._log.error(tb)
            return False
        except Exception as e:
            tb = traceback.format_exc()
            self._log.error("Exception Encountered: %s" % str(e))
            self._log.error(tb)
            return False

        return True

    async def sync_key(self, sync_log, key, val, end_execution=False, checkpointing=False, meta=None):
        if key in self._tags:
            existed = self._tags[key]
        else:
            if checkpointing:
                self._log.error(f"Key {key} is not in self._tags ({self._tags}). Checkpointing should be False...")

            # TODO: Add support to SyncObject factory
            existed = SyncObjectWrapper(self._referer)
            self._tags[key] = existed

        # self._log.debug("Syncing {}...".format(key))

        # Switch context
        old_main_modules = sys.modules["__main__"]
        sys.modules["__main__"] = self._module

        if isinstance(val, Dataset):
            self._log.debug(f"Synchronizing Dataset \"{val.name}\". Will convert to pointer before appending to RaftLog.")
            dataset_pointer: DatasetPointer = DatasetPointer(dataset = val)
            val = dataset_pointer
        elif isinstance(val, DeepLearningModel):
            self._log.debug(f"Synchronizing Model \"{val.name}\". Will convert to pointer before appending to RaftLog.")
            model_pointer: ModelPointer = ModelPointer(deep_learning_model = val)
            val = model_pointer

        if checkpointing:
            sync_val = existed.dump(meta=meta)
        else:
            # On checkpointing, the SyncObject must have been available in tags.
            # Get start time of the execution.
            # start_time = time.time()
            sync_val = existed.diff(val, meta=meta)
            # Print time elapsed.
            # self._log.debug("Time elapsed in diff: {}".format(time.time() - start_time))

        sys.modules["__main__"] = old_main_modules
        # End of switch context

        # Because should_checkpoint_callback will be called during final append call,
        # set the end of _syncing before the final append call.
        if end_execution:
            self._syncing = False

        if sync_val is not None:
            sync_val.set_election_term(self._ast.execution_count)
            sync_val.set_key(key)
            sync_val.set_should_end_execution(end_execution)
            await sync_log.append(sync_val)
        elif end_execution:
            # Synthesize end
            await sync_log.append(
                SynchronizedValue(None, None, election_term=self._ast.execution_count, should_end_execution=True,
                                  key=KEY_SYNC_END, proposer_id=self._node_id))

    def should_checkpoint_callback(self, sync_log: SyncLog) -> bool:
        cp = False
        if self.execution_count < 2 or self._syncing or sync_log.num_changes < MIN_CHECKPOINT_LOGS:
            pass
        else:
            cp = (((self._opts & CHECKPOINT_AUTO) > 0 and sync_log.num_changes >= len(self._tags.keys())) or
                  ((self._opts & CHECKPOINT_ON_CHANGE) > 0 and sync_log.num_changes > 0))
        # self._log.debug("in should_checkpoint_callback: {}".format(cp))
        return cp

    def checkpoint_callback(self, checkpointer: Checkpointer) -> None:
        self._log.debug("Checkpointing...")
        checkpointer.lead(self._ast.execution_count)
        # await self.sync(None, source="checkpoint", checkpointer=checkpointer)
        # checkpointer.close()
        asyncio.run_coroutine_threadsafe(self.sync(None, source="checkpoint", checkpointer=checkpointer),
                                         self._async_loop)
