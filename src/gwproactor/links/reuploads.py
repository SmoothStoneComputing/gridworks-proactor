"""A 'reupload' is a process of uploading events that have not yet been acked when communication with the upstream
peer is re-activated. The re-upload proceeds slowly by receiving acks so as not to overwhelm processing queues. Events
that occur after communication has been reactivated and/or during a re-upload are not part of the reupload, but are sent
as they occur.

This module provides a class, Reuploads, for tracking which events are part of the reupload.

This module only manages events ids; it does not change (add or remove) event storage.
"""

from typing import Optional

from gwproactor.logger import ProactorLogger
from gwproactor.stats import LinkStats


class _ReuploadDiffLogger:  # pragma: no cover
    """Helper class for logging results of an ack without to much logging code bulk in the ack processing routine"""

    reuploads: Optional["Reuploads"] = None
    event_id: str = ""
    verbose: bool = False
    begin_reuploading: bool = False
    begin_num_unacked: int = -1
    begin_num_pending: int = -1
    begin_verbose_str: str = ""

    def init(
        self,
        reuploads: Optional["Reuploads"] = None,
        *,
        verbose: bool = False,
    ) -> None:
        self.reuploads = reuploads
        self.verbose = verbose
        if reuploads is not None:
            self.begin_reuploading = reuploads.reuploading()
            self.begin_num_unacked = reuploads.num_reuploaded_unacked
            self.begin_num_pending = reuploads.num_reupload_pending
            if verbose:
                self.begin_verbose_str = reuploads.get_str(num_events=5)

    def diff_str(self, path_dbg: int) -> str:
        s = ""
        if self.reuploads is not None:
            if self.verbose:
                s += f"Begin reuploads:\n{self.begin_verbose_str}\n"
                s += f"End reuploads:\n{self.reuploads.get_str(num_events=5)}\n"
            s += (
                f"path:0x{path_dbg:08X}  "
                f"reuploading: {int(self.begin_reuploading)} -> {int(self.reuploads.reuploading())}  "
                f"unacked: {self.begin_num_unacked} -> {self.reuploads.num_reuploaded_unacked}  "
                f"pending: {self.begin_num_pending} -> {self.reuploads.num_reupload_pending}  "
            )
        return s

    def ack_str(self, path_dbg: int) -> str:
        return f"--process_ack_for_reupload  {self.diff_str(path_dbg)}"

    def log_ack(self, path_dbg: int) -> None:
        if self.reuploads is not None and self.reuploads.logger.path_enabled:
            self.reuploads.logger.path(self.ack_str(path_dbg))


class Reuploads:
    """Track event uids that are part of a re-upload, both those that have not yet been sent (pending) and those that are
    "in-flight", (unacked). Upon ack, update records and return next message to be sent.
    """

    NUM_INITIAL_EVENTS: int = 5
    """Default number of events to send when re-upload starts."""

    _reupload_pending: dict[str, None]
    """'Ordered set' of *unsent* events that are part of this reupload.
    A dict is used to provide insertion order and fast lookup."""

    _reuploaded_unacked: dict[str, None]
    """'Ordered set' of *sent but as-yet unacked*
    A dict is used to provide insertion order and fast lookup."""

    _num_initial_events: int
    """Number of events to send when re-upload starts."""

    stats: Optional[LinkStats] = None
    """Object into which we can record reupload start and complete. Set during
    LinkManager.start() since upstream client does not exist during LinkManager
    construction. """

    _logger: ProactorLogger

    def __init__(
        self,
        logger: ProactorLogger,
        num_initial_events: int = NUM_INITIAL_EVENTS,
    ) -> None:
        self._reupload_pending = {}
        self._reuploaded_unacked = {}
        self._num_initial_events = num_initial_events
        self._logger = logger

    def start_reupload(self, pending_events: list[str]) -> list[str]:
        """Track all pending_events for reupload. Of the pending_events,
        record the first _num_initial_events as "unacked" and the rest
        as "pending". Return the "unacked" group so they can be sent.
        """
        reupload_now = pending_events[: self._num_initial_events]
        self._reuploaded_unacked = dict.fromkeys(reupload_now)
        self._reupload_pending = dict.fromkeys(
            pending_events[self._num_initial_events :]
        )
        if self.reuploading() and self.stats is not None:
            self.stats.start_reupload()
        self._log_start_reupload(len(pending_events), len(reupload_now))
        return reupload_now

    def clear_unacked_event(self, ack_id: str) -> None:
        self._reuploaded_unacked.pop(ack_id)

    def process_ack_for_reupload(self, ack_id: str) -> list[str]:
        """If ack_id is in our "unacked" store, remove it from the unacked store. If any events remain in our "pending"
        store, move the first pending event from the pending store to the unacked store and return it for sending
        next."""

        path_dbg = 0
        was_reuploading = self.reuploading()
        ack_logger = _ReuploadDiffLogger()
        if self._logger.path_enabled:
            ack_logger.init(self, verbose=True)
        reupload_now = []
        if ack_id in self._reuploaded_unacked:
            path_dbg |= 0x00000001
            self._reuploaded_unacked.pop(ack_id)
            if self._reupload_pending:
                path_dbg |= 0x00000002
                reupload_next = next(iter(self._reupload_pending))
                self._reupload_pending.pop(reupload_next)
                self._reuploaded_unacked[reupload_next] = None
                reupload_now = [reupload_next]
        # This case is likely in testing (which explicitly generates
        # the awaiting_setup state), but unlikely in the real works, since
        # unless we have many subscriptions we will get one suback for all of
        # them, meaning we can't enter the awaiting_setup state.
        # In awaiting_setup, this case is likely since we generate an event for
        # the suback before we start the reupload - we generate the event, send it,
        # then add it to the reupload. Because old events are sent first in the reupload,
        # the suback event is likely to be in pending, not in unacked.
        # In theory this could also happen if an ack for an event sent prior to
        # communication loss was somehow preserved in a queue and delivered after comm
        # restore.
        elif ack_id in self._reupload_pending:
            path_dbg |= 0x00000004
            self._reupload_pending.pop(ack_id)
        if was_reuploading and not self.reuploading() and self.stats is not None:
            path_dbg |= 0x00000008
            self.stats.complete_reupload()
            self._logger.comm_event(
                f"Reupload completed. Reuploads started: {self.stats.reupload_counts.started}  "
                f"completed: {self.stats.reupload_counts.completed}."
            )
        if self._logger.path_enabled:
            ack_logger.log_ack(path_dbg)
        return reupload_now

    @property
    def num_reupload_pending(self) -> int:
        return len(self._reupload_pending)

    @property
    def num_reuploaded_unacked(self) -> int:
        return len(self._reuploaded_unacked)

    @property
    def logger(self) -> ProactorLogger:
        return self._logger

    def reuploading(self) -> bool:
        return bool(len(self._reuploaded_unacked) + len(self._reupload_pending))

    def clear(self) -> None:
        self._reupload_pending.clear()
        self._reuploaded_unacked.clear()

    def get_str(self, *, verbose: bool = True, num_events: int = 5) -> str:
        s = f"Reuploads  reuploading:{int(self.reuploading())}  unacked/sent:{len(self._reuploaded_unacked)}  pending/unsent:{len(self._reupload_pending)}"
        if verbose:
            s += f"  num initial:{self._num_initial_events}\n"
            s += f"  unacked:{len(self._reuploaded_unacked)}\n"
            for message_id in self._reuploaded_unacked:
                s += f"    {message_id[:8]}...\n"
            s += f"  pending:{len(self._reupload_pending)}\n"
            for i, message_id in enumerate(self._reupload_pending):
                s += f"    {message_id[:8]}...\n"
                if i == num_events - 1:
                    break
        return s.rstrip()

    def __str__(self) -> str:
        return self.get_str(verbose=False)

    def _log_start_reupload(
        self, num_pending_events: int, num_reupload_now: int
    ) -> None:
        if self._logger.comm_event_enabled:
            if self.reuploading():
                state_str = f"{self.num_reupload_pending} reupload events pending."
            else:
                state_str = "reupload complete."
            s = (
                f"start_reupload: sent {num_reupload_now} events. "  # noqa: G004
                f"{state_str} "
                f"Total events in reupload: {num_pending_events}.  "
            )
            if self.stats is not None:
                s += (
                    f"Reuploads started: {self.stats.reupload_counts.started}  "
                    f"completed: {self.stats.reupload_counts.completed}."
                )
            self._logger.comm_event(s)
        if self._logger.path_enabled:
            self._logger.path(self.get_str(num_events=5))
