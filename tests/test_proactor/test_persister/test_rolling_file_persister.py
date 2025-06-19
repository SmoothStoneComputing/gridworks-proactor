# ruff: noqa: PLR2004
# mypy: disable-error-code="union-attr"

import datetime
import json
import shutil
import time
from pathlib import Path
from typing import Any, Optional, Union

import gwproto.messages
from freezegun import freeze_time
from gwproto.messages import ProblemEvent
from result import Result

from gwproactor import AppSettings, ExternalWatchdogCommandBuilder, Problems
from gwproactor.persister import (
    FileExistedWarning,
    FileMissing,
    FileMissingWarning,
    PersisterError,
    ReindexError,
    TimedRollingFilePersister,
    TrimFailed,
    UIDExistedWarning,
    UIDMissingWarning,
)
from gwproactor.persister.timed_rolling_file import _PersistedItem  # noqa


def _day_start(dt: datetime.datetime) -> datetime.datetime:
    return datetime.datetime(
        year=dt.year,
        month=dt.month,
        day=dt.day,
        tzinfo=dt.tzinfo,
    )


def _today() -> datetime.datetime:
    return _day_start(datetime.datetime.now(tz=datetime.timezone.utc))


def _yesterday() -> datetime.datetime:
    return _day_start(_today() - datetime.timedelta(seconds=1))


def _tomorrow() -> datetime.datetime:
    return _day_start(_today() + datetime.timedelta(days=1))


class PatWatchdogWithFile(ExternalWatchdogCommandBuilder):
    pat_dir: Path
    """Explicitly set this path on the class before running tests"""

    @classmethod
    def default_pat_args(cls, pid: Optional[int] = None) -> list[str]:  # noqa: ARG003
        pat_code = "from pathlib import Path; import time; p = Path(f'"
        pat_code += str(cls.pat_dir / "pat")
        pat_code += "{time.time()}.txt'); p.open('w')"
        return ["python", "-c", pat_code]


class SlowIndexer(TimedRollingFilePersister):
    SLOW_REINDEX_PAT_SECONDS: float = 0.1

    def __init__(
        self,
        base_dir: Path | str,
        max_bytes: int = TimedRollingFilePersister.DEFAULT_MAX_BYTES,
        pat_watchdog_args: Optional[list[str]] = None,
    ) -> None:
        super().__init__(
            base_dir=base_dir,
            max_bytes=max_bytes,
            pat_watchdog_args=pat_watchdog_args,
            reindex_pat_seconds=self.SLOW_REINDEX_PAT_SECONDS,
        )

    @classmethod
    def _persisted_item_from_file_path(cls, filepath: Path) -> Optional[_PersistedItem]:
        sleep_time = cls.SLOW_REINDEX_PAT_SECONDS * 1.1
        time.sleep(sleep_time)
        return super()._persisted_item_from_file_path(filepath)


def assert_contents(
    p: TimedRollingFilePersister,
    uids: Optional[list[str]] = None,
    exp_paths: Optional[list[Path | str]] = None,
    nearby_days: bool = True,
    possible_days: Optional[list[datetime.datetime]] = None,
    exact_days: Optional[list[datetime.datetime]] = None,
    num_pending: Optional[int] = None,
    curr_bytes: Optional[int] = None,
    curr_dir: Optional[Union[str, Path]] = None,
    check_index: bool = True,
    max_bytes: Optional[int] = None,
    num_persists: Optional[int] = None,
    num_retrieves: Optional[int] = None,
    num_clears: Optional[int] = None,
) -> None:
    assert p.num_pending == len(p.pending_ids())
    if num_pending is not None:
        assert p.num_pending == num_pending
    if curr_bytes is not None:
        assert p.curr_bytes == curr_bytes
    if curr_dir is None and exact_days:
        curr_dir = exact_days[-1].isoformat()
    if curr_dir is not None:
        if isinstance(curr_dir, str):
            assert p.curr_dir.name == curr_dir
        else:
            assert p.curr_dir == curr_dir
    if max_bytes is not None:
        assert p.max_bytes == max_bytes
    if uids is not None:
        str_uids = [str(uid) for uid in uids]
        assert p.pending_ids() == str_uids
        assert p.num_pending == len(str_uids)
        for str_uid in str_uids:
            assert str_uid in p
    got_paths = [p.get_path(uid) for uid in p.pending_ids()]
    if exp_paths is not None:
        assert exp_paths == got_paths
    if nearby_days:
        if possible_days is None:
            possible_days = []
        possible_days.extend([_yesterday(), _today(), _tomorrow()])
    if exact_days is not None:
        possible_days = exact_days
    if possible_days is not None and got_paths:
        possible_days_str_list = [dt.isoformat() for dt in possible_days]
        got_day_strs_list = [path.parent.name for path in got_paths]
        if exact_days:
            assert possible_days_str_list == got_day_strs_list
            assert p.curr_dir.name == possible_days_str_list[-1]
            dirs_exp = sorted(set(possible_days_str_list))
            dirs_got = sorted(
                [path.name for path in p.base_dir.iterdir() if path.is_dir()]
            )
            assert dirs_exp == dirs_got
        else:
            possible_day_strs = set(possible_days_str_list)
            got_day_strs = set(got_day_strs_list)
            assert got_day_strs.issubset(
                possible_day_strs
            ), f"\ngot: {got_day_strs}\nexp: {possible_day_strs}"
            assert p.curr_dir.name in possible_days_str_list
    if num_persists is not None:
        assert p.num_persists == num_persists
    if num_retrieves is not None:
        assert p.num_retrieves == num_retrieves
    if num_clears is not None:
        assert p.num_clears == num_clears
    if check_index:
        p2 = TimedRollingFilePersister(
            base_dir=p.base_dir,
            max_bytes=p.max_bytes,
        )
        assert p2.reindex().is_ok()
        if p.num_pending == 0:
            assert p.curr_bytes == 0
        assert p.pending_ids() == p2.pending_ids()
        # noinspection PyProtectedMember
        assert p.pending_ids() == list(p.pending_ids())
        # noinspection PyProtectedMember
        assert got_paths == list(p.pending_paths())
        for uid, path in zip(p.pending_ids(), got_paths):
            assert path.exists()
            assert uid in str(path.name)


def test_persister_happy_path(tmp_path: Path) -> None:
    settings = AppSettings()
    settings.paths.mkdirs()
    event = ProblemEvent(
        Src="foo",
        ProblemType=gwproto.messages.Problems.error,
        Summary="Problems, I've got a few",
        Details="Too numerous to name",
    )
    event_bytes = event.model_dump_json().encode()

    # empty persister
    persister = TimedRollingFilePersister(settings.paths.event_dir)
    assert persister.reindex().is_ok()
    assert persister.get_path("foo") is None
    assert_contents(
        persister,
        num_pending=0,
        max_bytes=TimedRollingFilePersister.DEFAULT_MAX_BYTES,
        curr_bytes=0,
        num_persists=0,
        num_retrieves=0,
        num_clears=0,
    )

    # add one
    result = persister.persist(event.MessageId, event_bytes)
    assert result.is_ok()
    assert_contents(
        persister,
        uids=[event.MessageId],
        num_pending=1,
        curr_bytes=len(event_bytes),
        num_persists=1,
        num_retrieves=0,
        num_clears=0,
    )

    # retrieve
    retrieved = persister.retrieve(event.MessageId)
    assert retrieved.is_ok(), str(retrieved)
    assert retrieved.value == event_bytes
    assert persister.num_retrieves == 1

    # deserialize
    loaded = json.loads(retrieved.value.decode("utf-8"))
    assert loaded == json.loads(event.model_dump_json())
    assert isinstance(retrieved.value, bytes)
    loaded_event = ProblemEvent.model_validate_json(retrieved.value)
    assert loaded_event == event

    # add another
    event2 = ProblemEvent(
        Src="foo",
        Summary="maybe not great",
        ProblemType=gwproto.messages.Problems.warning,
    )
    event2_bytes = event2.model_dump_json().encode()
    result = persister.persist(event2.MessageId, event2.model_dump_json().encode())
    assert result.is_ok()
    assert_contents(
        persister,
        uids=[event.MessageId, event2.MessageId],
        num_pending=2,
        curr_bytes=len(event_bytes) + len(event2_bytes),
        num_persists=2,
        num_retrieves=1,
        num_clears=0,
    )

    # reindex
    assert persister.reindex().is_ok()
    assert_contents(
        persister,
        uids=[event.MessageId, event2.MessageId],
        num_pending=2,
        curr_bytes=len(event_bytes) + len(event2_bytes),
    )

    # clear second one
    cleared = persister.clear(event2.MessageId)
    assert cleared.is_ok()
    assert event2.MessageId not in persister.pending_ids()
    assert persister.get_path(event2.MessageId) is None
    assert_contents(
        persister,
        uids=[event.MessageId],
        num_pending=1,
        curr_bytes=len(event_bytes),
        num_persists=2,
        num_retrieves=1,
        num_clears=1,
    )

    # clear first one
    old_path = persister.get_path(event.MessageId)
    assert old_path.exists()
    cleared = persister.clear(event.MessageId)
    assert cleared.is_ok()
    assert not old_path.exists()
    assert event.MessageId not in persister.pending_ids()
    assert persister.get_path(event.MessageId) is None
    assert_contents(
        persister,
        num_pending=0,
        curr_bytes=0,
        num_persists=2,
        num_retrieves=1,
        num_clears=2,
    )

    # reindex
    assert persister.reindex().is_ok()
    assert len(persister.pending_ids()) == persister.num_pending == 0
    assert persister.curr_dir.name in [
        _today().isoformat(),
        _yesterday().isoformat(),
    ]
    assert persister.curr_bytes == 0
    assert_contents(
        persister,
        num_pending=0,
        curr_bytes=0,
        num_persists=2,
        num_retrieves=1,
        num_clears=2,
    )


def test_persister_max_size() -> None:
    settings = AppSettings()
    settings.paths.mkdirs()
    event = ProblemEvent(
        MessageId=" 0",
        Src=".",
        ProblemType=gwproto.messages.Problems.error,
        Summary="0",
        Details="x" * 1024,
    )

    def inc_event() -> None:
        event.MessageId = f"{int(event.MessageId) + 1:2d}"

    event_bytes = event.model_dump_json().encode()
    num_events_supported = 4
    with freeze_time(_today()):
        # empty persister
        max_bytes = (num_events_supported + 1) * 1000
        p = TimedRollingFilePersister(settings.paths.event_dir, max_bytes=max_bytes)
        assert p.reindex().is_ok()
        assert_contents(p, max_bytes=max_bytes, num_pending=0)

        # a few
        uids = []
        for i in range(1, num_events_supported + 1):
            inc_event()
            uids.append(event.MessageId)
            result = p.persist(event.MessageId, event.model_dump_json().encode())
            assert result.is_ok(), str(result)
            assert_contents(
                p,
                num_pending=i,
                curr_bytes=i * len(event_bytes),
                uids=uids,
                num_persists=i,
                num_retrieves=0,
                num_clears=0,
            )

        # a few more - now size should not change
        for i in range(1, (num_events_supported * 2) + 1):
            inc_event()
            uids.append(event.MessageId)
            uids = uids[1:]
            result = p.persist(event.MessageId, event.model_dump_json().encode())
            assert result.is_ok(), str(result)
            assert_contents(
                p,
                num_pending=num_events_supported,
                curr_bytes=num_events_supported * len(event_bytes),
                uids=uids,
                num_persists=num_events_supported + i,
                num_retrieves=0,
                num_clears=i,
            )
        assert p.num_persists == num_events_supported * 3

        # add a bigger item; more than one must be removed.
        inc_event()
        old_size = len(event_bytes)
        event.Details *= 2
        big_event_bytes = event.model_dump_json().encode()
        exp_size = p.curr_bytes - (2 * old_size) + len(big_event_bytes)
        exp_pending = num_events_supported - 1
        uids.append(event.MessageId)
        uids = uids[2:]
        result = p.persist(event.MessageId, big_event_bytes)
        assert p.num_persists == (num_events_supported * 3) + 1
        assert result.is_ok(), str(result)
        assert_contents(
            p,
            num_pending=exp_pending,
            curr_bytes=exp_size,
            uids=uids,
            num_persists=(num_events_supported * 3) + 1,
            num_retrieves=0,
        )
        assert p.num_clears > (num_events_supported + 1)

        # Cannot add one too large, state of persister doesn't change
        inc_event()
        event.Details = "." * (max_bytes + 1)
        result = p.persist(event.MessageId, event.model_dump_json().encode())
        assert not result.is_ok()
        assert_contents(p, num_pending=exp_pending, curr_bytes=exp_size, uids=uids)
        assert p.num_persists == (num_events_supported * 3) + 2


def test_persister_roll_day() -> None:
    settings = AppSettings()
    settings.paths.mkdirs()
    event = ProblemEvent(
        MessageId=" 0",
        Src=".",
        ProblemType=gwproto.messages.Problems.error,
        Summary="0",
        Details="x" * 1024,
    )

    def inc_event() -> None:
        event.MessageId = f"{int(event.MessageId) + 1:2d}"

    uids = [event.MessageId]
    d1 = _today()
    d2 = d1 + datetime.timedelta(days=1)
    d3 = d2 + datetime.timedelta(days=1)

    with freeze_time(d1):
        exact_days = [d1]
        p = TimedRollingFilePersister(settings.paths.event_dir)
        assert p.reindex().is_ok()
        assert_contents(p, num_pending=0, curr_dir=d1.isoformat())
        result = p.persist(event.MessageId, event.model_dump_json().encode())
        assert result.is_ok()
        assert_contents(p, num_pending=1, uids=uids, exact_days=exact_days)
        assert p.get_path(event.MessageId).parent.name == exact_days[-1].isoformat()

    with freeze_time(d2):
        exact_days.append(d2)
        inc_event()
        uids.append(event.MessageId)
        result = p.persist(event.MessageId, event.model_dump_json().encode())
        assert result.is_ok()
        assert_contents(p, num_pending=2, uids=uids, exact_days=exact_days)
        assert p.get_path(event.MessageId).parent.name == exact_days[-1].isoformat()

    with freeze_time(d3):
        exact_days.append(d3)
        inc_event()
        uids.append(event.MessageId)
        result = p.persist(event.MessageId, event.model_dump_json().encode())
        assert result.is_ok()
        assert_contents(p, num_pending=3, uids=uids, exact_days=exact_days)
        assert p.get_path(event.MessageId).parent.name == exact_days[-1].isoformat()

        # d3 - add another
        exact_days.append(d3)
        inc_event()
        uids.append(event.MessageId)
        result = p.persist(event.MessageId, event.model_dump_json().encode())
        assert result.is_ok()
        assert_contents(p, num_pending=4, uids=uids, exact_days=exact_days)
        assert p.get_path(event.MessageId).parent.name == exact_days[-1].isoformat()

        # verify first day directory is present
        uid = p.pending_ids()[0]
        path = p.get_path(uid)
        day_dir = path.parent
        assert path.exists()
        assert day_dir.exists()

        # clear the first entry from d1, verify d1 is gone
        result = p.clear(uid)
        assert result.ok()
        assert not path.exists()
        assert not day_dir.exists()
        uids = uids[1:]
        exact_days = exact_days[1:]
        assert_contents(p, num_pending=3, uids=uids, exact_days=exact_days)

        # Repeat with d2.
        # Clear the first entry from d2, verify d2 is gone
        uid = p.pending_ids()[0]
        path = p.get_path(uid)
        day_dir = path.parent
        assert path.exists()
        assert day_dir.exists()
        result = p.clear(uid)
        assert result.ok()
        assert not path.exists()
        assert not day_dir.exists()
        uids = uids[1:]
        exact_days = exact_days[1:]
        assert_contents(p, num_pending=2, uids=uids, exact_days=exact_days)

        # clear first entry from third day, verify third day dir still exists
        uid = p.pending_ids()[0]
        path = p.get_path(uid)
        day_dir = path.parent
        assert path.exists()
        assert day_dir.exists()
        result = p.clear(uid)
        assert result.ok()
        assert not path.exists()
        assert day_dir.exists()
        uids = uids[1:]
        assert_contents(p, num_pending=1, uids=uids, exact_days=exact_days[1:])

        uid = p.pending_ids()[0]
        path = p.get_path(uid)
        assert path.exists()
        assert path.parent == day_dir
        result = p.clear(uid)
        assert result.ok()
        assert not path.exists()
        assert not day_dir.exists()
        assert_contents(p, num_pending=0)


def test_persister_size_and_roll() -> None:
    settings = AppSettings()
    settings.paths.mkdirs()
    uidi = 1

    def inc_uid() -> str:
        nonlocal uidi
        uid_ = f"{uidi:2d}"
        uidi += 1
        return uid_

    uids = []
    d1 = _today()
    d2 = d1 + datetime.timedelta(days=1)
    d3 = d2 + datetime.timedelta(days=1)
    d4 = d3 + datetime.timedelta(days=1)
    d5 = d4 + datetime.timedelta(days=1)

    num_supported = 10
    packet_size = 1000
    max_size = num_supported * packet_size
    buf = ("." * packet_size).encode()

    exact_days = []
    # d1, add two
    with freeze_time(d1):
        p = TimedRollingFilePersister(settings.paths.event_dir, max_bytes=max_size)
        assert p.reindex().is_ok()
        assert_contents(p, num_pending=0, curr_dir=d1.isoformat(), max_bytes=max_size)
        for i in range(1, 3):
            uids.append(inc_uid())
            exact_days.append(d1)
            result = p.persist(uids[-1], buf)
            assert result.is_ok()
            assert_contents(
                p,
                num_pending=i,
                curr_bytes=packet_size * i,
                uids=uids,
                exact_days=exact_days,
            )
            assert p.get_path(uids[-1]).parent.name == exact_days[-1].isoformat()

    # d2, add another two
    with freeze_time(d2):
        for i in range(3, 5):
            uids.append(inc_uid())
            exact_days.append(d2)
            result = p.persist(uids[-1], buf)
            assert result.is_ok(), str(result)
            assert_contents(
                p,
                num_pending=i,
                curr_bytes=packet_size * i,
                uids=uids,
                exact_days=exact_days,
            )
            assert p.get_path(uids[-1]).parent.name == exact_days[-1].isoformat()

    # d3, add three
    with freeze_time(d3):
        for i in range(5, 8):
            uids.append(inc_uid())
            exact_days.append(d3)
            result = p.persist(uids[-1], buf)
            assert result.is_ok()
            assert_contents(
                p,
                num_pending=i,
                curr_bytes=packet_size * i,
                uids=uids,
                exact_days=exact_days,
            )
            assert p.get_path(uids[-1]).parent.name == exact_days[-1].isoformat()

    # d4, add two
    with freeze_time(d4):
        for i in range(8, 10):
            uids.append(inc_uid())
            exact_days.append(d4)
            result = p.persist(uids[-1], buf)
            assert result.is_ok()
            assert_contents(
                p,
                num_pending=i,
                curr_bytes=packet_size * i,
                uids=uids,
                exact_days=exact_days,
            )
            assert p.get_path(uids[-1]).parent.name == exact_days[-1].isoformat()

    # d5, add 1
    with freeze_time(d5):
        for i in range(10, 11):
            uids.append(inc_uid())
            exact_days.append(d5)
            result = p.persist(uids[-1], buf)
            assert result.is_ok()
            assert_contents(
                p,
                num_pending=i,
                curr_bytes=packet_size * i,
                uids=uids,
                exact_days=exact_days,
            )
            assert p.get_path(uids[-1]).parent.name == exact_days[-1].isoformat()

        assert p.curr_bytes == p.max_bytes

        # add one more, which keeps size the same but removes first entry. First day dir should remain.
        uid = p.pending_ids()[0]
        path = p.get_path(uid)
        day_dir = path.parent
        assert path.exists()
        assert day_dir.exists()

        uids.append(inc_uid())
        uids = uids[1:]
        exact_days.append(d5)
        exact_days = exact_days[1:]
        result = p.persist(uids[-1], buf)
        assert result.is_ok()
        assert_contents(
            p, num_pending=10, curr_bytes=max_size, uids=uids, exact_days=exact_days
        )
        assert p.get_path(uids[-1]).parent.name == exact_days[-1].isoformat()
        assert not path.exists()
        assert day_dir.exists()

        # add one more, which now should first day dir to be gone.
        uid = p.pending_ids()[0]
        path = p.get_path(uid)
        day_dir = path.parent
        assert path.exists()
        assert day_dir.exists()
        uids.append(inc_uid())
        uids = uids[1:]
        exact_days.append(d5)
        exact_days = exact_days[1:]
        result = p.persist(uids[-1], buf)
        assert result.is_ok()
        assert_contents(
            p, num_pending=10, curr_bytes=max_size, uids=uids, exact_days=exact_days
        )
        assert p.get_path(uids[-1]).parent.name == exact_days[-1].isoformat()
        assert not path.exists()
        assert not day_dir.exists()

        # add a large one, 2x size, which now should remove two entries and second day
        uid0 = p.pending_ids()[0]
        path0 = p.get_path(uid0)
        uid1 = p.pending_ids()[1]
        path1 = p.get_path(uid1)
        day_dir = path0.parent
        assert path1.parent == day_dir
        assert path0.exists()
        assert path1.exists()
        assert day_dir.exists()
        uids.append(inc_uid())
        uids = uids[2:]
        exact_days.append(d5)
        exact_days = exact_days[2:]
        result = p.persist(uids[-1], buf * 2)
        assert result.is_ok()
        assert_contents(
            p, num_pending=9, curr_bytes=max_size, uids=uids, exact_days=exact_days
        )
        assert p.get_path(uids[-1]).parent.name == exact_days[-1].isoformat()
        assert not path0.exists()
        assert not path1.exists()
        assert not day_dir.exists()

        # add a large one, 4x size, which now should third day and one entry of fourth day
        removed_uids = p.pending_ids()[:4]
        paths = [p.get_path(uid) for uid in removed_uids]
        day_dirs = [path.parent for path in paths]
        assert all(path.exists() for path in paths)
        assert all(day_dir.exists() for day_dir in day_dirs)
        uids.append(inc_uid())
        uids = uids[4:]
        exact_days.append(d5)
        exact_days = exact_days[4:]
        result = p.persist(uids[-1], buf * 4)
        assert result.is_ok()
        assert_contents(
            p, num_pending=6, curr_bytes=max_size, uids=uids, exact_days=exact_days
        )
        assert all(not path.exists() for path in paths)
        assert not day_dirs[0].exists()
        assert not day_dirs[1].exists()
        assert not day_dirs[2].exists()
        assert day_dirs[3].exists()

        # clear last entry in fourth day
        uid = p.pending_ids()[0]
        path = p.get_path(uid)
        day_dir = path.parent
        assert path.exists()
        assert day_dir.exists()
        uids = uids[1:]
        exact_days = exact_days[1:]
        exp_size = max_size - packet_size
        result = p.clear(uid)
        assert result.is_ok()
        assert_contents(
            p, num_pending=5, curr_bytes=exp_size, uids=uids, exact_days=exact_days
        )
        assert p.get_path(uids[-1]).parent.name == exact_days[-1].isoformat()
        assert not path.exists()
        assert not day_dir.exists()

        # clear the rest
        num_pending = p.num_pending
        while p.num_pending:
            uid = p.pending_ids()[0]
            path = p.get_path(uid)
            path_size = path.stat().st_size
            assert path.parent == p.curr_dir
            result = p.clear(uid)
            assert result.is_ok()
            num_pending -= 1
            uids = uids[1:]
            exact_days = exact_days[1:]
            exp_size -= path_size
            assert_contents(
                p,
                num_pending=num_pending,
                curr_bytes=exp_size,
                uids=uids,
                exact_days=exact_days,
            )
            assert not path.exists()
            if p.num_pending:
                assert path.parent.exists()
            else:
                assert not path.parent.exists()


def test_persister_indexing() -> None:
    settings = AppSettings()
    settings.paths.mkdirs()
    uidi = 1

    def inc_uid() -> str:
        nonlocal uidi
        uid_ = f"{uidi:2d}"
        uidi += 1
        return uid_

    buf = ("." * 100).encode()
    d1 = _today()
    d2 = d1 + datetime.timedelta(days=1)
    d3 = d2 + datetime.timedelta(days=1)
    d4 = d3 + datetime.timedelta(days=1)

    with freeze_time(d1):
        p = TimedRollingFilePersister(settings.paths.event_dir)
        assert p.reindex().is_ok()
        p.persist(inc_uid(), buf)
        p.persist(inc_uid(), buf)

    with freeze_time(d2):
        p.persist(inc_uid(), buf)
        p.persist(inc_uid(), buf)

    with freeze_time(d3):
        p.persist(inc_uid(), buf)
        p.persist(inc_uid(), buf)

    with freeze_time(d4):
        p.persist(inc_uid(), buf)
        p.persist(inc_uid(), buf)

        index = p.pending_dict()
        p = TimedRollingFilePersister(settings.paths.event_dir)
        assert p.reindex().is_ok()
        assert p.pending_dict() == index

        # removed dir
        shutil.rmtree(p.get_path(p.pending_ids()[0]).parent, ignore_errors=True)
        index.pop(p.pending_ids()[0])
        index.pop(p.pending_ids()[1])
        # removed file
        p.get_path(p.pending_ids()[2]).unlink()
        index.pop(p.pending_ids()[2])
        # removed all files in dir, left dir
        p.get_path(p.pending_ids()[4]).unlink()
        index.pop(p.pending_ids()[4])
        p.get_path(p.pending_ids()[5]).unlink()
        index.pop(p.pending_ids()[5])
        p6 = p.get_path(p.pending_ids()[6])
        assert p6 is not None
        p6_dir = p6.parent
        # invalid file - rgx failure
        shutil.copy(p6, p6_dir / (p6.name + "x"))
        # invalid file - invalid date
        shutil.copy(p6, p6_dir / ("x" + p6.name))
        p = TimedRollingFilePersister(settings.paths.event_dir)
        assert p.reindex().is_ok()
        assert p.pending_dict() == index


def test_persister_problems() -> None:
    settings = AppSettings()
    settings.paths.mkdirs()
    uidi = 1

    def inc_uid() -> str:
        nonlocal uidi
        uid_ = f"{uidi:2d}"
        uidi += 1
        return uid_

    uids = []
    exact_days = []
    buf = ("." * 100).encode()
    d1 = _today()

    with freeze_time(d1):
        p = TimedRollingFilePersister(settings.paths.event_dir)
        assert p.reindex().is_ok()
        uids.append(inc_uid())
        exact_days.append(d1)

        # persist, uid exists
        result = p.persist(uids[-1], buf)
        assert result.is_ok()
        assert_contents(
            p, uids=uids, num_pending=1, curr_bytes=len(buf), exact_days=exact_days
        )
        result = p.persist(uids[-1], buf)
        assert not result.is_ok()
        problems = result.err()
        assert len(problems.errors) == 0
        assert len(problems.warnings) == 2
        assert isinstance(problems.warnings[0], UIDExistedWarning)
        assert isinstance(problems.warnings[1], FileExistedWarning)
        p.get_path(uids[-1]).unlink()
        result = p.persist(uids[-1], buf)
        problems = result.err()
        assert not result.is_ok()
        assert len(problems.errors) == 0
        assert len(problems.warnings) == 2
        assert isinstance(problems.warnings[0], UIDExistedWarning)
        assert isinstance(problems.warnings[1], FileMissingWarning)

        # persist, unexpected error
        class BrokenRoller(TimedRollingFilePersister):
            def _roll_curr_dir(self) -> None:
                raise ValueError("whoops")

        broken = BrokenRoller(settings.paths.event_dir)
        assert broken.reindex().is_ok()
        problems = broken.persist("bla", buf).unwrap_err()
        assert len(problems.errors) == 2
        assert len(problems.warnings) == 0
        assert isinstance(problems.errors[0], ValueError)
        assert isinstance(problems.errors[1], PersisterError)

        # _trim_old_storage, clear exception
        class BrokenRoller2(TimedRollingFilePersister):
            def clear(self, uid: str) -> Result[bool, Problems]:  # noqa: ARG002
                raise ValueError("arg")

        p = BrokenRoller2(settings.paths.event_dir, max_bytes=len(buf) + 50)
        assert p.reindex().is_ok()
        problems = p.persist("xxxbla", buf).unwrap_err()
        assert len(problems.errors) == 3
        assert len(problems.warnings) == 0
        assert isinstance(problems.errors[0], ValueError)
        assert isinstance(problems.errors[1], PersisterError)
        assert isinstance(problems.errors[2], TrimFailed)

        # _trim_old_storage, clear error, file missing
        p = TimedRollingFilePersister(settings.paths.event_dir, max_bytes=len(buf) + 50)
        assert p.num_clears == 0
        assert p.num_retrieves == 0
        assert p.num_persists == 0

        assert p.reindex().is_ok()
        p.get_path(uids[-1]).unlink()
        assert p.num_clears == 0
        assert p.num_retrieves == 0
        assert p.num_persists == 0
        problems = p.persist("xxxbla", buf).unwrap_err()
        assert p.num_clears == 1
        assert p.num_retrieves == 0
        assert p.num_persists == 1
        assert len(problems.errors) == 0
        assert len(problems.warnings) == 1
        assert isinstance(problems.warnings[0], FileMissingWarning)

        # clear, uid missing
        assert p.num_clears == 1
        problems = p.clear("foo").unwrap_err()
        assert len(problems.errors) == 0
        assert len(problems.warnings) == 1
        assert isinstance(problems.warnings[0], UIDMissingWarning)
        assert p.num_clears == 2
        assert p.num_retrieves == 0
        assert p.num_persists == 1

        # retrieve, file missing
        p.persist(uids[-1], buf).expect("")
        p.get_path(uids[-1]).unlink()
        problems = p.retrieve(uids[-1]).unwrap_err()
        assert len(problems.errors) == 1
        assert len(problems.warnings) == 0
        assert isinstance(problems.errors[0], FileMissing)
        assert p.num_clears == 3
        assert p.num_retrieves == 1
        assert p.num_persists == 2

        # reindex, _persisted_item_from_file_path exception
        shutil.rmtree(p.base_dir)
        settings.paths.mkdirs()
        p = TimedRollingFilePersister(settings.paths.event_dir, max_bytes=len(buf) + 50)
        assert p.reindex().is_ok()
        p.persist(uids[-1], buf).unwrap()

        class BrokenRoller3(TimedRollingFilePersister):
            @classmethod
            def _persisted_item_from_file_path(cls, filepath: Path) -> None:  # noqa: ARG003
                raise ValueError("arg")

        p = BrokenRoller3(settings.paths.event_dir, max_bytes=len(buf) + 50)
        problems = p.reindex().unwrap_err()
        assert len(problems.errors) == 2
        assert len(problems.warnings) == 0
        assert isinstance(problems.errors[0], ValueError)
        assert isinstance(problems.errors[1], ReindexError)

        # reindex, _is_iso_parseable exception
        class BrokenRoller4(TimedRollingFilePersister):
            @classmethod
            def _is_iso_parseable(cls, s: str | Path) -> bool:  # noqa: ARG003
                raise ValueError("arg")

        p = BrokenRoller4(settings.paths.event_dir, max_bytes=len(buf) + 50)
        problems = p.reindex().unwrap_err()
        assert len(problems.errors) == 2
        assert len(problems.warnings) == 0
        assert isinstance(problems.errors[0], ValueError)
        assert isinstance(problems.errors[1], ReindexError)


def test_reindex_pat(tmp_path: Path, monkeypatch: Any) -> None:
    PatWatchdogWithFile.pat_dir = tmp_path / "pats"
    PatWatchdogWithFile.pat_dir.mkdir(parents=True)
    service_name = "foo"
    monkeypatch.setenv(PatWatchdogWithFile.service_variable_name(service_name), "1")
    args = PatWatchdogWithFile.pat_args(service_name=service_name)
    assert len(args) > 0

    def _num_pats() -> int:
        return len([x for x in PatWatchdogWithFile.pat_dir.glob("*") if x.is_file()])

    event_dir = tmp_path / "events"
    event_dir.mkdir(parents=True)
    events = [
        ProblemEvent(
            Src="foo",
            ProblemType=gwproto.messages.Problems.error,
            Summary="Problems, I've got a few",
        ),
        ProblemEvent(
            Src="foo",
            ProblemType=gwproto.messages.Problems.error,
            Summary="maybe not great",
        ),
        ProblemEvent(
            Src="foo",
            ProblemType=gwproto.messages.Problems.error,
            Summary="Don't worry, be happy",
        ),
    ]

    # construction
    p = SlowIndexer(event_dir, pat_watchdog_args=args)
    assert_contents(p, num_pending=0)
    assert _num_pats() == 0

    # explicit empty reindex
    assert p.reindex().is_ok()
    assert_contents(p, num_pending=0)
    assert _num_pats() == 0

    # add events
    for i, event in enumerate(events):
        result = p.persist(event.MessageId, event.model_dump_json().encode())
        assert result.is_ok()
        assert_contents(p, num_pending=i + 1)

    # reindex
    assert p.reindex().is_ok()
    assert_contents(p, num_pending=len(events))
    assert _num_pats() >= (len(events) - 1)
