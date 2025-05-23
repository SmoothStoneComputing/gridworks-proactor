# ruff: noqa: PLR2004
# mypy: disable-error-code="union-attr"

import json
from pathlib import Path
from typing import Optional, Union

import gwproto
from gwproto.messages import ProblemEvent

from gwproactor import AppSettings
from gwproactor.persister import SQLitePersister


def assert_contents(
    p: SQLitePersister,
    uids: Optional[list[str]] = None,
    num_pending: Optional[int] = None,
    curr_bytes_range: Optional[tuple[int, int]] = None,
    curr_dir: Optional[Union[str, Path]] = None,
    check_index: bool = True,
    max_bytes: Optional[int] = None,
) -> None:
    assert p.num_pending == len(p.pending_ids())
    if num_pending is not None:
        assert p.num_pending == num_pending
    if curr_bytes_range is not None:
        assert p.curr_bytes >= curr_bytes_range[0]
        assert p.curr_bytes <= curr_bytes_range[1]
    if max_bytes is not None:
        assert p.max_bytes == max_bytes
    if uids is not None:
        str_uids = sorted([str(uid) for uid in uids])
        assert p.pending_ids() == str_uids
        assert p.num_pending == len(str_uids)
        for str_uid in str_uids:
            assert str_uid in p
    if check_index:
        p2 = SQLitePersister(
            database_dir=p.database_path.parent,  # noqa: SLF001
            max_bytes=p.max_bytes,
        )
        assert p2.reindex().is_ok()
        assert p.pending_ids() == p2.pending_ids()
        assert p.curr_bytes == p2.curr_bytes


def test_sqlite_persister_happy_path(tmp_path: Path) -> None:
    settings = AppSettings()
    settings.paths.mkdirs()
    # empty persister
    persister = SQLitePersister(settings.paths.event_dir)
    assert persister.database_path == Path(settings.paths.event_dir) / "events.sqlite"
    assert not persister.database_path.exists()
    assert persister.curr_bytes == 0
    assert persister.reindex().is_ok()
    assert persister.curr_bytes > 0
    initial_bytes = persister.curr_bytes
    assert_contents(
        persister,
        num_pending=0,
        max_bytes=SQLitePersister.DEFAULT_MAX_BYTES,
    )

    # add one
    event = ProblemEvent(
        Src="foo",
        ProblemType=gwproto.messages.Problems.error,
        Summary="Problems, I've got a few",
        Details="Too numerous to name",
    )
    event_bytes = event.model_dump_json().encode()
    result = persister.persist(event.MessageId, event_bytes)
    assert result.is_ok()
    assert_contents(
        persister,
        uids=[event.MessageId],
        num_pending=1,
        curr_bytes_range=(initial_bytes, initial_bytes + len(event_bytes)),
    )

    # retrieve
    retrieved = persister.retrieve(event.MessageId)
    assert retrieved.is_ok(), str(retrieved)
    assert retrieved.value == event_bytes

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
    initial_bytes = persister.curr_bytes
    result = persister.persist(event2.MessageId, event2.model_dump_json().encode())
    assert result.is_ok()
    assert_contents(
        persister,
        uids=[event.MessageId, event2.MessageId],
        num_pending=2,
        curr_bytes_range=(initial_bytes, initial_bytes + len(event2_bytes)),
    )

    # reindex
    initial_bytes = persister.curr_bytes
    assert persister.reindex().is_ok()
    assert_contents(
        persister,
        uids=[event.MessageId, event2.MessageId],
        num_pending=2,
        curr_bytes_range=(initial_bytes, initial_bytes),
    )

    # clear second one
    cleared = persister.clear(event2.MessageId)
    assert cleared.is_ok()
    assert event2.MessageId not in persister.pending_ids()
    assert_contents(
        persister,
        uids=[event.MessageId],
        num_pending=1,
    )

    # clear first one
    cleared = persister.clear(event.MessageId)
    assert cleared.is_ok()
    assert event.MessageId not in persister.pending_ids()
    assert_contents(
        persister,
        num_pending=0,
    )

    # reindex
    initial_bytes = persister.curr_bytes
    assert persister.reindex().is_ok()
    assert len(persister.pending_ids()) == persister.num_pending == 0
    assert persister.curr_bytes == initial_bytes
    assert_contents(
        persister,
        num_pending=0,
    )
