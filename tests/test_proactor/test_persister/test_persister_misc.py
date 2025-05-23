from pathlib import Path

from gwproactor.persister import PersisterException


def test_persister_exception() -> None:
    e = PersisterException("foo")
    assert str(e)
    assert e.uid == ""
    assert e.path is None

    e = PersisterException("foo", "bar")
    assert str(e)
    assert e.uid == "bar"
    assert e.path is None

    e = PersisterException(uid="bar", path=Path())
    assert str(e)
    assert e.uid == "bar"
    assert e.path == Path()
