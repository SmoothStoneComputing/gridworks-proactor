# ruff: noqa: PLR2004

from gwproactor import Problems
from gwproactor.persister import PersisterError, PersisterWarning


def test_problems() -> None:
    p = Problems()
    assert not p
    assert not str(p)
    assert repr(p) == str(p)
    assert p.max_problems == Problems.MAX_PROBLEMS
    p.add_error(PersisterError(uid="1"))
    assert p
    assert len(p.errors) == 1
    assert len(p.warnings) == 0
    p.add_warning(PersisterWarning(uid="2"))
    assert p
    assert len(p.errors) == 1
    assert len(p.warnings) == 1
    assert repr(p) == str(p)
    p2 = Problems(
        errors=[PersisterError(uid="3"), PersisterError(uid="4")],
        warnings=[PersisterWarning(uid="5"), PersisterWarning(uid="6")],
        max_problems=4,
    )
    assert p2
    assert len(p2.errors) == 2
    assert len(p2.warnings) == 2
    p2.add_problems(p)
    assert len(p2.errors) == 3
    assert len(p2.warnings) == 3
    assert str(p2)
    p3 = Problems(
        errors=[PersisterError(uid="7"), PersisterError(uid="8")],
        warnings=[PersisterWarning(uid="9"), PersisterWarning(uid="10")],
    )
    p2.add_problems(p3)
    assert len(p2.errors) == 4
    assert len(p2.warnings) == 4
    p2.add_error(PersisterError(uid="11"))
    p2.add_warning(PersisterWarning(uid="12"))
    assert len(p2.errors) == 4
    assert len(p2.warnings) == 4
    assert all(isinstance(entry, PersisterError) for entry in p2.errors)
    assert all(isinstance(entry, PersisterWarning) for entry in p2.warnings)
    for error, exp_uid in zip(p2.errors, [3, 4, 1, 7]):
        assert isinstance(error, PersisterError)
        assert int(error.uid) == exp_uid
    for error, exp_uid in zip(p2.warnings, [5, 6, 2, 9]):
        assert isinstance(error, PersisterWarning)
        assert int(error.uid) == exp_uid
    assert str(p2)
