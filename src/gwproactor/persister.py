import abc
import contextlib
import datetime
import re
import shutil
import subprocess
import time
from abc import abstractmethod
from pathlib import Path
from typing import NamedTuple, Optional

from result import Err, Ok, Result

from gwproactor.problems import Problems


class PersisterException(Exception):
    path: Optional[Path] = None
    uid: str = ""

    def __init__(
        self, msg: str = "", uid: str = "", path: Optional[Path] = None
    ) -> None:
        self.path = path
        self.uid = uid
        super().__init__(msg)

    def __str__(self) -> str:
        s = self.__class__.__name__
        super_str = super().__str__()
        if super_str:
            s += f" [{super_str}]"
        s += f"  for uid: {self.uid}  path:{self.path}"
        return s


class PersisterError(PersisterException): ...


class PersisterWarning(PersisterException): ...


class WriteFailed(PersisterError): ...


class ContentTooLarge(PersisterError): ...


class FileMissing(PersisterError): ...


class ReadFailed(PersisterError): ...


class TrimFailed(PersisterError): ...


class ReindexError(PersisterError): ...


class DecodingError(PersisterError): ...


class ByteDecodingError(DecodingError): ...


class JSONDecodingError(DecodingError): ...


class UIDExistedWarning(PersisterWarning): ...


class FileExistedWarning(PersisterWarning): ...


class FileMissingWarning(PersisterWarning): ...


class UIDMissingWarning(PersisterWarning): ...


class FileEmptyWarning(PersisterWarning): ...


class PersisterInterface(abc.ABC):
    @abstractmethod
    def persist(self, uid: str, content: bytes) -> Result[bool, Problems]:
        """Persist content, indexed by uid"""

    @abstractmethod
    def clear(self, uid: str) -> Result[bool, Problems]:
        """Delete content persisted for uid. It is error to clear a uid which is not currently persisted."""

    @abstractmethod
    def pending_ids(self) -> list[str]:
        """Get list of pending (persisted and not cleared) uids"""

    @property
    @abstractmethod
    def num_pending(self) -> int:
        """Get number of pending uids"""

    @property
    @abstractmethod
    def curr_bytes(self) -> int:
        """Return number of bytes used to store events, if known."""

    @abstractmethod
    def __contains__(self, uid: str) -> bool:
        """Check whether a uid is pending"""

    @abstractmethod
    def retrieve(self, uid: str) -> Result[Optional[bytes], Problems]:
        """Load and return persisted content for uid"""

    @abstractmethod
    def reindex(self) -> Result[Optional[bool], Problems]:
        """Re-created pending index from persisted storage"""


class _PersistedItem(NamedTuple):
    uid: str
    path: Path


class StubPersister(PersisterInterface):
    def persist(self, uid: str, content: bytes) -> Result[bool, Problems]:  # noqa: ARG002
        return Ok()

    def clear(self, uid: str) -> Result[bool, Problems]:  # noqa: ARG002
        return Ok()

    def pending_ids(self) -> list[str]:
        return []

    @property
    def num_pending(self) -> int:
        return 0

    @property
    def curr_bytes(self) -> int:
        return 0

    def __contains__(self, uid: str) -> bool:
        return False

    def retrieve(self, uid: str) -> Result[Optional[bytes], Problems]:  # noqa: ARG002
        return Ok(None)

    def reindex(self) -> Result[Optional[bool], Problems]:
        return Ok()


class SimpleDirectoryWriter(StubPersister):
    _base_dir: Path

    def __init__(
        self,
        base_dir: Path | str,
    ) -> None:
        self._base_dir = Path(base_dir).resolve()

    @classmethod
    def _make_name(cls, dt: datetime.datetime, uid: str) -> str:
        return f"{dt.isoformat()}.uid[{uid}].json"

    def persist(self, uid: str, content: bytes) -> Result[bool, Problems]:
        problems = Problems()
        try:
            if not self._base_dir.exists():
                self._base_dir.mkdir(parents=True, exist_ok=True)
            path = self._base_dir / self._make_name(
                datetime.datetime.now(tz=datetime.timezone.utc), uid
            )
            try:
                with path.open("wb") as f:
                    f.write(content)
            except Exception as e:  # pragma: no cover  # noqa: BLE001
                return Err(
                    problems.add_error(e).add_error(
                        WriteFailed("Open or write failed", uid=uid, path=path)
                    )
                )
        except Exception as e:  # noqa: BLE001
            return Err(
                problems.add_error(e).add_error(
                    PersisterError("Unexpected error", uid=uid)
                )
            )
        if problems:
            return Err(problems)
        return Ok()


class TimedRollingFilePersister(PersisterInterface):
    DEFAULT_MAX_BYTES: int = 500 * 1024 * 1024
    FILENAME_RGX: re.Pattern[str] = re.compile(r"(?P<dt>.*)\.uid\[(?P<uid>.*)].json$")
    REINDEX_PAT_SECONDS = 1.0

    _base_dir: Path
    _max_bytes: int = DEFAULT_MAX_BYTES
    _pending: dict[str, Path]
    _curr_dir: Path
    _curr_bytes: int
    _pat_watchdog_args: Optional[list[str]] = None
    _reindex_pat_seconds: float = REINDEX_PAT_SECONDS

    def __init__(
        self,
        base_dir: Path | str,
        max_bytes: int = DEFAULT_MAX_BYTES,
        pat_watchdog_args: Optional[list[str]] = None,
        reindex_pat_seconds: float = REINDEX_PAT_SECONDS,
    ) -> None:
        self._base_dir = Path(base_dir).resolve()
        self._max_bytes = max_bytes
        self._curr_dir = self._today_dir()
        self._curr_bytes = 0
        self._pat_watchdog_args = pat_watchdog_args
        self._reindex_pat_seconds = reindex_pat_seconds
        self._pending = {}

    @property
    def max_bytes(self) -> int:
        return self._max_bytes

    @property
    def curr_bytes(self) -> int:
        return self._curr_bytes

    @property
    def base_dir(self) -> Path:
        return self._base_dir

    @property
    def curr_dir(self) -> Path:
        return self._curr_dir

    def persist(self, uid: str, content: bytes) -> Result[bool, Problems]:
        problems = Problems()
        try:
            if len(content) > self._max_bytes:
                return Err(
                    problems.add_error(
                        ContentTooLarge(
                            f"content bytes ({len(content)} > max bytes {self._max_bytes}",
                            uid=uid,
                        )
                    )
                )
            if len(content) + self._curr_bytes > self._max_bytes:
                trimmed = self._trim_old_storage(len(content))
                match trimmed:
                    case Err(trim_problems):
                        problems.add_problems(trim_problems)
                        if problems.errors:
                            return Err(problems.add_error(TrimFailed(uid=uid)))
            existing_path = self._pending.pop(uid, None)
            if existing_path is not None:
                problems.add_warning(UIDExistedWarning(uid=uid, path=existing_path))
                if existing_path.exists():
                    self._curr_bytes -= existing_path.stat().st_size
                    problems.add_warning(
                        FileExistedWarning(uid=uid, path=existing_path)
                    )
                else:
                    problems.add_warning(
                        FileMissingWarning(uid=uid, path=existing_path)
                    )
            self._roll_curr_dir()
            self._pending[uid] = self._curr_dir / self._make_name(
                datetime.datetime.now(tz=datetime.timezone.utc), uid
            )
            try:
                with self._pending[uid].open("wb") as f:
                    f.write(content)
                self._curr_bytes += len(content)
            except Exception as e:  # pragma: no cover  # noqa: BLE001
                return Err(
                    problems.add_error(e).add_error(
                        WriteFailed("Open or write failed", uid=uid, path=existing_path)
                    )
                )
        except Exception as e:  # noqa: BLE001
            return Err(
                problems.add_error(e).add_error(
                    PersisterError("Unexpected error", uid=uid)
                )
            )
        if problems:
            return Err(problems)
        return Ok()

    def _trim_old_storage(self, needed_bytes: int) -> Result[bool, Problems]:
        problems = Problems()
        last_day_dir: Optional[Path] = None
        items = list(self._pending.items())
        for uid, path in items:
            try:
                match self.clear(uid):
                    case Err(other):
                        problems.add_problems(other)
                day_dir = path.parent
                if last_day_dir is not None and last_day_dir != day_dir:
                    shutil.rmtree(last_day_dir, ignore_errors=True)
                last_day_dir = day_dir
            except Exception as e:  # noqa: BLE001
                problems.add_error(e)
                problems.add_error(
                    PersisterError("Unexpected error", uid=uid, path=path)
                )
            if self._curr_bytes <= self._max_bytes - needed_bytes:
                break
        try:
            if last_day_dir is not None and (
                not self._pending
                or next(iter(self._pending.values())).parent != last_day_dir
            ):
                shutil.rmtree(last_day_dir, ignore_errors=True)
        except Exception as e:  # pragma: no cover  # noqa: BLE001
            problems.add_error(e)
            problems.add_error(PersisterError("Unexpected error"))
        if problems:
            return Err(problems)
        return Ok()

    def clear(self, uid: str) -> Result[bool, Problems]:
        problems = Problems()
        path = self._pending.pop(uid, None)
        if path:
            if path.exists():
                self._curr_bytes -= path.stat().st_size
                path.unlink()
                path_dir = path.parent
                # Remove directory if empty.
                # This is much faster than using iterdir.
                with contextlib.suppress(OSError):
                    path_dir.rmdir()
            else:
                problems.add_warning(FileMissingWarning(uid=uid, path=path))
        else:
            problems.add_warning(UIDMissingWarning(uid=uid, path=path))
        if problems:
            return Err(problems)
        return Ok()

    def pending_ids(self) -> list[str]:
        return list(self._pending.keys())

    def pending_paths(self) -> list[Path]:
        return list(self._pending.values())

    def pending_dict(self) -> dict[str, Path]:
        return dict(self._pending)

    @property
    def num_pending(self) -> int:
        return len(self._pending)

    def __contains__(self, uid: str) -> bool:
        return uid in self._pending

    def get_path(self, uid: str) -> Optional[Path]:
        return self._pending.get(uid, None)

    def retrieve(self, uid: str) -> Result[Optional[bytes], Problems]:
        problems = Problems()
        content: Optional[bytes] = None
        path = self._pending.get(uid, None)
        if path:
            if path.exists():
                try:
                    with path.open("rb") as f:
                        content = f.read()
                except Exception as e:  # pragma: no cover  # noqa: BLE001
                    problems.add_error(e).add_error(
                        ReadFailed("Open or read failed", uid=uid, path=path)
                    )
            else:
                problems.add_error(FileMissing(uid=uid, path=path))
        if problems:
            return Err(problems)
        return Ok(content)

    def reindex(self) -> Result[bool, Problems]:
        problems = Problems()
        self._curr_bytes = 0
        paths: list[_PersistedItem] = []
        last_pat = time.time()
        for base_dir_entry in self._base_dir.iterdir():  # noqa: PLR1702
            try:
                if base_dir_entry.is_dir() and self._is_iso_parseable(base_dir_entry):
                    for day_dir_entry in base_dir_entry.iterdir():
                        if self._pat_watchdog_args:
                            now = time.time()
                            if now > last_pat + self._reindex_pat_seconds:
                                last_pat = now
                                subprocess.run(self._pat_watchdog_args, check=True)  # noqa: S603
                        try:
                            if persisted_item := self._persisted_item_from_file_path(
                                day_dir_entry
                            ):
                                self._curr_bytes += persisted_item.path.stat().st_size
                                paths.append(persisted_item)
                        except Exception as e:  # noqa: BLE001
                            problems.add_error(e).add_error(
                                ReindexError(path=day_dir_entry)
                            )
            except Exception as e:  # noqa: BLE001, PERF203
                problems.add_error(e).add_error(ReindexError())
        # The next line is correct, though PyCharm gives a false-positive warning.
        # paths is a list of tuples, which the dict constructor will treat
        # as a list of key-values pairs, which is the intended behavior.
        self._pending = dict(sorted(paths, key=lambda item: item.path))  # type: ignore[attr-defined]
        if problems:
            return Err(problems)
        return Ok()

    def _today_dir(self) -> Path:
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        return (
            self._base_dir
            / datetime.datetime(
                now.year, now.month, now.day, tzinfo=now.tzinfo
            ).isoformat()
        )

    def _roll_curr_dir(self) -> None:
        today_dir = self._today_dir()
        if today_dir != self._curr_dir:
            self._curr_dir = today_dir
        if not self._curr_dir.exists():
            self._curr_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def _make_name(cls, dt: datetime.datetime, uid: str) -> str:
        return f"{dt.isoformat()}.uid[{uid}].json"

    @classmethod
    def _persisted_item_from_file_path(cls, filepath: Path) -> Optional[_PersistedItem]:
        item = None

        try:
            match = cls.FILENAME_RGX.match(filepath.name)
            if match and cls._is_iso_parseable(match.group("dt")):
                item = _PersistedItem(match.group("uid"), filepath)
        except:  # pragma: no cover  # noqa: E722, S110
            pass
        return item

    @classmethod
    def _is_iso_parseable(cls, s: str | Path) -> bool:
        try:
            if isinstance(s, Path):
                s = s.name
            return isinstance(datetime.datetime.fromisoformat(s), datetime.datetime)
        except:  # noqa: E722
            return False
