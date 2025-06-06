# ruff: noqa: ERA001

import datetime
from pathlib import Path

from result import Err, Ok, Result

from gwproactor.persister.exceptions import PersisterError, WriteFailed
from gwproactor.persister.stub import StubPersister
from gwproactor.problems import Problems


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
        self._num_persists += 1
        # from gwproto.messages import AnyEvent
        # event = AnyEvent.model_validate_json(content.decode())
        # print(
        #     f"{self._num_persists:3d}  "
        #     f"{event.TypeName:45s}  "
        #     f"{event.Src:35s}  "
        #     f"{event.MessageId[:8]}"
        # )
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
