from typing import Optional

from result import Ok, Result

from gwproactor.persister.interface import PersisterInterface
from gwproactor.problems import Problems


class StubPersister(PersisterInterface):
    _num_persists: int = 0
    _num_retrieves: int = 0
    _num_clears: int = 0

    def persist(self, uid: str, content: bytes) -> Result[bool, Problems]:  # noqa: ARG002
        self._num_persists += 1
        return Ok()

    def clear(self, uid: str) -> Result[bool, Problems]:  # noqa: ARG002
        self._num_clears += 1
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
        self._num_retrieves += 1
        return Ok()

    @property
    def num_persists(self) -> int:
        return self._num_persists

    @property
    def num_retrieves(self) -> int:
        return self._num_retrieves

    @property
    def num_clears(self) -> int:
        return self._num_clears
