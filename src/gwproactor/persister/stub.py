from typing import Optional

from gwproto.messages import EventBase
from result import Ok, Result

from gwproactor.persister.interface import PersisterInterface
from gwproactor.problems import Problems


class StubPersister(PersisterInterface):
    ENCODING = "utf-8"

    def persist_event(self, event: EventBase) -> Result[bool, Problems]:  # noqa: ARG002
        """Persist content, indexed by uid"""
        return self.persist(
            event.MessageId, event.model_dump_json(indent=2).encode(self.ENCODING)
        )

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
