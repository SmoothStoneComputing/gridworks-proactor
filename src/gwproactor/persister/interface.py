import abc
from abc import abstractmethod
from typing import Optional

from result import Result

from gwproactor.problems import Problems

ENCODING: str = "utf-8"


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

    @property
    @abstractmethod
    def num_persists(self) -> int:
        """Total number of calls to persist() since construction."""

    @property
    @abstractmethod
    def num_retrieves(self) -> int:
        """Total number of calls to retrieve() since construction."""

    @property
    @abstractmethod
    def num_clears(self) -> int:
        """Total number of calls to clear() since construction."""
