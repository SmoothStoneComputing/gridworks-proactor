from pathlib import Path
from typing import Optional


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


class EventDecodingError(DecodingError): ...


class UIDExistedWarning(PersisterWarning): ...


class FileExistedWarning(PersisterWarning): ...


class FileMissingWarning(PersisterWarning): ...


class UIDMissingWarning(PersisterWarning): ...


class FileEmptyWarning(PersisterWarning): ...
