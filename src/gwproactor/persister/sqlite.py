from pathlib import Path
from typing import ClassVar, Optional

from result import Err, Ok, Result
from sqlalchemy import Engine, create_engine, func, text
from sqlmodel import Field, Session, SQLModel, col, select

from gwproactor.persister import UIDExistedWarning, WriteFailed
from gwproactor.persister.interface import PersisterInterface
from gwproactor.problems import Problems


class DatabaseEvent(SQLModel, table=True):
    message_id: str = Field(primary_key=True)
    content: bytes
    __tablename__: ClassVar[str] = "events"


class SQLitePersister(PersisterInterface):
    DATABASE_FILE: str = "events.sqlite"
    DEFAULT_MAX_BYTES: int = 500 * 1024 * 1024

    _database_path: Path
    _max_bytes: int
    _curr_bytes: int = 0
    _url: str
    _engine: Optional[Engine] = None
    _echo: bool = False

    def __init__(
        self,
        database_dir: Path | str,
        *,
        max_bytes: int = DEFAULT_MAX_BYTES,
        sql_echo: bool = False,
    ) -> None:
        self._database_path = Path(database_dir).resolve() / self.DATABASE_FILE
        self._url = f"sqlite:///{self._database_path}"
        self._max_bytes = max_bytes
        self._curr_bytes = 0
        self._echo = sql_echo

    def reindex(self) -> Result[Optional[bool], Problems]:
        self._engine = create_engine(self._url, echo=self._echo)
        SQLModel.metadata.create_all(self._engine)
        self._curr_bytes = self._database_path.stat().st_size
        return Ok()

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            raise RuntimeError(
                "ERROR. SQLitePersister engine has been accessed before reindex() was called"
            )
        return self._engine

    def persist(self, uid: str, content: bytes) -> Result[bool, Problems]:  # noqa: ARG002
        problems = Problems()
        try:
            with Session(self.engine) as session:
                got = session.get(DatabaseEvent, uid)
                if got is not None:
                    problems.add_warning(UIDExistedWarning(uid=uid))
                    session.add(got)
                else:
                    session.add(DatabaseEvent(message_id=uid, content=content))
                session.commit()
            self._curr_bytes = self._database_path.stat().st_size
        except Exception as e:  # noqa: BLE001
            problems.add_error(e).add_error(
                WriteFailed("Add to database failed", uid=uid)
            )
        if problems:
            return Err(problems)
        return Ok()

    def clear(self, uid: str) -> Result[bool, Problems]:
        with Session(self.engine) as session:
            if (event := session.get(DatabaseEvent, uid)) is not None:
                session.delete(event)
                session.commit()
        self._curr_bytes = self._database_path.stat().st_size
        return Ok()

    def pending_ids(self) -> list[str]:
        with Session(self._engine) as session:
            statement = select(DatabaseEvent.message_id)
            results = session.exec(statement)
            return list(results)  # noqa

    @property
    def database_path(self) -> Path:
        return self._database_path

    @property
    def num_pending(self) -> int:
        with Session(self._engine) as session:
            statement = select(func.count(col(DatabaseEvent.message_id)))
            return session.exec(statement).one()

    @property
    def curr_bytes(self) -> int:
        return self._curr_bytes

    @property
    def max_bytes(self) -> int:
        return self._max_bytes

    def __contains__(self, uid: str) -> bool:
        with Session(self.engine) as session:
            return session.get(DatabaseEvent, uid) is not None

    def retrieve(self, uid: str) -> Result[Optional[bytes], Problems]:
        with Session(self.engine) as session:
            event = session.get(DatabaseEvent, uid)
            if event is None:
                return Ok(None)
            return Ok(event.content)

    def vacuum(self) -> None:
        with Session(self.engine) as session:
            session.exec(text("VACUUM;"))  # type: ignore[call-overload]
        self._curr_bytes = self._database_path.stat().st_size
