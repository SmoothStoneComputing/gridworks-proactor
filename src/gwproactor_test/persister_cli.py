# ruff: noqa: T201
import abc
import cProfile
import pstats
import shutil
import time
import uuid
from enum import Enum
from pathlib import Path
from pstats import SortKey
from typing import NamedTuple, Optional

import rich
import typer
from pydantic import BaseModel

from gwproactor.config import Paths
from gwproactor.persister import (
    PersisterInterface,
    TimedRollingFilePersister,
)

MEASUREMENT_FILE = "persister-measurements.json"

app = typer.Typer(
    no_args_is_help=True,
    pretty_exceptions_enable=False,
    rich_markup_mode="rich",
    help="Measure persister performance",
)


class PersisterType(str, Enum):
    json = "json"
    all = "all"


persister_classes: dict[PersisterType, type] = {
    PersisterType.json: TimedRollingFilePersister,
}


def paths_name(persister_type: PersisterType) -> str:
    return f"persister-test-{persister_type.value}"


class MeasurementParams(NamedTuple):
    persister_type: str
    func_name: str
    n: int
    measurements: int
    measure_size: int

    def __str__(self) -> str:
        return f"{self.persister_type}-{self.func_name}-n{self.n}-m{self.measurements}-ms{self.measure_size}"


class FuncMeasurements(BaseModel):
    params: MeasurementParams
    i: list[int] = []
    single_times: list[float] = []
    step_times: list[float] = []
    total_time: float = 0


class Measurements(BaseModel):
    funcs: dict[str, FuncMeasurements] = {}


class MeasureCommand(abc.ABC):
    paths: Paths
    measurements_dir: Path
    measurement_file: Path
    stats_file: Path
    persister: PersisterInterface
    params: MeasurementParams

    def __init__(
        self,
        persister_type: PersisterType,
        n: Optional[int] = None,
        measurements: int = 10,
        *,
        do_reindex: bool = True,
        persister: Optional[PersisterInterface] = None,
        paths: Optional[Paths] = None,
    ) -> None:
        if persister_type not in persister_classes:
            raise ValueError(f"Unexpected persister type {persister_type}")
        if paths is None:
            self.paths = Paths(name=paths_name(persister_type))
        else:
            self.paths = paths
        self.paths.mkdirs()
        self.measurements_dir = Path(self.paths.data_dir) / "measurements"
        self.measurements_dir.mkdir(exist_ok=True, parents=True)
        self.measurement_file = self.measurements_dir / MEASUREMENT_FILE
        if persister is None:
            self.persister = persister_classes[persister_type](self.paths.event_dir)
            if do_reindex:
                self.persister.reindex()
        else:
            self.persister = persister
        if n is None:
            n = self.persister.num_pending
        measurements = min(measurements, n)
        self.params = MeasurementParams(
            persister_type=persister_type.value,
            func_name=self.get_name(),
            n=n,
            measurements=measurements,
            measure_size=n // measurements if n else 0,
        )
        self.stats_file = self.measurements_dir / f"{self.params}.prf"

    def measure(
        self,
        *,
        profile: bool = False,
    ) -> FuncMeasurements:
        data = FuncMeasurements(params=self.params)
        step_start_time = time.time()
        call_start_time: float = step_start_time
        prf: Optional[cProfile.Profile]
        start_size_str = self.size_str()
        if profile:
            prf = cProfile.Profile()
            prf.enable()
        else:
            prf = None
        event_ids = self.get_event_ids()
        total_start_time = time.time()
        for i in range(data.params.n):
            measure_now = (i + 1) % data.params.measure_size == 0
            if measure_now:
                call_start_time = time.time()
            self.func_to_measure(event_ids[i])
            if measure_now:
                end_time = time.time()
                data.i.append(i)
                data.single_times.append(end_time - call_start_time)
                data.step_times.append(end_time - step_start_time)
                step_start_time = end_time
        data.total_time = time.time() - total_start_time
        if prf is not None:
            prf.disable()
            prf.create_stats()
            prf.dump_stats(self.stats_file)
            sortby = SortKey.CUMULATIVE
            ps = pstats.Stats(prf).sort_stats(sortby)
            rich.print(f"Profiling for {data.params}:")
            ps.print_stats(20)
        else:
            rich.print(f"Measurements for {data.params}:")
            rich.print(data)
            if self.measurement_file.exists():
                with self.measurement_file.open("r") as f:
                    read = f.read()
                if read:
                    recorded = Measurements.model_validate_json(read)
                else:
                    recorded = Measurements()
            else:
                recorded = Measurements()
            param_str = str(data.params)
            if param_str in recorded.funcs:
                existing = recorded.funcs[param_str]
                existing.i.extend(data.i)
                existing.single_times.extend(data.single_times)
                existing.step_times.extend(data.step_times)
            else:
                recorded.funcs[param_str] = data
            with self.measurement_file.open("w") as f:
                f.write(recorded.model_dump_json(indent=2))
        rich.print(f"Size before: {start_size_str}")
        rich.print(f"Size after:  {self.size_str()}")
        return data

    @abc.abstractmethod
    def func_to_measure(self, event_id: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_event_ids(self) -> list[str]:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def get_name(cls) -> str:
        raise NotImplementedError

    def size_str(self) -> str:
        curr_bytes = self.persister.curr_bytes
        curr_KB = curr_bytes / 1024
        curr_KB_str = f"{curr_KB:12.1f}" if curr_KB < 1 else f"{int(curr_KB):14d}"
        curr_MB = curr_KB / 1024
        return f"{curr_bytes:14d} bytes    {curr_KB_str} KB    {curr_MB:6.1f} MB"

    def __str__(self) -> str:
        return (
            f"Commands: {self.__class__.__name__}\n"
            f"  Persister: {self.persister.__class__.__name__}\n"
            f"  Measure params: {self.params}\n"
            f"  Data dir:\n    {self.paths.data_dir}\n"
            f"  Measurement file:\n    {self.measurement_file}\n"
            f"  Stats file:\n    {self.stats_file}\n"
        )


class MeasurePersist(MeasureCommand):
    record: bytes

    def __init__(
        self,
        persister_type: PersisterType,
        n: int = 10000,
        measurements: int = 10,
        rec_size: int = 10 * 1024,
        *,
        persister: Optional[PersisterInterface] = None,
        paths: Optional[Paths] = None,
    ) -> None:
        self.record = ("#" * rec_size).encode()
        super().__init__(
            persister_type=persister_type,
            n=n,
            measurements=measurements,
            paths=paths,
            persister=persister,
        )

    def func_to_measure(self, event_id: str) -> None:
        self.persister.persist(event_id, self.record)

    def get_event_ids(self) -> list[str]:
        return [str(uuid.uuid4()) for _ in range(self.params.n)]

    @classmethod
    def get_name(cls) -> str:
        return "persist"


class MeasureRetrieve(MeasureCommand):
    def func_to_measure(self, event_id: str) -> None:
        self.persister.retrieve(event_id)

    def get_event_ids(self) -> list[str]:
        return self.persister.pending_ids()

    @classmethod
    def get_name(cls) -> str:
        return "retrieve"


class MeasureClear(MeasureCommand):
    def func_to_measure(self, event_id: str) -> None:
        self.persister.clear(event_id)

    def get_event_ids(self) -> list[str]:
        return self.persister.pending_ids()

    @classmethod
    def get_name(cls) -> str:
        return "clear"


class MeasureReindex(MeasureCommand):
    def __init__(
        self,
        persister_type: PersisterType,
        *,
        persister: Optional[PersisterInterface] = None,
        paths: Optional[Paths] = None,
    ) -> None:
        super().__init__(
            persister_type=persister_type,
            do_reindex=False,
            n=1,
            measurements=1,
            paths=paths,
            persister=persister,
        )

    def func_to_measure(self, _: str) -> None:
        self.persister.reindex()

    def get_event_ids(self) -> list[str]:
        return [""]

    @classmethod
    def get_name(cls) -> str:
        return "reindex"


@app.command()
def persist(
    persister_type: PersisterType,
    n: int = 10000,
    rec_size: int = 10 * 1024,
    measurements: int = 10,
    profile: bool = False,
    dry_run: bool = False,
) -> None:
    """Measure timing of PersisterInterface.persist()"""
    command = MeasurePersist(
        persister_type=persister_type, n=n, measurements=measurements, rec_size=rec_size
    )
    rich.print(command)
    if not dry_run:
        command.measure(profile=profile)


@app.command()
def reindex(
    persister_type: PersisterType, profile: bool = False, dry_run: bool = False
) -> None:
    """Measure timing of PersisterInterface.reindex()"""
    command = MeasureReindex(persister_type=persister_type)
    rich.print(command)
    if not dry_run:
        command.measure(profile=profile)


@app.command()
def retrieve(
    persister_type: PersisterType,
    n: Optional[int] = None,
    measurements: int = 10,
    profile: bool = False,
    dry_run: bool = False,
) -> None:
    """Measure timing of PersisterInterface.retrieve()"""
    command = MeasureRetrieve(
        persister_type=persister_type, n=n, measurements=measurements
    )
    rich.print(command)
    if not dry_run:
        command.measure(profile=profile)


@app.command()
def clear(
    persister_type: PersisterType,
    n: Optional[int] = None,
    measurements: int = 10,
    profile: bool = False,
    dry_run: bool = False,
) -> None:
    """Measure timing of PersisterInterface.retrieve()"""
    command = MeasureClear(
        persister_type=persister_type, n=n, measurements=measurements
    )
    rich.print(command)
    if not dry_run:
        command.measure(profile=profile)


@app.command()
def size(persister_type: PersisterType, dry_run: bool = False) -> None:
    """Print the storage size used by the requested persister."""
    command = MeasureReindex(persister_type=persister_type)
    if not dry_run:
        command.persister.reindex()
        rich.print(command.size_str())


@app.command()
def cleanup(
    persister_type: PersisterType,
    dry_run: bool = False,
    measurements: bool = False,
) -> None:
    """Delete event directories and, optionally, measurements."""
    persister_types = (
        persister_classes.keys()
        if persister_type == PersisterType.all
        else [persister_type]
    )
    for persister_type_ in persister_types:
        rich.print(f"Cleanup for persister {persister_type_.value}")
        paths = Paths(name=paths_name(persister_type_))
        event_dir = Path(paths.event_dir)
        rich.print(f"Removing {event_dir}")
        if not dry_run and event_dir.exists():
            shutil.rmtree(str(event_dir))
        if measurements:
            measurement_dir = Path(paths.data_dir) / "measurements"
            rich.print(f"Removing {measurement_dir}")
            if not dry_run and measurement_dir.exists():
                shutil.rmtree(str(measurement_dir))


def delimit_str(tag: str, markup: str) -> str:
    s = "\n"
    s += f"[{markup}]" + "#" * 80 + "\n"
    s += f"#[/] {tag}\n"
    s += f"[{markup}]" + "#" * 80
    return s


def delimit(tag: str, markup: str) -> None:
    rich.print(delimit_str(tag, markup))


class TotalTime(NamedTuple):
    func_name: str
    time: float


def _persister_suite(
    persister_type: PersisterType,
    n: int = 10000,
    profile: bool = False,
    clean_measurements: bool = False,
    dry_run: bool = False,
) -> list[TotalTime]:
    profile_str = "with profiling" if profile else "without profiling"
    delimit_tag = f"{persister_type.value}  {profile_str}"
    delimit(delimit_tag, "orange3")
    cleanup(
        persister_type=persister_type, measurements=clean_measurements, dry_run=dry_run
    )
    times: list[TotalTime] = []
    measure_persist = MeasurePersist(persister_type=persister_type, n=n)
    paths = measure_persist.paths
    persister = measure_persist.persister
    commands = [
        measure_persist,
        MeasureReindex(
            persister_type=persister_type,
            persister=persister,
            paths=paths,
        ),
        MeasureRetrieve(
            persister_type=persister_type,
            n=n,
            persister=persister,
            paths=paths,
        ),
        MeasureClear(
            persister_type=persister_type,
            n=n,
            persister=persister,
            paths=paths,
        ),
    ]
    for command in commands:
        delimit(delimit_tag + f"  ({command.get_name()})", "yellow1")
        rich.print(command)
        if not dry_run:
            start_time = time.time()
            data = command.measure(profile=profile)
            times.append(
                TotalTime(
                    command.get_name(),
                    time.time() - start_time if profile else data.total_time,
                )
            )
    return times


class _SuiteTotals(NamedTuple):
    persister_type: PersisterType
    profile: bool
    times: list[TotalTime]


class SuiteTotals(NamedTuple):
    without_profiling: _SuiteTotals
    with_profiling: _SuiteTotals


def persister_suite(
    persister_type: PersisterType,
    n: int = 10000,
    profile: bool = True,
    dry_run: bool = False,
) -> SuiteTotals:
    delimit(f"{persister_type.value}", "green")
    without_profiling = _persister_suite(
        persister_type=persister_type,
        n=n,
        profile=False,
        clean_measurements=True,
        dry_run=dry_run,
    )
    if profile:
        with_profiling = _persister_suite(
            persister_type=persister_type,
            n=n,
            profile=True,
            clean_measurements=False,
            dry_run=dry_run,
        )
    else:
        with_profiling = []
    return SuiteTotals(
        without_profiling=_SuiteTotals(
            persister_type=persister_type,
            profile=False,
            times=without_profiling,
        ),
        with_profiling=_SuiteTotals(
            persister_type=persister_type,
            profile=True,
            times=with_profiling,
        ),
    )


@app.command()
def suite(
    persister_type: PersisterType = PersisterType.all,
    n: int = 10000,
    profile: bool = True,
    dry_run: bool = False,
) -> None:
    """Run a suite of measurements for all persister types."""
    persister_types = (
        persister_classes.keys()
        if persister_type == PersisterType.all
        else [persister_type]
    )
    persister_names = [persister_type.value for persister_type in persister_types]
    rich.print(
        delimit_str(
            f"Running a suite of measurements for persisters {persister_names}",
            "cyan",
        ).lstrip()
    )
    totals: list[SuiteTotals] = [
        persister_suite(
            persister_type=persister_type, n=n, profile=profile, dry_run=dry_run
        )
        for persister_type in persister_types
    ]
    if not dry_run:
        if profile:
            rich.print("\nElapsed time profiling:")
            for total in totals:
                rich.print(f"  {total.with_profiling.persister_type.value}:")
                for command_total in total.with_profiling.times:
                    rich.print(
                        f"    {command_total.func_name:10s}:  {command_total.time:10.7f}"
                    )
        rich.print("\nElapsed time without profiling:")
        for total in totals:
            rich.print(f"  {total.without_profiling.persister_type}:")
            for command_total in total.without_profiling.times:
                rich.print(
                    f"    {command_total.func_name:10s}:  {command_total.time:10.7f}"
                )
    rich.print("\n")


@app.callback()
def _main() -> None: ...


if __name__ == "__main__":
    app()
