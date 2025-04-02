import abc
import shutil
from pathlib import Path
from types import ModuleType
from typing import Optional

from gwproactor import ProactorSettings
from gwproactor.actors.actor import PrimeActor
from gwproactor.app import App
from gwproactor.codecs import CodecFactory
from gwproactor.config import DEFAULT_LAYOUT_FILE, Paths
from gwproactor_test.certs import copy_keys, uses_tls
from gwproactor_test.instrumented_proactor import InstrumentedProactor, RecorderStats

TEST_HARDWARE_LAYOUT_PATH = Path(__file__).parent / "config" / DEFAULT_LAYOUT_FILE


class InstrumentedApp(App, abc.ABC):
    src_layout_path: Path

    def __init__(
        self,
        *,
        paths: Optional[Paths] = None,
        proactor_settings: Optional[ProactorSettings] = None,
        prime_actor_type: Optional[type[PrimeActor]] = None,
        codec_factory: Optional[CodecFactory] = None,
        actors_module: Optional[ModuleType] = None,
        src_layout_path: Optional[str | Path] = TEST_HARDWARE_LAYOUT_PATH,
    ) -> None:
        if src_layout_path is not None:
            paths = Paths() if paths is None else paths
            self._copy_layout(Path(src_layout_path), paths)
        super().__init__(
            paths=paths,
            proactor_settings=proactor_settings,
            proactor_type=InstrumentedProactor,
            prime_actor_type=prime_actor_type,
            codec_factory=codec_factory,
            actors_module=actors_module,
        )
        self._copy_keys()

    @classmethod
    def _make_stats(cls) -> RecorderStats:
        return RecorderStats()

    def _copy_keys(self) -> None:
        if uses_tls(self.config.settings):
            copy_keys(
                str(self.config.settings.paths.name),
                self.config.settings,
            )

    @classmethod
    def _copy_layout(cls, src_path: Path, paths: Paths) -> None:
        Path(paths.hardware_layout).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src_path, paths.hardware_layout)
