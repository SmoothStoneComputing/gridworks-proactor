import abc
import shutil
from pathlib import Path
from typing import Any, Optional

from gwproactor import AppSettings
from gwproactor.app import App, SubTypes
from gwproactor.codecs import CodecFactory
from gwproactor.config import DEFAULT_LAYOUT_FILE, Paths
from gwproactor_test.certs import copy_keys, set_test_certificate_cache_dir, uses_tls
from gwproactor_test.instrumented_proactor import InstrumentedProactor

TEST_HARDWARE_LAYOUT_PATH = Path(__file__).parent / "config" / DEFAULT_LAYOUT_FILE

set_test_certificate_cache_dir(
    Path(__file__).parent.parent.parent / "tests/.certificate_cache"
)


class InstrumentedApp(App, abc.ABC):
    src_layout_path: Path

    def __init__(
        self,
        *,
        paths_name: Optional[str] = None,
        paths: Optional[Paths] = None,
        app_settings: Optional[AppSettings] = None,
        codec_factory: Optional[CodecFactory] = None,
        sub_types: Optional[SubTypes] = None,
        env_file: Optional[str | Path] = None,
        src_layout_path: Optional[str | Path] = TEST_HARDWARE_LAYOUT_PATH,
        **kwargs: Any,
    ) -> None:
        paths_name = paths_name or self.paths_name()
        if src_layout_path is not None:
            paths = Paths() if paths is None else paths
            if paths_name is not None:
                paths = paths.duplicate(name=paths_name)
            self._copy_layout(Path(src_layout_path), paths)
        super().__init__(
            paths_name=paths_name,
            paths=paths,
            app_settings=app_settings,
            codec_factory=codec_factory,
            sub_types=sub_types,
            env_file=env_file,
            **kwargs,
        )
        self._copy_keys()

    @classmethod
    def make_subtypes(cls) -> SubTypes:
        sub_types = super().make_subtypes()
        sub_types.proactor_type = InstrumentedProactor
        return sub_types

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
