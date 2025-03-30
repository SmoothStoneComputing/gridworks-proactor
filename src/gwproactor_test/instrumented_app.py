import abc
from typing import Self

from gwproactor.app import App
from gwproactor.config.proactor_config import ProactorConfig
from gwproactor_test.certs import copy_keys, uses_tls
from gwproactor_test.instrumented_proactor import InstrumentedProactor, RecorderStats


class TestApp(App, abc.ABC):
    @classmethod
    def make_stats(cls) -> RecorderStats:
        return RecorderStats()

    @classmethod
    def instantiate_proactor(cls, config: ProactorConfig) -> InstrumentedProactor:
        return InstrumentedProactor(config)

    def instantiate(self) -> Self:
        if uses_tls(self.config.settings):
            copy_keys(
                self.config.name.paths_name,
                self.config.settings,
            )
        return super().instantiate()
