from pathlib import Path
from typing import Optional

from gwproto import Message, MQTTTopic

from gwproactor import Proactor, ProactorSettings
from gwproactor.config import Paths, paths
from gwproactor.config.proactor_config import ProactorName
from gwproactor.links import QOS
from gwproactor.persister import PersisterInterface, TimedRollingFilePersister
from gwproactor_test.dummies.names import (
    CHILD_SHORT_NAME,
    DUMMY_CHILD_NAME,
)
from gwproactor_test.dummies.pair.child_config import DummyChildSettings
from gwproactor_test.instrumented_app import TestApp


class DummyChildApp(TestApp):
    @classmethod
    def get_name(cls) -> ProactorName:
        return ProactorName(
            long_name=DUMMY_CHILD_NAME,
            short_name=CHILD_SHORT_NAME,
            paths_name=DUMMY_CHILD_NAME,
        )

    @classmethod
    def get_settings(cls, paths_name: Optional[str | Path] = None) -> ProactorSettings:
        if paths_name is None:
            paths_name = paths.DEFAULT_NAME_DIR
        return DummyChildSettings(paths=Paths(name=paths_name))

    @classmethod
    def make_persister(cls, settings: ProactorSettings) -> PersisterInterface:
        return TimedRollingFilePersister(settings.paths.event_dir)

    def assemble_links(self, proactor: Proactor) -> None:
        super().assemble_links(proactor)
        for topic in [
            MQTTTopic.encode_subscription(Message.type_name(), "1", "a"),
            MQTTTopic.encode_subscription(Message.type_name(), "2", "b"),
        ]:
            proactor.links.subscribe(
                DummyChildSettings.PARENT_MQTT, topic, QOS.AtMostOnce
            )
