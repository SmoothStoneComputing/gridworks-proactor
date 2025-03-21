"""Scada implementation"""

from typing import Optional, cast

from gwproto import MQTTCodec, create_message_model

from gwproactor.links.link_settings import LinkSettings
from gwproactor.persister import SimpleDirectoryWriter
from gwproactor.proactor_implementation import Proactor
from gwproactor_test.dummies.names import (
    CHILD_SHORT_NAME,
    DUMMY_CHILD_NAME,
    DUMMY_PARENT_NAME,
    PARENT_SHORT_NAME,
)
from gwproactor_test.dummies.pair.parent_config import DummyParentSettings


class ParentMQTTCodec(MQTTCodec):
    def __init__(self) -> None:
        super().__init__(
            create_message_model(
                model_name="ParentMessageDecoder",
                module_names=["gwproto.messages", "gwproactor.message"],
            )
        )

    def validate_source_and_destination(self, src: str, dst: str) -> None:
        if src != DUMMY_CHILD_NAME or dst != PARENT_SHORT_NAME:
            raise ValueError(
                "ERROR validating src and/or dst\n"
                f"  exp: {DUMMY_CHILD_NAME} -> {PARENT_SHORT_NAME}\n"
                f"  got: {src} -> {dst}"
            )


class DummyParent(Proactor):
    CHILD_MQTT = "child"

    def __init__(
        self,
        name: str = "",
        settings: Optional[DummyParentSettings] = None,
    ) -> None:
        super().__init__(
            name=name or DUMMY_PARENT_NAME,
            settings=DummyParentSettings() if settings is None else settings,
        )
        self._links.add_mqtt_link(
            LinkSettings(
                client_name=self.CHILD_MQTT,
                gnode_name=DUMMY_CHILD_NAME,
                spaceheat_name=CHILD_SHORT_NAME,
                mqtt=self.settings.child_mqtt,
                codec=ParentMQTTCodec(),
                downstream=True,
            )
        )

    @classmethod
    def make_event_persister(
        cls,
        settings: DummyParentSettings,  # type: ignore[override]
    ) -> SimpleDirectoryWriter:
        return SimpleDirectoryWriter(settings.paths.event_dir)

    @property
    def publication_name(self) -> str:
        return self.name

    @property
    def settings(self) -> DummyParentSettings:
        return cast(DummyParentSettings, self._settings)

    @property
    def subscription_name(self) -> str:
        return PARENT_SHORT_NAME
