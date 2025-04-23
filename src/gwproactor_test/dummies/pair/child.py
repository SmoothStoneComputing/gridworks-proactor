from gwproto import HardwareLayout, Message, MQTTTopic

from gwproactor import App, AppSettings, Proactor
from gwproactor.config import MQTTClient
from gwproactor.config.links import LinkSettings
from gwproactor.config.proactor_config import ProactorName
from gwproactor.links import QOS
from gwproactor.persister import PersisterInterface, TimedRollingFilePersister
from gwproactor_test.dummies import DUMMY_CHILD_NAME, DUMMY_PARENT_NAME


class DummyChildSettings(AppSettings):
    parent: MQTTClient = MQTTClient()


class DummyChildApp(App):
    PARENT_MQTT: str = DUMMY_PARENT_NAME

    @classmethod
    def app_settings_type(cls) -> type[DummyChildSettings]:
        return DummyChildSettings

    @classmethod
    def paths_name(cls) -> str:
        return DUMMY_CHILD_NAME

    def _get_name(self, layout: HardwareLayout) -> ProactorName:
        return ProactorName(
            long_name=layout.scada_g_node_alias,
            short_name="s",
        )

    def _get_link_settings(
        self,
        name: ProactorName,  # noqa: ARG002
        layout: HardwareLayout,
        brokers: dict[str, MQTTClient],  # noqa: ARG002
    ) -> dict[str, LinkSettings]:
        return {
            self.PARENT_MQTT: LinkSettings(
                broker_name=self.PARENT_MQTT,
                peer_long_name=layout.atn_g_node_alias,
                peer_short_name="a",
                upstream=True,
            )
        }

    @classmethod
    def _make_persister(cls, settings: AppSettings) -> PersisterInterface:
        return TimedRollingFilePersister(settings.paths.event_dir)

    def _connect_links(self, proactor: Proactor) -> None:
        super()._connect_links(proactor)
        for topic in [
            MQTTTopic.encode_subscription(Message.type_name(), "1", "a"),
            MQTTTopic.encode_subscription(Message.type_name(), "2", "b"),
        ]:
            proactor.links.subscribe(self.PARENT_MQTT, topic, QOS.AtMostOnce)
