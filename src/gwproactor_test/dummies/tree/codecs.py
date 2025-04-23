from gwproto import HardwareLayout, MQTTCodec, create_message_model

from gwproactor import LinkSettings
from gwproactor.codecs import CodecFactory
from gwproactor.config.proactor_config import ProactorName
from gwproactor_test.dummies.names import DUMMY_ADMIN_NAME
from gwproactor_test.dummies.tree.admin_messages import (
    AdminCommandReadRelays,
    AdminCommandSetRelay,
)


class AdminCodec(MQTTCodec):
    def __init__(self) -> None:
        super().__init__(
            create_message_model(
                model_name="AdminMessageDecoder",
                explicit_types=[AdminCommandSetRelay, AdminCommandReadRelays],
            )
        )

    def validate_source_and_destination(self, src: str, dst: str) -> None: ...


class ScadaCodecFactory(CodecFactory):
    def get_codec(
        self,
        link_name: str,
        link: LinkSettings,
        proactor_name: ProactorName,
        layout: HardwareLayout,  # noqa: ARG002
    ) -> MQTTCodec:
        if link_name == DUMMY_ADMIN_NAME:
            return AdminCodec()
        return super().get_codec(
            link_name=link_name,
            link=link,
            proactor_name=proactor_name,
            layout=layout,
        )
