import secrets
from typing import Any, ClassVar, Optional, Sequence

from gwproto import HardwareLayout, MessageDecoder, MQTTCodec, create_message_model
from gwproto.messages import AnyEvent
from pydantic import ValidationError
from pydantic_core import ErrorDetails

from gwproactor.config.links import LinkSettings
from gwproactor.config.proactor_config import ProactorName


class ProactorCodec(MQTTCodec):
    DEFAULT_MESSAGE_MODULES: ClassVar[list[str]] = [
        "gwproto.messages",
        "gwproactor.message",
    ]
    src_name: str
    dst_name: str

    def __init__(
        self,
        *,
        src_name: str = "",
        dst_name: str = "",
        module_names: Optional[Sequence[str]] = None,
        use_default_modules: bool = True,
    ) -> None:
        self.src_name = src_name
        self.dst_name = dst_name

        # Combine default and provided modules
        combined_modules = []
        if use_default_modules:
            combined_modules.extend(self.DEFAULT_MESSAGE_MODULES)
        if module_names:
            combined_modules.extend(module_names)

        super().__init__(module_names=combined_modules)

    @classmethod
    def create_message_model(
        cls,
        *,
        module_names: Optional[Sequence[str]] = None,
        use_default_modules: bool = True,
    ) -> MessageDecoder:
        module_names_used = []
        if use_default_modules:
            module_names_used.extend(cls.DEFAULT_MESSAGE_MODULES)
        if module_names is not None:
            module_names_used.extend(module_names)
        # model_name parameter removed - was only used for debugging/introspection
        # and added unnecessary complexity. Codecs are identified by link name.

        return create_message_model(
            module_names=module_names_used
        )

    def validate_source_and_destination(self, src: str, dst: str) -> None:
        if (self.src_name and src != self.src_name) or (
            self.dst_name and dst != self.dst_name
        ):
            raise ValueError(
                "ERROR validating src and/or dst\n"
                f"  exp: {self.src_name} -> {self.dst_name}\n"
                f"  got: {src} -> {dst}"
            )

class CodecFactory:
    # noinspection PyMethodMayBeStatic
    def get_codec(
        self,
        link_name: str,  # noqa: ARG002
        link: LinkSettings,
        proactor_name: ProactorName,
        layout: HardwareLayout,  # noqa: ARG002
    ) -> MQTTCodec:
        return ProactorCodec(
            src_name=link.peer_long_name,
            dst_name=proactor_name.short_name,
            module_names=link.codec.message_modules,
            use_default_modules=link.codec.use_default_message_modules,
        )
