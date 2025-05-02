import secrets
from typing import Any, ClassVar, Optional, Sequence

from gwproto import HardwareLayout, Message, MQTTCodec, create_message_model
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
        model_name: str = "",
        module_names: Optional[Sequence[str]] = None,
        use_default_modules: bool = True,
    ) -> None:
        self.src_name = src_name
        self.dst_name = dst_name
        super().__init__(
            self.create_message_model(
                model_name=model_name,
                module_names=module_names,
                use_default_modules=use_default_modules,
            )
        )

    @classmethod
    def create_message_model(
        cls,
        *,
        model_name: str = "",
        module_names: Optional[Sequence[str]] = None,
        use_default_modules: bool = True,
    ) -> type[Message[Any]]:
        module_names_used = []
        if use_default_modules:
            module_names_used.extend(cls.DEFAULT_MESSAGE_MODULES)
        if module_names is not None:
            module_names_used.extend(module_names)
        model_name = (
            model_name if model_name else "ProactorCodec-" + secrets.token_hex(4)
        )
        return create_message_model(
            model_name=model_name, module_names=module_names_used
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

    @classmethod
    def _may_be_event(cls, details: ErrorDetails) -> bool:
        loc: Sequence[str | int] = details.get("loc", [])
        return (
            len(loc) >= 2  # noqa: PLR2004
            and loc[0] == "Payload"
            and isinstance(loc[1], str)
            and loc[1].startswith("gridworks.event")
        )

    @classmethod
    def get_unrecognized_payload_error(
        cls, e: ValidationError
    ) -> Optional[ErrorDetails]:
        super_error = super().get_unrecognized_payload_error(e)
        if super_error is None:
            for error in e.errors():
                if cls._may_be_event(error):
                    return error
        return super_error

    def handle_unrecognized_payload(  # noqa
        self, payload: bytes, e: ValidationError, details: ErrorDetails
    ) -> Message[Any]:
        if self._may_be_event(details):
            try:
                return Message[AnyEvent].model_validate_json(payload)
            except ValidationError as e2:
                raise e2 from e
        return super().handle_unrecognized_payload(
            payload=payload, e=e, details=details
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
            model_name=link.codec.message_model_name,
            module_names=link.codec.message_modules,
            use_default_modules=link.codec.use_default_message_modules,
        )
