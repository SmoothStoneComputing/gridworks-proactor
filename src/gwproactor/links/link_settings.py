from gwproto import Message, MQTTCodec, MQTTTopic

from gwproactor.config import MQTTClient


class LinkConfig:
    client_name: str
    gnode_name: str
    spaceheat_name: str
    subscription_name: str = ""
    mqtt: MQTTClient
    codec: MQTTCodec
    upstream: bool = False
    downstream: bool = False

    def __init__(  # noqa: PLR0913
        self,
        *,
        client_name: str,
        gnode_name: str,
        spaceheat_name: str,
        mqtt: MQTTClient,
        codec: MQTTCodec,
        subscription_name: str = "",
        upstream: bool = False,
        downstream: bool = False,
    ) -> None:
        if upstream and downstream:
            raise ValueError(
                f"ERROR. Link {client_name} can be 0 or 1 of upstream and "
                "downstream, but not both."
            )
        self.client_name = client_name
        self.gnode_name = gnode_name
        self.spaceheat_name = spaceheat_name
        self.subscription_name = subscription_name
        self.upstream = upstream
        self.mqtt = mqtt
        self.codec = codec
        self.downstream = downstream

    def subscription_topic(self, receiver_spaceheat_name: str) -> str:
        return MQTTTopic.encode(
            envelope_type=Message.type_name(),
            src=self.gnode_name,
            dst=receiver_spaceheat_name,
            message_type="#",
        )
