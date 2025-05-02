from pydantic_settings import BaseSettings

# 	publish 	MY_LONG_NAME/to/PEER_SHORT_NAME
# 	subscribe	PEER_LONG_NAME/to/MY_SHORT_NAME
#
# 	subscription topic:
# 		src: peer gnode_name/long_name
# 		dst: link subscription_name or link mgr subscription_name
#
#
# 	publication topic
# 		src: self.long_name or message.Header.Src
# 		dst: peer.short_name or message.Header.Dst


class CodecSettings(BaseSettings):
    message_model_name: str = ""
    message_modules: list[str] = []
    use_default_message_modules: bool = True


class LinkSettings(BaseSettings):
    broker_name: str = ""
    enabled: bool = True
    peer_long_name: str = ""
    peer_short_name: str = ""
    link_subscription_short_name: str = ""
    codec: CodecSettings = CodecSettings()
    upstream: bool = False
    downstream: bool = False
