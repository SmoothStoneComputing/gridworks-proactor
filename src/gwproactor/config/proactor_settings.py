from pydantic_settings import BaseSettings, SettingsConfigDict

MQTT_LINK_POLL_SECONDS = 60.0
ACK_TIMEOUT_SECONDS = 5.0
NUM_INITIAL_EVENT_REUPLOADS: int = 5


class ProactorSettings(BaseSettings):
    mqtt_link_poll_seconds: float = MQTT_LINK_POLL_SECONDS
    ack_timeout_seconds: float = ACK_TIMEOUT_SECONDS
    num_initial_event_reuploads: int = NUM_INITIAL_EVENT_REUPLOADS
    model_config = SettingsConfigDict(
        env_prefix="PROACTOR_",
        env_nested_delimiter="__",
        nested_model_default_partial_update=True,
    )
