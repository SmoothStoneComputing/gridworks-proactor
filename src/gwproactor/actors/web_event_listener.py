import asyncio
import uuid
from http import HTTPStatus
from typing import Any, Optional

from aiohttp.web_request import Request
from aiohttp.web_response import Response
from gwproto import Message
from gwproto.messages import AnyEvent, EventBase, ProblemEvent
from gwproto.named_types import SpaceheatNodeGt
from gwproto.named_types.web_server_gt import DEFAULT_WEB_SERVER_NAME
from pydantic import BaseModel, ValidationError
from result import Result

from gwproactor.actors.actor import Actor
from gwproactor.proactor_interface import AppInterface
from gwproactor.problems import Problems

EVENT_PATH: str = "/events"


class WebEventListenerSettings(BaseModel):
    http_path: str = EVENT_PATH
    server_name: str = DEFAULT_WEB_SERVER_NAME


class WebEventListener(Actor):
    DEFAULT_NODE_NAME: str = "web-event"

    def __init__(
        self,
        name: str,
        services: AppInterface,
        settings: Optional[WebEventListenerSettings] = None,
    ) -> None:
        super().__init__(name, services)
        if settings is None:
            settings = WebEventListenerSettings()
        self.services.add_web_route(
            server_name=settings.server_name,
            method="POST",
            path=settings.http_path,
            handler=self._handle_web_post,
        )

    async def _handle_web_post(self, request: Request) -> Response:
        status = HTTPStatus.OK
        response_text = ""
        problem_event: Optional[ProblemEvent] = None
        try:
            text = await request.text()
            try:
                event = AnyEvent.model_validate_json(text)
            except ValidationError as validation_error:
                status = HTTPStatus.UNPROCESSABLE_ENTITY
                response_text = validation_error.json(indent=2)
                problem_event = Problems(errors=[validation_error]).problem_event(
                    summary=f"ERROR decoding event <{self.name}>: <{response_text}>"
                )
            else:
                await asyncio.to_thread(self._wait_for_event_processing, event)
        except Exception as e:  # noqa: BLE001
            status = HTTPStatus.INTERNAL_SERVER_ERROR
            problem_event = Problems(errors=[e]).problem_event(
                summary=(
                    f"ERROR handling event post <{self.name}>: " f"{type(e)} <{e}>"
                )
            )
        if problem_event is not None:
            self.services.send_threadsafe(Message(Payload=problem_event))
        return Response(status=status, body=response_text)

    def _wait_for_event_processing(self, event: EventBase) -> None:
        self.services.wait_for_processing_threadsafe(
            Message(Payload=event),
        )

    def process_message(self, message: Message[Any]) -> Result[bool, Exception]:
        raise NotImplementedError("WebEventListener does not process internal messages")

    def start(self) -> None:
        """IOLoop will take care of start."""

    def stop(self) -> None:
        """IOLoop will take care of stop."""

    async def join(self) -> None:
        """IOLoop will take care of shutting down the associated task."""

    @classmethod
    def default_node(
        cls, name: str = DEFAULT_NODE_NAME, parent_name: str = "s"
    ) -> SpaceheatNodeGt:
        return SpaceheatNodeGt(
            ShNodeId=str(uuid.uuid4()),
            Name=name,
            ActorHierarchyName=f"{parent_name}.{name}",
            ActorClass=cls.__name__,
        )
