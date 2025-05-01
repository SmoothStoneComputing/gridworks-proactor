import asyncio
import copy
from collections import defaultdict
from typing import Any

from aiohttp import web
from aiohttp.typedefs import Handler as HTTPHandler
from aiohttp.web_routedef import RouteDef
from gwproto import Message
from gwproto.type_helpers import WebServerGt
from result import Result

from gwproactor.proactor_interface import AppInterface, Communicator, Runnable


class _RunWebServer:
    config: WebServerGt
    routes: list[RouteDef]

    def __init__(
        self,
        config: WebServerGt,
        routes: list[RouteDef],
    ) -> None:
        self.config = config
        self.routes = routes.copy()

    async def __call__(self) -> None:
        app = web.Application()
        app.add_routes(self.routes)
        runner = web.AppRunner(app)
        try:
            await runner.setup()
            site = web.TCPSite(
                runner,
                host=self.config.Host,
                port=self.config.Port,
                **self.config.Kwargs,
            )
            await site.start()
            while True:  # noqa: ASYNC110
                await asyncio.sleep(10)
        finally:
            try:  # noqa: SIM105
                await runner.cleanup()
            except:  # noqa: E722, S110
                pass


class _WebManager(Communicator, Runnable):
    _configs: dict[str, WebServerGt]
    _routes: dict[str, list[RouteDef]]

    def __init__(self, services: AppInterface) -> None:
        super().__init__("_WebManager", services)
        self._configs = {}
        self._routes = defaultdict(list)

    def process_message(self, message: Message[Any]) -> Result[bool, Exception]:  # noqa: ARG002
        raise ValueError("_WebManager does not currently process any messages")

    def disable(self) -> None:
        self._configs.clear()
        self._routes.clear()

    def start(self) -> None:
        for server_name, server_config in self._configs.items():
            if server_config.Enabled:
                self._services.io_loop_manager.add_io_coroutine(
                    _RunWebServer(
                        config=server_config,
                        routes=self._routes.get(server_name, []),
                    )(),
                    name=f"{self.name}.{server_name}",
                )

    def stop(self) -> None:
        pass

    async def join(self) -> None:
        pass

    def add_web_server_config(
        self,
        *,
        name: str,
        host: str,
        port: int,
        enabled: bool,
        server_kwargs: dict[str, Any],
    ) -> None:
        if name in self._configs:
            raise ValueError(f"ERROR: Server with name '{name}' already exists")
        self._configs[name] = WebServerGt(
            Name=name, Host=host, Port=port, Enabled=enabled, Kwargs=server_kwargs
        )

    def add_web_route(
        self,
        server_name: str,
        method: str,
        path: str,
        handler: HTTPHandler,
        **kwargs: Any,
    ) -> None:
        self._routes[server_name].append(
            RouteDef(
                method=method,
                path=path,
                handler=handler,
                kwargs=kwargs,
            )
        )

    def get_route_strings(self) -> dict[str, list[str]]:
        return {
            config_name: [str(route) for route in self._routes[config_name]]
            for config_name in self._routes
        }

    def get_configs(self) -> dict[str, WebServerGt]:
        return copy.deepcopy(self._configs)
