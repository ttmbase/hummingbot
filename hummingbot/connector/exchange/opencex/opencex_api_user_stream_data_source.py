import asyncio
from typing import TYPE_CHECKING, Any, Dict, Optional

from hummingbot.connector.exchange.opencex import opencex_constants as CONSTANTS
from hummingbot.connector.exchange.opencex.opencex_auth import OpencexAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest, WSPlainTextRequest, WSResponse
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.opencex.opencex_exchange import OpencexExchange


class OpencexAPIUserStreamDataSource(UserStreamTrackerDataSource):

    _logger: Optional[HummingbotLogger] = None

    def __init__(
            self,
            auth: OpencexAuth,
            connector: 'OpencexExchange',
            api_factory: WebAssistantsFactory):
        super().__init__()
        self._auth: OpencexAuth = auth
        self._connector = connector
        self._api_factory = api_factory

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange
        """

        ws: WSAssistant = await self._get_ws_assistant()
        async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_CONNECTION_LIMIT_ID):
            await ws.connect(
                ws_url=CONSTANTS.OPENCEX_WS_URI_PRIVATE,
                ping_timeout=CONSTANTS.PING_TIMEOUT,
                message_timeout=CONSTANTS.SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE,
                ws_headers=await self._auth.get_ws_authentication_headers(),
            )

        payload = {"command": "hmac_login"}

        login_request: WSJSONRequest = WSJSONRequest(payload=payload)

        async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_LOGIN_LIMIT_ID):
            await ws.send(login_request)

        response: WSResponse = await ws.receive()
        message = response.data
        if message.get("kind") != "login":
            self.logger().error("Error authenticating the private websocket connection")
            raise IOError("Private websocket connection authentication failed")

        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        try:
            payload = {
                "command": "add_balance",
                "params": {},
            }
            subscribe_account_request: WSJSONRequest = WSJSONRequest(payload=payload)

            payload = {
                "command": "add_opened_orders",
                "params": {}
            }
            subscribe_opened_orders_request: WSJSONRequest = WSJSONRequest(payload=payload)

            payload = {
                "command": "add_closed_orders",
                "params": {}
            }
            subscribe_closed_orders_request: WSJSONRequest = WSJSONRequest(payload=payload)

            # payload = {
            #     "command": "add_user_trades",
            #     "params": {}
            # }
            # subscribe_user_trades_request: WSJSONRequest = WSJSONRequest(payload=payload)

            async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIPTION_LIMIT_ID):
                await websocket_assistant.send(subscribe_account_request)
            async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIPTION_LIMIT_ID):
                await websocket_assistant.send(subscribe_opened_orders_request)
            async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIPTION_LIMIT_ID):
                await websocket_assistant.send(subscribe_closed_orders_request)
            # async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIPTION_LIMIT_ID):
            #     await websocket_assistant.send(subscribe_user_trades_request)
            self.logger().info("Subscribed to private account and orders channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to order book trading and delta streams...")
            raise

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        while True:
            try:
                await super()._process_websocket_messages(
                    websocket_assistant=websocket_assistant,
                    queue=queue)
            except asyncio.TimeoutError:
                ping_request = WSPlainTextRequest(payload="ping")
                await websocket_assistant.send(request=ping_request)

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        if len(event_message) > 0 and "kind" in event_message:
            queue.put_nowait(event_message)

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant
