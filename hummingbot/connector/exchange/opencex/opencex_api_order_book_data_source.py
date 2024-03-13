import asyncio
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.opencex import opencex_constants as CONSTANTS, opencex_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest, WSPlainTextRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.opencex.opencex_exchange import OpencexExchange


class OpencexAPIOrderBookDataSource(OrderBookTrackerDataSource):

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'OpencexExchange',
                 api_factory: WebAssistantsFactory):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot_response: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = int(snapshot_response["timestamp"]) * 1e-3
        update_id: int = int(snapshot_timestamp)

        order_book_message_content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": snapshot_response["bids"],
            "asks": snapshot_response["asks"],
        }
        snapshot_msg: OrderBookMessage = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            order_book_message_content,
            snapshot_timestamp)

        return snapshot_msg

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        pair = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.OPENCEX_ORDER_BOOK_PATH.format(pair=pair)),
            params={"limit": 400},
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.OPENCEX_ORDER_BOOK_PATH,
        )

        return data

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trading_pair = raw_message["pair"]
        snapshot_data = raw_message["stack"]
        snapshot_timestamp: float = int(snapshot_data["ts"])
        update_id: int = int(snapshot_timestamp)

        order_book_message_content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": [(s["price"], s["quantity"]) for s in snapshot_data["buys"]],
            "asks": [(s["price"], s["quantity"]) for s in snapshot_data["sells"]],
        }
        snapshot_msg: OrderBookMessage = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            order_book_message_content,
            snapshot_timestamp)

        message_queue.put_nowait(snapshot_msg)

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trade_updates = raw_message["results"]

        for trade_data in trade_updates:
            trade_timestamp = trade_data["updated"]
            trading_pair = trade_data["pair"]
            message_content = {
                "trade_id": trade_data["id"],
                "update_id": trade_timestamp,
                "trading_pair": trading_pair,
                "trade_type": float(TradeType.BUY.value) if trade_data["operation"] == 0 else float(
                    TradeType.SELL.value),
                "amount": trade_data["quantity"],
                "price": trade_data["price"]
            }
            trade_message: Optional[OrderBookMessage] = OrderBookMessage(
                message_type=OrderBookMessageType.TRADE,
                content=message_content,
                timestamp=trade_timestamp)

            message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        # Opencex never sends diff messages. This method will never be called
        pass

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            for trading_pair in self._trading_pairs:
                payload = {
                    "command": "add_trades",
                    "params": {"pair_name": trading_pair}
                }
                subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=payload)

                payload = {
                    "command": "add_stack",
                    "params": {"pair_name": trading_pair}
                }
                subscribe_orderbook_request: WSJSONRequest = WSJSONRequest(payload=payload)

                await ws.send(subscribe_trade_request)
                await ws.send(subscribe_orderbook_request)

            self.logger().info("Subscribed to public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to order book trading and delta streams...")
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = None
        if "kind" in event_message:
            event_channel = event_message["kind"]
            if event_channel == CONSTANTS.OPENCEX_WS_PUBLIC_TRADES_CHANNEL:
                channel = self._trade_messages_queue_key
            elif event_channel == CONSTANTS.OPENCEX_WS_PUBLIC_BOOKS_CHANNEL:
                channel = self._snapshot_messages_queue_key
        return channel

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        while True:
            try:
                await super()._process_websocket_messages(websocket_assistant=websocket_assistant)
            except asyncio.TimeoutError:
                ping_request = WSPlainTextRequest(payload="ping")
                await websocket_assistant.send(request=ping_request)

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_CONNECTION_LIMIT_ID):
            await ws.connect(
                ws_url=CONSTANTS.OPENCEX_WS_URI_PUBLIC,
                ping_timeout=CONSTANTS.PING_TIMEOUT,
                message_timeout=CONSTANTS.SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE)
        return ws
