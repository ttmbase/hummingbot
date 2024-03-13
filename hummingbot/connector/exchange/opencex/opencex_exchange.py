import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from aiohttp import ContentTypeError
from bidict import bidict

from hummingbot.connector.exchange.opencex import opencex_constants as CONSTANTS, opencex_web_utils as web_utils
from hummingbot.connector.exchange.opencex.opencex_api_order_book_data_source import OpencexAPIOrderBookDataSource
from hummingbot.connector.exchange.opencex.opencex_api_user_stream_data_source import OpencexAPIUserStreamDataSource
from hummingbot.connector.exchange.opencex.opencex_auth import OpencexAuth
from hummingbot.connector.exchange.opencex.opencex_utils import get_opencex_order_state, to_decimal
from hummingbot.connector.exchange_base import s_decimal_NaN
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class OpencexExchange(ExchangePyBase):

    web_utils = web_utils

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 opencex_api_key: str,
                 opencex_secret_key: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True):

        self.opencex_api_key = opencex_api_key
        self.opencex_secret_key = opencex_secret_key
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        super().__init__(client_config_map)

    @property
    def authenticator(self):
        return OpencexAuth(
            api_key=self.opencex_api_key,
            secret_key=self.opencex_secret_key,
            time_provider=self._time_synchronizer)

    @property
    def name(self) -> str:
        return "opencex"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return ""

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ID_LEN

    @property
    def client_order_id_prefix(self):
        return ""

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.OPENCEX_INSTRUMENTS_PATH

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.OPENCEX_INSTRUMENTS_PATH

    @property
    def check_network_request_path(self):
        return CONSTANTS.OPENCEX_SERVER_TIME_PATH

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return False

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        return [OrderType.LIMIT]

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        error_description = str(request_exception)
        is_time_synchronizer_related = '"code":"50113"' in error_description
        return is_time_synchronizer_related

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        # TODO: implement this method correctly for the connector
        # The default implementation was added when the functionality to detect not found orders was introduced in the
        # ExchangePyBase class. Also fix the unit test test_lost_order_removed_if_not_found_during_order_status_update
        # when replacing the dummy implementation
        return "not_found" in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        # TODO: implement this method correctly for the connector
        # The default implementation was added when the functionality to detect not found orders was introduced in the
        # ExchangePyBase class. Also fix the unit test test_cancel_order_not_found_in_the_exchange when replacing the
        # dummy implementation
        return False

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return OpencexAPIOrderBookDataSource(
            trading_pairs=self.trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return OpencexAPIUserStreamDataSource(
            auth=self._auth,
            connector=self,
            api_factory=self._web_assistants_factory)

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:

        is_maker = is_maker or (order_type is OrderType.LIMIT_MAKER)
        fee = build_trade_fee(
            self.name,
            is_maker,
            base_currency=base_currency,
            quote_currency=quote_currency,
            order_type=order_type,
            order_side=order_side,
            amount=amount,
            price=price,
        )
        return fee

    async def _initialize_trading_pair_symbol_map(self):
        try:
            exchange_info = await self._api_get(
                path_url=self.trading_pairs_request_path
            )
            self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)
        except Exception:
            self.logger().exception("There was an error requesting exchange info.")

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        for symbol, symbol_data in exchange_info["data"].items():
            base_sym = symbol_data["base_symbol"]
            quote_sym = symbol_data["quote_symbol"]
            mapping[f"{base_sym}_{quote_sym}"] = combine_to_hb_trading_pair(base=base_sym, quote=quote_sym)
        self._set_trading_pair_symbol_map(mapping)

    async def _update_trading_rules(self):
        exchange_info = await self._api_get(
            path_url=self.trading_rules_request_path,
        )
        trading_rules_list = await self._format_trading_rules(exchange_info)
        self._trading_rules.clear()
        for trading_rule in trading_rules_list:
            self._trading_rules[trading_rule.trading_pair] = trading_rule
        self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)

    async def _format_trading_rules(self, raw_trading_pair_info: List[Dict[str, Any]]) -> List[TradingRule]:
        trading_rules = []
        for pair, pair_data in raw_trading_pair_info.get("data", {}).items():

            rule = TradingRule(
                trading_pair=pair,
                supports_market_orders=False,
                min_order_size=to_decimal(pair_data["min_order_size"]),
                min_price_increment=to_decimal(pair_data["min_price_increment"]),
                min_base_amount_increment=to_decimal(pair_data["min_base_amount_increment"]),
            )
            trading_rules.append(rule)
        return trading_rules

    async def _update_trading_fees(self):
        """
        # TODO implement
        Update fees information from the exchange
        """
        pass

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:

        operation = CONSTANTS.TRADE_TYPES_MAP.get(trade_type)
        if operation is None:
            raise IOError("Trading type RANGE is not supported")

        data = {
            "type": 0,
            "operation": operation,
            "pair": trading_pair,
            "quantity": str(amount),
            "price": str(price),
        }

        order_resp = await self._api_request(
            path_url=CONSTANTS.OPENCEX_PLACE_ORDER_PATH,
            method=RESTMethod.POST,
            data=data,
            is_auth_required=True,
            limit_id=CONSTANTS.OPENCEX_PLACE_ORDER_PATH,
        )
        self.logger().debug(f"Created: {order_resp['id']}")
        return str(order_resp["id"]), self.current_timestamp

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        """
        This implementation specific function is called by _cancel, and returns True if successful
        """
        if not tracked_order.exchange_order_id:
            return False
        try:
            await self._api_request(
                path_url=CONSTANTS.OPENCEX_ORDER_CANCEL_PATH.format(order_id=tracked_order.exchange_order_id),
                method=RESTMethod.DELETE,
                is_auth_required=True,
                limit_id=CONSTANTS.OPENCEX_ORDER_CANCEL_PATH,
            )
        except ContentTypeError:
            pass
        except Exception as e:
            # can not cancel closed order
            if "open_order_cant_cancel" in str(e):
                return True
            raise IOError(f"Error cancelling order {order_id}: {e}")
        self.logger().debug(f"Place cancel: {tracked_order.exchange_order_id} - {order_id}")
        return True

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        pair = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        resp_json = await self._api_request(
            path_url=CONSTANTS.OPENCEX_TICKER_PATH
        )
        ticker_data = resp_json[pair]
        return float(ticker_data["last_price"])

    async def _update_balances(self):
        balances = await self._api_request(
            path_url=CONSTANTS.OPENCEX_BALANCE_PATH,
            is_auth_required=True)

        self._account_available_balances.clear()
        self._account_balances.clear()

        self._update_balance_from_details(balances)

    def _update_balance_from_details(self, balance_dict):
        for symbol, balance_data in balance_dict.items():
            total = balance_data['actual'] + balance_data['orders']
            self._account_balances[symbol] = Decimal(str(total))
            self._account_available_balances[symbol] = Decimal(str(balance_data['actual']))

    async def _request_order_update(self, order: InFlightOrder) -> Dict[str, Any]:
        if not order.exchange_order_id:
            raise IOError("exchange_order_id is empty")

        res = await self._api_request(
            method=RESTMethod.GET,
            path_url=CONSTANTS.OPENCEX_ORDER_DETAILS_PATH.format(order_id=order.exchange_order_id),
            is_auth_required=True,
            limit_id=CONSTANTS.OPENCEX_ORDER_DETAILS_PATH,
        )
        return res

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        # not used
        return []

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        updated_order_data = await self._request_order_update(order=tracked_order)

        new_state = get_opencex_order_state(updated_order_data)

        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(updated_order_data["id"]),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=int(updated_order_data["updated"]),
            new_state=new_state,
        )
        return order_update

    async def _user_stream_event_listener(self):
        async for stream_message in self._iter_user_event_queue():
            try:
                channel = stream_message.get("kind", None)

                if channel in [CONSTANTS.OPENCEX_WS_OPENED_ORDERS_CHANNEL, CONSTANTS.OPENCEX_WS_CLOSED_ORDERS_CHANNEL]:
                    for data in stream_message.get("results", []):
                        order_state = get_opencex_order_state(data)
                        exchange_order_id = str(data["id"])
                        # fillable_order = self._order_tracker.all_updatable_orders_by_exchange_order_id.get(exchange_order_id)
                        updatable_order = self._order_tracker.all_updatable_orders_by_exchange_order_id.get(exchange_order_id)

                        # if (fillable_order is not None
                        #         and order_status in [OrderState.PARTIALLY_FILLED, OrderState.FILLED]):
                        #     fee = TradeFeeBase.new_spot_fee(
                        #         fee_schema=self.trade_fee_schema(),
                        #         trade_type=fillable_order.trade_type,
                        #         percent_token=data["fillFeeCcy"],
                        #         flat_fees=[TokenAmount(amount=Decimal(data["fillFee"]), token=data["fillFeeCcy"])]
                        #     )
                        #     trade_update = TradeUpdate(
                        #         trade_id=str(data["tradeId"]),
                        #         client_order_id=fillable_order.client_order_id,
                        #         exchange_order_id=str(data["ordId"]),
                        #         trading_pair=fillable_order.trading_pair,
                        #         fee=fee,
                        #         fill_base_amount=Decimal(data["fillSz"]),
                        #         fill_quote_amount=Decimal(data["fillSz"]) * Decimal(data["fillPx"]),
                        #         fill_price=Decimal(data["fillPx"]),
                        #         fill_timestamp=int(data["uTime"]) * 1e-3,
                        #     )
                        #     self._order_tracker.process_trade_update(trade_update)

                        if updatable_order is not None:
                            order_update = OrderUpdate(
                                trading_pair=updatable_order.trading_pair,
                                update_timestamp=int(data["updated"]),
                                new_state=order_state,
                                client_order_id=updatable_order.client_order_id,
                                exchange_order_id=str(data["id"]),
                            )
                            self._order_tracker.process_order_update(order_update=order_update)
                            self.logger().debug(
                                f"{updatable_order.client_order_id} - id:{exchange_order_id}, s:{order_state}, q:{data['quantity']}, ql:{data['quantity_left']}")
                        else:
                            self.logger().debug(
                                f"N/A - id:{exchange_order_id}, s:{order_state}, q:{data['quantity']}, ql:{data['quantity_left']}")


                elif channel == CONSTANTS.OPENCEX_WS_ACCOUNT_CHANNEL:
                    balance_dict = stream_message.get("balance", {})
                    self._update_balance_from_details(balance_dict)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in user stream listener loop.")
                await self._sleep(5.0)
