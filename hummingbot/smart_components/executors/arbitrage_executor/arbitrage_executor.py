import asyncio
import logging
from decimal import Decimal
from functools import lru_cache
from typing import Union

from hummingbot.client.settings import AllConnectorSettings
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.event.events import BuyOrderCreatedEvent, MarketOrderFailureEvent, SellOrderCreatedEvent
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.logger import HummingbotLogger
from hummingbot.smart_components.executors.arbitrage_executor.data_types import ArbitrageConfig, ArbitrageExecutorStatus
from hummingbot.smart_components.executors.position_executor.data_types import TrackedOrder
from hummingbot.smart_components.smart_component_base import SmartComponentBase
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class ArbitrageExecutor(SmartComponentBase):
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @staticmethod
    @lru_cache(maxsize=10)
    def is_amm(exchange: str) -> bool:
        return exchange in sorted(
            AllConnectorSettings.get_gateway_amm_connector_names()
        )

    @property
    def is_closed(self):
        return self.arbitrage_status in [ArbitrageExecutorStatus.COMPLETED, ArbitrageExecutorStatus.FAILED]

    @staticmethod
    def _are_tokens_interchangeable(first_token: str, second_token: str):
        interchangeable_tokens = [
            {"WETH", "ETH"},
            {"WBTC", "BTC"},
            {"WBNB", "BNB"},
            {"WMATIC", "MATIC"},
            {"WAVAX", "AVAX"},
            {"WONE", "ONE"},
        ]
        same_token_condition = first_token == second_token
        tokens_interchangeable_condition = any(({first_token, second_token} <= interchangeable_pair
                                                for interchangeable_pair
                                                in interchangeable_tokens))
        # for now, we will consider all the stablecoins interchangeable
        stable_coins_condition = "USD" in first_token and "USD" in second_token
        return same_token_condition or tokens_interchangeable_condition or stable_coins_condition

    def __init__(self, strategy: ScriptStrategyBase, arbitrage_config: ArbitrageConfig, update_interval: float = 1.0):
        if not self.is_arbitrage_valid(pair1=arbitrage_config.buying_market.trading_pair,
                                       pair2=arbitrage_config.selling_market.trading_pair):
            raise Exception("Arbitrage is not valid since the trading pairs are not interchangeable.")
        connectors = [arbitrage_config.buying_market.exchange, arbitrage_config.selling_market.exchange]
        self.buying_market = arbitrage_config.buying_market
        self.selling_market = arbitrage_config.selling_market
        self.min_profitability = arbitrage_config.min_profitability
        self.order_amount = arbitrage_config.order_amount
        self.max_retries = arbitrage_config.max_retries
        self.arbitrage_status = ArbitrageExecutorStatus.NOT_STARTED

        # Order tracking
        self._buy_order: TrackedOrder = TrackedOrder()
        self._sell_order: TrackedOrder = TrackedOrder()

        self._last_buy_price = Decimal("1")
        self._last_sell_price = Decimal("1")
        self._last_tx_cost = Decimal("1")
        self._cumulative_failures = 0
        super().__init__(strategy, list(connectors), update_interval)

    def is_arbitrage_valid(self, pair1, pair2):
        base_asset1, quote_asset1 = split_hb_trading_pair(pair1)
        base_asset2, quote_asset2 = split_hb_trading_pair(pair2)
        return self._are_tokens_interchangeable(base_asset1, base_asset2) and \
            self._are_tokens_interchangeable(quote_asset1, quote_asset2)

    @property
    def net_pnl(self) -> Decimal:
        if self.arbitrage_status == ArbitrageExecutorStatus.COMPLETED:
            sell_quote_amount = self.sell_order.order.executed_amount_base * self.sell_order.average_executed_price
            buy_quote_amount = self.buy_order.order.executed_amount_base * self.buy_order.average_executed_price
            cum_fees = self.buy_order.cum_fees + self.sell_order.cum_fees
            return sell_quote_amount - buy_quote_amount - cum_fees
        else:
            return Decimal("0")

    @property
    def net_pnl_pct(self) -> Decimal:
        if self.arbitrage_status == ArbitrageExecutorStatus.COMPLETED:
            return self.net_pnl / self.buy_order.order.executed_amount_base
        else:
            return Decimal("0")

    @property
    def buy_order(self) -> TrackedOrder:
        return self._buy_order

    @buy_order.setter
    def buy_order(self, value: TrackedOrder):
        self._buy_order = value

    @property
    def sell_order(self) -> TrackedOrder:
        return self._sell_order

    @sell_order.setter
    def sell_order(self, value: TrackedOrder):
        self._sell_order = value

    async def get_resulting_price_for_amount(self, exchange: str, trading_pair: str, is_buy: bool, order_amount: Decimal):
        return await self.connectors[exchange].get_quote_price(trading_pair, is_buy, order_amount)

    async def control_task(self):
        if self.arbitrage_status == ArbitrageExecutorStatus.NOT_STARTED:
            try:
                trade_pnl_pct = await self.get_trade_pnl_pct()
                fee_pct = await self.get_tx_cost_pct()
                profitability = trade_pnl_pct - fee_pct
                if profitability > self.min_profitability:
                    await self.execute_arbitrage()
            except Exception as e:
                self.logger().error(f"Error calculating profitability: {e}")
        elif self.arbitrage_status == ArbitrageExecutorStatus.ACTIVE_ARBITRAGE:
            if self._cumulative_failures > self.max_retries:
                self.arbitrage_status = ArbitrageExecutorStatus.FAILED
                self.terminate_control_loop()
            else:
                self.check_order_status()

    def check_order_status(self):
        if self.buy_order.order and self.buy_order.order.is_filled and \
                self.sell_order.order and self.sell_order.order.is_filled:
            self.arbitrage_status = ArbitrageExecutorStatus.COMPLETED
            self.terminate_control_loop()

    async def execute_arbitrage(self):
        self.arbitrage_status = ArbitrageExecutorStatus.ACTIVE_ARBITRAGE
        self.place_buy_arbitrage_order()
        self.place_sell_arbitrage_order()

    def place_buy_arbitrage_order(self):
        self.buy_order.order_id = self.place_order(
            connector_name=self.buying_market.exchange,
            trading_pair=self.buying_market.trading_pair,
            order_type=OrderType.MARKET,
            side=TradeType.BUY,
            amount=self.order_amount,
            price=self._last_buy_price,
        )

    def place_sell_arbitrage_order(self):
        self.sell_order.order_id = self.place_order(
            connector_name=self.selling_market.exchange,
            trading_pair=self.selling_market.trading_pair,
            order_type=OrderType.MARKET,
            side=TradeType.SELL,
            amount=self.order_amount,
            price=self._last_sell_price,
        )

    async def get_tx_cost_pct(self) -> Decimal:
        base, quote = split_hb_trading_pair(trading_pair=self.buying_market.trading_pair)
        # TODO: also due the fact that we don't have a good rate oracle source we have to use a fixed token
        base_without_wrapped = base[1:] if base.startswith("W") else base
        buy_fee = await self.get_tx_cost_in_asset(
            exchange=self.buying_market.exchange,
            trading_pair=self.buying_market.trading_pair,
            is_buy=True,
            order_amount=self.order_amount,
            asset=base_without_wrapped
        )
        sell_fee = await self.get_tx_cost_in_asset(
            exchange=self.selling_market.exchange,
            trading_pair=self.selling_market.trading_pair,
            is_buy=False,
            order_amount=self.order_amount,
            asset=base_without_wrapped)
        self._last_tx_cost = buy_fee + sell_fee
        return self._last_tx_cost / self.order_amount

    async def get_buy_and_sell_prices(self):
        buy_price_task = asyncio.create_task(self.get_resulting_price_for_amount(
            exchange=self.buying_market.exchange,
            trading_pair=self.buying_market.trading_pair,
            is_buy=True,
            order_amount=self.order_amount))
        sell_price_task = asyncio.create_task(self.get_resulting_price_for_amount(
            exchange=self.selling_market.exchange,
            trading_pair=self.selling_market.trading_pair,
            is_buy=False,
            order_amount=self.order_amount))

        buy_price, sell_price = await asyncio.gather(buy_price_task, sell_price_task)
        return buy_price, sell_price

    async def get_trade_pnl_pct(self):
        self._last_buy_price, self._last_sell_price = await self.get_buy_and_sell_prices()
        if not self._last_buy_price or not self._last_sell_price:
            raise Exception("Could not get buy and sell prices")
        return (self._last_sell_price - self._last_buy_price) / self._last_buy_price

    async def get_tx_cost_in_asset(self, exchange: str, trading_pair: str, is_buy: bool, order_amount: Decimal, asset: str):
        connector = self.connectors[exchange]
        price = await self.get_resulting_price_for_amount(exchange, trading_pair, is_buy, order_amount)
        if self.is_amm(exchange=exchange):
            gas_cost = connector.network_transaction_fee
            conversion_price = RateOracle.get_instance().get_pair_rate(f"{asset}-{gas_cost.token}")
            return gas_cost.amount / conversion_price
        else:
            fee = connector.get_fee(
                base_currency=asset,
                quote_currency=asset,
                order_type=OrderType.MARKET,
                order_side=TradeType.BUY if is_buy else TradeType.SELL,
                amount=order_amount,
                price=price,
                is_maker=False
            )
            return fee.fee_amount_in_token(
                trading_pair=trading_pair,
                price=price,
                order_amount=order_amount,
                token=asset,
                exchange=connector,
            )

    def process_order_created_event(self, _, market, event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        if self.buy_order.order_id == event.order_id:
            self.buy_order.order = self.get_in_flight_order(self.buying_market.exchange, event.order_id)
            self.logger().info("Buy Order Created")
        elif self.sell_order.order_id == event.order_id:
            self.logger().info("Sell Order Created")
            self.sell_order.order = self.get_in_flight_order(self.selling_market.exchange, event.order_id)

    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        if self.buy_order.order_id == event.order_id:
            self.place_buy_arbitrage_order()
            self._cumulative_failures += 1
        elif self.sell_order.order_id == event.order_id:
            self.place_sell_arbitrage_order()
            self._cumulative_failures += 1

    def to_format_status(self):
        lines = []
        if self._last_buy_price and self._last_sell_price:
            trade_pnl_pct = (self._last_sell_price - self._last_buy_price) / self._last_buy_price
            tx_cost_pct = self._last_tx_cost / self.order_amount
            base, quote = split_hb_trading_pair(trading_pair=self.buying_market.trading_pair)
            lines.extend([f"""
    Arbitrage Status: {self.arbitrage_status}
    - BUY: {self.buying_market.exchange}:{self.buying_market.trading_pair}  --> SELL: {self.selling_market.exchange}:{self.selling_market.trading_pair} | Amount: {self.order_amount:.2f}
    - Trade PnL (%): {trade_pnl_pct * 100:.2f} % | TX Cost (%): -{tx_cost_pct * 100:.2f} % | Net PnL (%): {(trade_pnl_pct - tx_cost_pct) * 100:.2f} %
    -------------------------------------------------------------------------------
    """])
            if self.arbitrage_status == ArbitrageExecutorStatus.COMPLETED:
                lines.extend([f"Total Profit (%): {self.net_pnl_pct * 100:.2f} | Total Profit ({quote}): {self.net_pnl:.4f}"])
            return lines
        else:
            msg = ["There was an error while formatting the status for the executor."]
            self.logger().warning(msg)
            return lines.extend(msg)
