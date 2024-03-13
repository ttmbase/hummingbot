import sys

from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState

MAX_ID_LEN = 32
SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE = 600
PING_TIMEOUT = 20

DEFAULT_DOMAIN = ""

# URLs

TTMBASE_BASE_URL = "http://ttmbase:8080/"

# Doesn't include base URL as the tail is required to generate the signature

TTMBASE_SERVER_TIME_PATH = '/api/public/v1/servertime'
TTMBASE_INSTRUMENTS_PATH = '/api/public/v1/pairs'
TTMBASE_FEES_AND_LIMITS_PATH = '/api/public/v1/limits'
TTMBASE_TICKER_PATH = '/api/public/v1/ticker'
TTMBASE_ORDER_BOOK_PATH = '/api/public/v1/orderbook/{pair}'

# Auth required
TTMBASE_PLACE_ORDER_PATH = '/api/public/v1/order'
TTMBASE_ORDER_DETAILS_PATH = '/api/public/v1/order/{order_id}'
TTMBASE_ORDER_CANCEL_PATH = '/api/public/v1/order/{order_id}'
TTMBASE_BALANCE_PATH = '/api/public/v1/balance'
TTMBASE_TRADE_FILLS_PATH = "/api/public/v1/matches"  # todo

# WS
TTMBASE_WS_BASE_DOMAIN = "ttmbase-wss:8000"
TTMBASE_WS_URI_PUBLIC = f"ws://{TTMBASE_WS_BASE_DOMAIN}/wsapi/v1/live_notifications"
TTMBASE_WS_URI_PRIVATE = f"ws://{TTMBASE_WS_BASE_DOMAIN}/wsapi/v1/live_notifications"


TTMBASE_WS_ACCOUNT_CHANNEL = "balance"
TTMBASE_WS_OPENED_ORDERS_CHANNEL = "opened_orders"
TTMBASE_WS_CLOSED_ORDERS_CHANNEL = "closed_orders"
# TTMBASE_WS_USER_TRADES_CHANNEL = "user_trades"
TTMBASE_WS_PUBLIC_TRADES_CHANNEL = "trades"
TTMBASE_WS_PUBLIC_BOOKS_CHANNEL = "stack"


WS_CONNECTION_LIMIT_ID = "WSConnection"
WS_REQUEST_LIMIT_ID = "WSRequest"
WS_SUBSCRIPTION_LIMIT_ID = "WSSubscription"
WS_LOGIN_LIMIT_ID = "WSLogin"

ORDER_STATE = {
    "live": OrderState.OPEN,
    "filled": OrderState.FILLED,
    "partially_filled": OrderState.PARTIALLY_FILLED,
    "canceled": OrderState.CANCELED,
}

TRADE_TYPES_MAP = {
    TradeType.BUY: 0,
    TradeType.SELL: 1,
}


NO_LIMIT = sys.maxsize

RATE_LIMITS = [
    RateLimit(WS_CONNECTION_LIMIT_ID, limit=10, time_interval=1),
    RateLimit(WS_REQUEST_LIMIT_ID, limit=10, time_interval=1),
    RateLimit(WS_SUBSCRIPTION_LIMIT_ID, limit=10, time_interval=1),
    RateLimit(WS_LOGIN_LIMIT_ID, limit=10, time_interval=1),
    RateLimit(limit_id=TTMBASE_SERVER_TIME_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=TTMBASE_INSTRUMENTS_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=TTMBASE_TICKER_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=TTMBASE_ORDER_BOOK_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=TTMBASE_PLACE_ORDER_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=TTMBASE_ORDER_DETAILS_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=TTMBASE_ORDER_CANCEL_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=TTMBASE_BALANCE_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=TTMBASE_TRADE_FILLS_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=TTMBASE_FEES_AND_LIMITS_PATH, limit=10, time_interval=1),
]
