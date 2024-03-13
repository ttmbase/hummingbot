import sys

from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState

MAX_ID_LEN = 32
SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE = 600
PING_TIMEOUT = 20

DEFAULT_DOMAIN = ""

# URLs

OPENCEX_BASE_URL = "http://opencex:8080/"

# Doesn't include base URL as the tail is required to generate the signature

OPENCEX_SERVER_TIME_PATH = '/api/public/v1/servertime'
OPENCEX_INSTRUMENTS_PATH = '/api/public/v1/pairs'
OPENCEX_FEES_AND_LIMITS_PATH = '/api/public/v1/limits'
OPENCEX_TICKER_PATH = '/api/public/v1/ticker'
OPENCEX_ORDER_BOOK_PATH = '/api/public/v1/orderbook/{pair}'

# Auth required
OPENCEX_PLACE_ORDER_PATH = '/api/public/v1/order'
OPENCEX_ORDER_DETAILS_PATH = '/api/public/v1/order/{order_id}'
OPENCEX_ORDER_CANCEL_PATH = '/api/public/v1/order/{order_id}'
OPENCEX_BALANCE_PATH = '/api/public/v1/balance'
OPENCEX_TRADE_FILLS_PATH = "/api/public/v1/matches"  # todo

# WS
OPENCEX_WS_BASE_DOMAIN = "opencex-wss:8000"
OPENCEX_WS_URI_PUBLIC = f"ws://{OPENCEX_WS_BASE_DOMAIN}/wsapi/v1/live_notifications"
OPENCEX_WS_URI_PRIVATE = f"ws://{OPENCEX_WS_BASE_DOMAIN}/wsapi/v1/live_notifications"


OPENCEX_WS_ACCOUNT_CHANNEL = "balance"
OPENCEX_WS_OPENED_ORDERS_CHANNEL = "opened_orders"
OPENCEX_WS_CLOSED_ORDERS_CHANNEL = "closed_orders"
# OPENCEX_WS_USER_TRADES_CHANNEL = "user_trades"
OPENCEX_WS_PUBLIC_TRADES_CHANNEL = "trades"
OPENCEX_WS_PUBLIC_BOOKS_CHANNEL = "stack"


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
    RateLimit(limit_id=OPENCEX_SERVER_TIME_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=OPENCEX_INSTRUMENTS_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=OPENCEX_TICKER_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=OPENCEX_ORDER_BOOK_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=OPENCEX_PLACE_ORDER_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=OPENCEX_ORDER_DETAILS_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=OPENCEX_ORDER_CANCEL_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=OPENCEX_BALANCE_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=OPENCEX_TRADE_FILLS_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=OPENCEX_FEES_AND_LIMITS_PATH, limit=10, time_interval=1),
]
