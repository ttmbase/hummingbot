import hashlib
import hmac
from decimal import Decimal

from pydantic import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0008"),
    taker_percent_fee_decimal=Decimal("0.0001"),
)

CENTRALIZED = True

EXAMPLE_PAIR = "BTC-USDT"


class OpencexConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="opencex", const=True, client_data=None)
    opencex_api_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your OpenCEX API key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        ),
    )
    opencex_secret_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your OpenCEX secret key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        ),
    )


KEYS = OpencexConfigMap.construct()


def get_opencex_order_state(order_data):
    new_state = OrderState.OPEN
    if order_data["state"] == 1:
        new_state = OrderState.FILLED
    elif order_data["state"] == 2:
        new_state = OrderState.CANCELED
    elif order_data["quantity"] != order_data["quantity_left"]:
        new_state = OrderState.PARTIALLY_FILLED
    return new_state


def generate_signature(api_key, secret_key, timestamp):
    message = api_key + str(timestamp)
    sign = hmac.new(
        secret_key.encode(),
        message.encode('utf-8'),
        hashlib.sha256
    ).hexdigest().upper()
    return sign


def generate_signature_headers(api_key, secret_key, timestamp) -> dict:
    signature = generate_signature(api_key, secret_key, timestamp)

    return {
        'APIKEY': api_key,
        'SIGNATURE': signature,
        'NONCE': timestamp
    }


def get_value_from_chain(chain, *args, default_value=None):
    current = chain
    for key in args:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default_value
    return current


def to_decimal(value):
    return Decimal(str(value))
