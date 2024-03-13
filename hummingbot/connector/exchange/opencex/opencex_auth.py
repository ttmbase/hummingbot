from hummingbot.connector.exchange.opencex.opencex_utils import generate_signature_headers
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class OpencexAuth(AuthBase):

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key: str = api_key
        self.secret_key: str = secret_key
        self.time_provider: TimeSynchronizer = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds the server time and the signature to the request, required for authenticated interactions. It also adds
        the required parameter in the request header.

        :param request: the request to be configured for authenticated interaction

        :return: The RESTRequest with auth information included
        """

        headers = {}
        if request.headers is not None:
            headers.update(request.headers)

        timestamp = str(int(self.time_provider.time()))
        headers.update(generate_signature_headers(self.api_key, self.secret_key, timestamp))
        request.headers = headers

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        This method is intended to configure a websocket request to be authenticated. OpenCEX does not use this
        functionality
        """
        return request  # pass-through

    async def get_ws_authentication_headers(self) -> dict:
        timestamp = str(int(self.time_provider.time()) + 1)
        headers = generate_signature_headers(self.api_key, self.secret_key, timestamp)
        return headers
