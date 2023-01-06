import requests
from typing import Any
import oauthlib.oauth1
from flatten_dict import flatten
from op_dispatcher.constants import (
    CONFIG,
    METHOD_URL,
    OAUTH1_ENV,
    NETSUITE_ENV,
    ConfigFields
)
from op_dispatcher.conf import get_logger


logger = get_logger()
ENV = CONFIG.get(ConfigFields.ENVIRONMENT.value).title()


def make_api_call(
    url: str,
    method: str,
    header: str,
    data: str = None,
    timeout: int = 10,
    default_ciphers: str = None
) -> requests.Response:

    # For Techdata request add ciphers
    if default_ciphers:
        logger.debug("Adding ciphers in request")
        requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = default_ciphers

    try:
        response = requests.request(
            url=url,
            method=method,
            headers=header,
            timeout=timeout,
            data=data if method == "POST" else None
        )
    except Exception as e:
        logger.error(
            "Error during request to vendor for order status",
            exc_info=True
        )
        raise Exception(e)
    return response


def setup_netsuite_api_config(self) -> Any:
    logger.debug("Flattening the config json for Oauth1 creation")
    flat_request_config = flatten(self.request_config, reducer='dot')

    if not CONFIG.get(ConfigFields.ENVIRONMENT.value):
        raise Exception("ENVIRONMENT field missing in config file")

    if (
        CONFIG.get(ConfigFields.ENVIRONMENT.value)
        and not NETSUITE_ENV.get(ENV)
    ):
        raise Exception(
            "ENVIRONMENT field in config file is missing or is invalid"
        )

    logger.debug("Reading values needed for Oauth1 creation")
    consumerKey = flat_request_config.get(
        'api_request_template.%s.consumerKey' % OAUTH1_ENV.get(ENV)
    )
    consumerSecret = flat_request_config.get(
        'api_request_template.%s.consumerSecret' % OAUTH1_ENV.get(ENV)
    )
    token = flat_request_config.get(
        'api_request_template.%s.token' % OAUTH1_ENV.get(ENV)
    )
    signatureMethod = flat_request_config.get(
        'api_request_template.%s.signatureMethod' % OAUTH1_ENV.get(ENV)
    )
    realm = flat_request_config.get(
        'api_request_template.%s.realm' % OAUTH1_ENV.get(ENV)
    )
    tokenSecret = flat_request_config.get(
        'api_request_template.%s.tokenSecret' % OAUTH1_ENV.get(ENV)
    )

    logger.debug("Creating Oauth1 Client")
    try:
        client = oauthlib.oauth1.Client(
            consumerKey,
            resource_owner_key=token,
            client_secret=consumerSecret,
            resource_owner_secret=tokenSecret,
            realm=realm, signature_method=signatureMethod
        )
    except Exception as e:
        raise e

    return flat_request_config, client


def create_oauth_authorization(self, method: str = 'GET'):
    logger.debug("Preparing Oauth1 Client")
    flat_request_config, client = setup_netsuite_api_config(
        self=self
    )

    logger.debug("Preparing request url on the basis of environment")
    req_url = flat_request_config.get(
        'api_request_template.%s.%s' % (
            METHOD_URL.get(method),
            NETSUITE_ENV.get(ENV)
        )
    )

    url, headers, _ = client.sign(req_url)
    return url, headers, method
