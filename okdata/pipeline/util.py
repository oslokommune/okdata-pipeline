import functools

from okdata.aws.ssm import get_secret
from okdata.sdk.config import Config


@functools.cache
def sdk_config():
    sdk_config = Config()
    sdk_config.config["client_secret"] = get_secret(
        "/dataplatform/okdata-pipeline/keycloak-client-secret"
    )
    return sdk_config
