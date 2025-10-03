import os
from pathlib import Path

from kubernetes.client import Configuration
from kubernetes.client.api import BatchV1Api
from onyx import OnyxConfig, OnyxEnv
from varys import Varys


def init_varys(config_path: Path, log_path: Path) -> Varys:
    return Varys(
        profile="cherami",
        logfile=str(log_path),
        log_level="DEBUG",
        config_path=str(config_path),
        auto_acknowledge=False,
    )


def init_kubernetes() -> BatchV1Api:
    try:
        c = Configuration()
        with Path("/run/secrets/kubernetes.io/serviceaccount/token").open("rt") as token_fh:
            token = token_fh.read()
        c.api_key["authorization"] = token
        c.api_key_prefix["authorization"] = "Bearer"
        c.host = f"https://{os.getenv('KUBERNETES_SERVICE_HOST')}"
        c.ssl_ca_cert = "/run/secrets/kubernetes.io/serviceaccount/ca.crt"  # type: ignore

        Configuration.set_default(c)
        api_instance = BatchV1Api()
        return api_instance
    except Exception as e:
        raise RuntimeError(f"Failed to initialize Kubernetes client: {e}") from e


def init_onyx() -> OnyxConfig:
    try:
        return OnyxConfig(
            domain=os.environ[OnyxEnv.DOMAIN],
            token=os.environ[OnyxEnv.TOKEN],
        )
    except KeyError as e:
        raise ValueError(f"Missing environment variable: {e}") from e
    except Exception as e:
        raise RuntimeError(f"Failed to initialize Onyx client: {e}") from e
