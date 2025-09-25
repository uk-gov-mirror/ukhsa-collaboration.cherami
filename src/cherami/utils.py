import os
from pathlib import Path

from kubernetes.client import Configuration
from kubernetes.client.api import BatchV1Api
from varys import Varys


def init_varys(config_path: Path, log_path: Path) -> Varys:
    return Varys(
        profile="cherami",
        logfile=str(log_path),
        log_level="DEBUG",
        config_path=str(config_path),
        auto_acknowledge=False,
    )


def setup_k8() -> BatchV1Api:
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
