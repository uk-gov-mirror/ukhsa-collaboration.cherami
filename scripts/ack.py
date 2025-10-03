from pathlib import Path
from varys import Varys
import time


def init_varys(config_path: Path, log_path: Path) -> Varys:
    return Varys(
        profile="cherami",
        logfile=str(log_path),
        log_level="DEBUG",
        config_path=str(config_path),
        auto_acknowledge=False,
    )


varys_client = init_varys(Path("./conf/varys.cfg"), Path("./varys.log"))
while True:
    print("attempt rec")
    varys_message = varys_client.receive(
        exchange="cherami_test",
        queue_suffix="cherami",  # type: ignore
    )
    print(f"Received message: {varys_message}")
    varys_client.acknowledge_message(varys_message)
    print("Acknowledged message")
    time.sleep(1)
