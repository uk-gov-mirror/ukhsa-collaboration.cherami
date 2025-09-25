import logging

from colorama import Fore, Style

__version__ = "0.1.0"


class CustomFormatter(logging.Formatter):
    COLOURS = {
        "DEBUG": Fore.BLUE,
        "INFO": Fore.GREEN,
        "WARNING": Fore.YELLOW,
        "ERROR": Fore.RED,
    }

    def format_time(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        asctime = super().formatTime(record, datefmt)
        return f"{Fore.LIGHTBLACK_EX}{asctime}{Style.RESET_ALL}"

    def format(self, record: logging.LogRecord) -> str:
        levelname = record.levelname
        if levelname in self.COLOURS:
            record.module = f"{Fore.LIGHTBLACK_EX}{record.module}{Style.RESET_ALL}"
            record.levelname = f"{self.COLOURS[levelname]}[{levelname}]{Style.RESET_ALL}"
        return super().format(record)


handler = logging.StreamHandler()
handler.setFormatter(
    CustomFormatter(
        fmt="%(asctime)s::%(module)s::%(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ),
)

logger = logging.getLogger("cherami")
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)
