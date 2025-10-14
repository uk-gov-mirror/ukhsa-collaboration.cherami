import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from cherami.pipeline_orchestrator import SampleRun
from cherami.pipelines.base import BasePipeline

logger = logging.getLogger(__name__)


class BaseProfile(ABC):
    profile_name: str

    @abstractmethod
    ## should return a list of pipelines to run for the given sample_id
    def select_pipelines(self, sample_id: str) -> Sequence[BasePipeline] | None: ...

    @abstractmethod
    ## runs when a sample is completed
    def on_sample_complete(
        self, sample_run: SampleRun, varys_client: Any | None, varys_message: Any | None
    ) -> None: ...

    @property
    @abstractmethod
    ## message queue exchange name to LISTEN to
    def exchange(self) -> str: ...

    @property
    @abstractmethod
    ## needed for varys
    def queue_suffix(self) -> str: ...

    @property
    @abstractmethod
    ## path to the varys config file (maybe different for each profile)
    def varys_config_path(self) -> Path: ...
