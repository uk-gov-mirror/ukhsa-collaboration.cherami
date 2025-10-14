from collections.abc import Sequence
from pathlib import Path
from typing import Any

from cherami.pipeline_orchestrator import SampleRun
from cherami.pipelines import get_pipeline
from cherami.pipelines.base import BasePipeline
from cherami.profiles.base import BaseProfile
from cherami.profiles.registry import register_profile


@register_profile
class OrangeBox(BaseProfile):
    profile_name = "orange_box"

    def select_pipelines(self, sample_id: str) -> Sequence[BasePipeline] | None:
        ## only return the orange box pipeline
        selected = []
        selected.append(get_pipeline("orange_box"))
        return selected

    def on_sample_complete(self, sample_run: SampleRun, varys_client: Any | None, varys_message: Any | None) -> None:
        ## TODO: implement the message queue publishing to varys here
        pass

    @property
    def exchange(self) -> str:
        return "scylla_complete"

    @property
    def queue_suffix(self) -> str:
        return "orange_box_queue"

    @property
    def varys_config_path(self) -> Path:
        return Path("./varys.config.json")
