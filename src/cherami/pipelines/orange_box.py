import logging
from pathlib import Path

from cherami.pipelines.base import BasePipeline, PipelineConfig
from cherami.pipelines.registry import register_pipeline

logger = logging.getLogger(__name__)


@register_pipeline
class OrangeBoxPipeline(BasePipeline):
    @property
    def config(self) -> PipelineConfig:
        return PipelineConfig(
            name="orange_box",
            version="0.1.0",
            path="nf-core/demo",
            cpus=4,
            mem="8G",
            cpu_limit=4,
            mem_limit="8G",
            nf_config_path=Path("/shared/team/projects/downstream_orchestration/nextflow.config"),
            nf_profiles=["docker", "test"],
            nf_extra_args=[],
            work_dir=Path("/shared/team/projects/downstream_orchestration/test/work"),
            output_dir=Path("/shared/team/projects/downstream_orchestration/test/output"),
            namespace="ns-synthscape-ukhsa",
            container="quay.io/climb-tre/nextflow",
            backoff_limit=5,
            max_retries=1,
            retry_timeout=10,
            job_timeout=3600,
        )

    def generate_samplesheet(self, samples: list[str], job_id: str) -> str | None:
        # TODO
        return
