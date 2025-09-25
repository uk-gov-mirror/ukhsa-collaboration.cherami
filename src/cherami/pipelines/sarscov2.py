import logging
from pathlib import Path

from cherami.pipelines.base import Pipeline, PipelineConfig, PipelineCriteria, SampleQC

logger = logging.getLogger(__name__)


class SARSCoV2Pipeline(Pipeline):
    def __init__(self) -> None:
        config = PipelineConfig(
            name="sarscov2",
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
        )
        criteria = PipelineCriteria(
            min_taxon_reads=1000,
            min_total_reads=10,
            require_spike=True,
            qc_pass=True,
            percentage_genus=0.01,
            min_total_species_reads=5,
        )
        super().__init__(
            config=config,
            criteria=criteria,
        )

    def evaluate_sample(self, sample_qc: SampleQC) -> bool:
        return sample_qc.genus_percentage >= self.critera.percentage_genus

    def generate_samplesheet(self, samples: list[str], job_id: str) -> str | None:
        # TODO
        return
