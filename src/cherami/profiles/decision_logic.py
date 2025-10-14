import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from onyx import OnyxClient

from cherami.pipeline_orchestrator import SampleRun
from cherami.pipelines import available_pipelines
from cherami.pipelines.base import BasePipeline
from cherami.profiles.base import BaseProfile
from cherami.profiles.registry import register_profile
from cherami.utils import init_onyx

logger = logging.getLogger(__name__)


@register_profile
class DecisionLogicProfile(BaseProfile):
    profile_name = "decision_logic"

    def __init__(self) -> None:
        self._onyx_config = init_onyx()
        self._available_pipelines: Sequence[BasePipeline] = available_pipelines()

        ## Pipeline criteria maps pipeline names to a set of criteria for that pipeline
        ## e.g sarscov2 needs 10 reads total, and 1000 reads mapping to taxon 694009 (SARS-CoV-2)
        self._pipeline_criteria = {
            "sarscov2": {
                "min_total_reads": 10,
                "target_taxa": {694009: 1000},
            },
        }

    def _meets_criteria(self, total_reads: int, taxon_reads: dict[int, int], criteria: dict) -> bool:
        ## compares the fetched QC information to the criteria for a given pipeline
        ## returns True if the criteria are met, False otherwise
        ## TODO: Replace with actual QC fetching once implemented on onyx
        if total_reads < criteria["min_total_reads"]:
            return False
        return all(taxon_reads.get(taxon_id, 0) >= min_reads for taxon_id, min_reads in criteria["target_taxa"].items())

    def select_pipelines(self, sample_id: str) -> Sequence[BasePipeline] | None:
        target_taxa = set()
        for criteria in self._pipeline_criteria.values():
            target_taxa.update(criteria["target_taxa"].keys())

        with OnyxClient(self._onyx_config) as client:
            record = client.get("synthscape", sample_id, include=["classifier_calls"])
            classifier_calls = record.get("classifier_calls", [])

        taxon_reads = dict.fromkeys(target_taxa, 0)
        for classifier_call in classifier_calls:
            taxon_id = classifier_call.get("taxon_id")
            if taxon_id in taxon_reads:
                taxon_reads[taxon_id] = classifier_call.get("count_descendants", 0)

        total_reads = 1000000
        taxon_reads = {694009: 1000000}

        selected = []
        for pipeline in self._available_pipelines:
            criteria = self._pipeline_criteria.get(pipeline.config.name)
            if criteria and self._meets_criteria(total_reads, taxon_reads, criteria):
                selected.append(pipeline)

        return selected

    def on_sample_complete(self, sample_run: SampleRun, varys_client: Any | None, varys_message: Any | None) -> None:
        if sample_run.success:
            logger.info("Sample %s completed successfully", sample_run.sample_id)
            if varys_client and varys_message:
                varys_client.acknowledge_message(varys_message)
        else:
            logger.error("Sample %s failed", sample_run.sample_id)
            for pipeline_name, errors in sample_run.pipeline_errors.items():
                for error in errors:
                    logger.error("Pipeline %s: %s", pipeline_name, error)

    @property
    def exchange(self) -> str:
        return "cherami_test"

    @property
    def queue_suffix(self) -> str:
        return "decision_logic"

    @property
    def varys_config_path(self) -> Path:
        return Path("./varys.config.json")
