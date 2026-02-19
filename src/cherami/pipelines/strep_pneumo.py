import csv
import logging
from pathlib import Path

from onyx import OnyxClient

from cherami.config import PipelineConfig, WorkerConfig
from cherami.pipelines.pipeline import Pipeline
from cherami.pipelines.worker import Worker
from cherami.utils import init_onyx

logger = logging.getLogger(__name__)


class StrepPneumoPipeline(Pipeline):
    def generate_samplesheet(
        self, samples: list[str], job_id: str, output_filepath: Path
    ) -> None:
        config = init_onyx()
        rows = []
        with OnyxClient(config) as client:
            for climb_id in samples:
                climb_records = client.get(
                    project="synthscape",
                    climb_id=climb_id,
                    include=[
                        "human_filtered_reads_1",
                        "human_filtered_reads_2",
                        "taxon_reports",
                    ],
                )
                if not climb_records:
                    raise ValueError("no_records_found")
                # Pneumokity requires 2 fastqs as input so for SE pass same fastq twice
                if climb_records["human_filtered_reads_2"] == "":
                    climb_records["human_filtered_reads_2"] = climb_records["human_filtered_reads_1"]
                try:
                    row = {
                        "climb_id": climb_id,
                        "fastq_1": climb_records["human_filtered_reads_1"],
                        "fastq_2": climb_records["human_filtered_reads_2"],
                        "kraken_output": f"{climb_records['taxon_reports']}{climb_id}_PlusPF.kraken_assignments.tsv",
                        "kraken_report": f"{climb_records['taxon_reports']}{climb_id}_PlusPF.kraken_report.txt",
                    }
                except KeyError as e:
                    raise ValueError(
                        f"missing_expected_data: {e.args[0]}"
                    ) from e
                rows.append(row)

        if not rows:
            raise ValueError("samplesheet_generation_no_records")

        fieldnames = list(rows[0].keys())
        with output_filepath.open("w") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        logger.debug(
            "Generated Strep pneumo samplesheet at %s",
            output_filepath,
        )


def build_worker(
    worker_config: WorkerConfig,
    pipeline_config: PipelineConfig,
    work_dir: Path,
    output_dir: Path,
    audit_db_path: Path,
) -> Worker:
    pipeline = build_pipeline(pipeline_config)
    return Worker(
        worker_config,
        pipeline,
        work_dir,
        output_dir,
        audit_db_path=audit_db_path,
    )


def build_pipeline(pipeline_config: PipelineConfig) -> Pipeline:
    return StrepPneumoPipeline(pipeline_config)
