import csv
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineConfig:
    """Stores the configuration for a pipeline. Should be implemented for each pipeline separately."""

    ## general
    name: str
    version: str
    path: str
    ## compute
    cpus: int
    mem: str
    cpu_limit: int
    mem_limit: str
    ## nf configs
    nf_config_path: Path
    nf_profiles: list[str]
    nf_extra_args: list[str]
    work_dir: Path
    output_dir: Path
    ## k8 configs
    namespace: str
    container: str
    backoff_limit: int
    max_retries: int
    retry_timeout: int
    job_timeout: int


class BasePipeline(ABC):
    """
    Abstract class for defining a pipeline.

    Attributes:
        config: `PipelineConfig` object containing configuration for the pipeline.
        proc_names: Optional dictionary mapping process names to allowed exit codes for evaluating
                    the nextflow trace file. If not provided, all processes must exit with code 0
                    for the pipeline to be considered successful.
    """

    @property
    @abstractmethod
    def config(self) -> PipelineConfig:
        """Pipeline configuration implemented via a PipelineConfig dataclass."""
        ...

    @property
    def proc_names(self) -> dict[str, list[int]]:
        """
        Nextflow process names mapped to allowed exit codes for trace file evaluation.
        Override if nextflow proccess have allowed exit codes to be handelled in the orchestator
        """
        return {}

    @abstractmethod
    def generate_samplesheet(self, samples: list[str]) -> str | None: ...
    """
    Generates a samplesheet for the pipeline if applicable.
    Args:
        samples: List of sample identifiers.

    Returns:
        Path to the generated samplesheet file or None if not applicable.
    """

    def _check_paths(self) -> None:
        """
        Check if configured paths exist and log warnings if they don't.
        """
        if not self.config.work_dir.exists():
            logger.warning("Configured work_dir '%s' does not exist", self.config.work_dir)

        if not self.config.output_dir.exists():
            logger.warning("Configured output_dir '%s' does not exist", self.config.output_dir)

        if not self.config.nf_config_path.exists():
            logger.warning("Configured nf_config_path '%s' does not exist", self.config.nf_config_path)

    def validate(self) -> None:
        """
        Validate pipeline configuration.
        """
        self._check_paths()

    ## inspired by https://github.com/CLIMB-TRE/roz/blob/bd0ec88b29f9fd0fc18ca1cc500ad385128c121a/roz_scripts/mscape/mscape_ingest_validation.py#L997
    def evaluate_exit_status(self, trace_file: Path) -> bool:
        """
        Parses a nextflow trace file to determine if the pipeline completed successfully.

        By default, all processes must exit with code 0 for the pipeline to be considered successful.
        If `proc_names` is provided, only those processes are checked against their allowed exit codes
        for the pipeline to be considered successful.

        Args:
            trace_file: Path to the nextflow trace file.

        Returns:
            True if the pipeline completed successfully, False otherwise.
        """
        try:
            with trace_file.open("r") as trace_fh:
                reader = csv.DictReader(trace_fh, delimiter="\t")
                ## by default check all processes for exit code 0
                if not self.proc_names:
                    for row in reader:
                        if row["exit"] != "0":
                            logger.warning(
                                "Process %s failed with exit code %s",
                                row["name"],
                                row["exit"],
                            )
                            return False
                    return True
                ## if proc_names provided - determine allowed exit codes per process
                ## this also allows you to only check a subset of processes if you want
                for row in reader:
                    if row["name"] in self.proc_names:
                        allowed_exit_codes = self.proc_names[row["name"]]
                        if int(row["exit"]) not in allowed_exit_codes:
                            logger.warning(
                                "Process %s failed with exit code %s",
                                row["name"],
                                row["exit"],
                            )
                            return False
                return True
        except FileNotFoundError:
            ## TODO: do we re-queue the job if trace file not found?
            ## Decide on wider retry strategy
            logger.error("Trace file %s not found", trace_file)
            return False

    def create_job_manifest(self, samplesheet_path: str | None, job_id: str) -> dict[str, Any]:
        """
        Creates a Kubernetes Job manifest for the pipeline.

        Args:
            samplesheet_path: Path to the samplesheet file, if applicable.
            job_id: Unique job ID for this pipeline run.

        Returns:
            A dictionary representing the Kubernetes Job manifest.
        """
        job_name = f"{self.config.name}-{job_id}"

        job_output_dir = self.config.output_dir / job_id
        nxf_work_dir = self.config.work_dir / job_id
        nxf_home_dir = self.config.work_dir / ".nextflow"

        pod_env_vars = [
            {"name": "NXF_WORK", "value": str(nxf_work_dir)},
            {"name": "NXF_HOME", "value": str(nxf_home_dir)},
        ]

        nextflow_cmd = ["nextflow"]
        nextflow_cmd.extend(["run", str(self.config.path)])

        if self.config.nf_config_path:
            nextflow_cmd.extend(["-c", str(self.config.nf_config_path)])
        if self.config.nf_profiles:
            nextflow_cmd.extend(["-profile", ",".join(self.config.nf_profiles)])
        if self.config.nf_extra_args:
            nextflow_cmd.extend(self.config.nf_extra_args)
        if self.config.output_dir:
            nextflow_cmd.extend(["--outdir", str(job_output_dir)])
        if samplesheet_path:
            nextflow_cmd.extend(["--samplesheet", samplesheet_path])

        command = " ".join(nextflow_cmd)
        logger.info("Nextflow command: %s", command)

        return {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "namespace": self.config.namespace,
            },
            "spec": {
                "ttlSecondsAfterFinished": 120,
                "backoffLimit": self.config.backoff_limit,
                "template": {
                    "spec": {
                        "hostname": job_name,
                        "subdomain": self.config.namespace,
                        "securityContext": {
                            "runAsNonRoot": True,
                            "runAsUser": 1000,
                            "runAsGroup": 1000,
                            "fsGroup": 1000,
                        },
                        "restartPolicy": "Never",
                        "volumes": [
                            {
                                "name": "shared-public",
                                "persistentVolumeClaim": {"claimName": "cephfs-shared-ro-public"},
                            },
                            {
                                "name": "shared-team",
                                "persistentVolumeClaim": {"claimName": "cephfs-shared-team"},
                            },
                        ],
                        "nodeSelector": {"hub.jupyter.org/node-purpose": "user-compute"},
                        "containers": [
                            {
                                "name": job_name,
                                "image": self.config.container,
                                "resources": {
                                    "requests": {
                                        "cpu": str(self.config.cpus),
                                        "memory": self.config.mem,
                                    },
                                    "limits": {
                                        "cpu": str(self.config.cpu_limit),
                                        "memory": self.config.mem_limit,
                                    },
                                },
                                "volumeMounts": [
                                    {
                                        "mountPath": "/shared/public/",
                                        "name": "shared-public",
                                        "readOnly": True,
                                    },
                                    {
                                        "mountPath": "/shared/team/",
                                        "name": "shared-team",
                                    },
                                ],
                                "workingDir": str(self.config.work_dir),
                                "env": pod_env_vars,
                                "args": ["/bin/sh", "-c", command],
                            },
                        ],
                    },
                },
            },
        }
