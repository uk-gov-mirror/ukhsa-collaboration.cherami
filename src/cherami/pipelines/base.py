import csv
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    pipeline_name: str
    job_id: str
    success: bool


@dataclass
class PipelineConfig:
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


@dataclass
class PipelineCriteria:
    min_taxon_reads: int
    min_total_reads: int
    require_spike: bool
    qc_pass: bool
    percentage_genus: float
    min_total_species_reads: int


@dataclass
class SampleQC:
    sample_id: str
    qc_pass: bool
    total_reads: int
    spike_reads: int
    taxon_reads: dict[str, int]
    genus_percentage: float = 0.0
    species_reads: int = 0


class Pipeline(ABC):
    def __init__(
        self,
        config: PipelineConfig,
        criteria: PipelineCriteria,
        proc_names: dict[str, list[int]] | None = None,
    ) -> None:
        self.config = config
        self.critera = criteria
        self.proc_names = proc_names if proc_names is not None else {}
        self._check_paths()

    @abstractmethod
    def generate_samplesheet(self, samples: list[str], job_id: str) -> str | None: ...

    @abstractmethod
    def evaluate_sample(self, sample_qc: SampleQC) -> bool: ...

    def _check_paths(self) -> None:
        if not self.config.work_dir.exists():
            logger.warning("Configured output_dir '%s' does not exist", self.config.work_dir)

        if not self.config.output_dir.exists():
            logger.warning("Configured nf_config_path '%s' does not exist", self.config.output_dir)

        if not self.config.nf_config_path.exists():
            logger.warning("Configured nf_config_path '%s' does not exist", self.config.nf_config_path)

    ## inspired by https://github.com/CLIMB-TRE/roz/blob/bd0ec88b29f9fd0fc18ca1cc500ad385128c121a/roz_scripts/mscape/mscape_ingest_validation.py#L997
    def evaluate_exit_status(self, trace_file: Path) -> bool:
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
                "backoffLimit": 1,
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
