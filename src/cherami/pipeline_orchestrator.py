import json
import logging
import time
from collections.abc import Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from kubernetes.client.api import BatchV1Api

from cherami.pipelines.base import BasePipeline

logger = logging.getLogger(__name__)


@dataclass
class SampleRun:
    """
    Stores the run state of a given sample.

    Contains the list of futures for each samples pipelines that are being run, and is returned from the `submit_sample`
    method.`check_complete` returns true once all pipelines have completed, which sets success to true only if all
    pipelines were successful. results is set based on the return values of each pipelines future -
    and contains true/false and any error messages.
    """

    sample_id: str
    futures: list[tuple[str, Future]]
    sample_log: Path
    pipelines: list[str] = field(default_factory=list)
    submitted_at: datetime = field(default_factory=lambda: datetime.now())
    completed_at: datetime | None = None
    results: list[tuple[bool, list[str]]] | None = None
    success: bool | None = None
    pipeline_errors: dict[str, list[str]] = field(default_factory=dict)

    def handle_complete(self) -> None:
        """Set result and success status on sample completion"""
        results = []
        for pipeline_name, future in self.futures:
            success_status, errors = future.result()
            if errors:
                self.pipeline_errors[pipeline_name] = errors
            results.append((success_status, errors))

        self.results = results
        self.success = all(success for success, _ in results)
        self.completed_at = datetime.now()

    def check_complete(self) -> bool:
        """Returns true when all pipelines have completed."""
        if not all(f.done() for _, f in self.futures):
            return False
        self.handle_complete()
        return True

    def to_json(self) -> dict[str, Any]:
        """Dump SampleRun to JSON"""
        return {
            "sample_id": self.sample_id,
            "pipelines": self.pipelines,
            "pending_pipelines": [name for name, fut in self.futures if not fut.done()],
            "submitted_at": self.submitted_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "success": self.success,
            "pipeline_errors": self.pipeline_errors,
        }

    def log_sample(self) -> None:
        """Logs a sample run"""
        try:
            with self.sample_log.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(self.to_json()))
                fh.write("\n")
        except Exception:
            logger.exception("Failed writing sample log for %s", self.sample_id)


class PipelineOrchestrator:
    """
    Orchestrates the execution of pipelines based on sample evaluations.

    Manages job submissions to Kubernetes and monitors job statuses. `submit_job` evaluates
    pipelines for a sample and submits jobs for each selected pipeline. The main public entry points are
    `submit_sample` and `shutdown`. `submit_sample` returns a `SampleRun` object that is used to track the status
    of all the samples running pipelines. `shutdown` cleans up the thread pool and optionally cancels any running
    k8s jobs.

    Attributes:
        k8_api: Kubernetes BatchV1Api instance for job management.
        _executor: ThreadPoolExecutor for job submissions.
        _sample_log: Path to the sample log file.

    """

    def __init__(
        self,
        k8_api: BatchV1Api,
        sample_log: Path,
    ) -> None:
        self.k8_api = k8_api
        self._executor = ThreadPoolExecutor()
        self._sample_log = sample_log
        logger.info("Initialised orchestrator")

    def _execute_pipeline(self, pipeline: BasePipeline, sample_id: str, job_uuid: str) -> tuple[bool, list[str]]:
        ## Creates the job manifest and submits to k8
        ## Waits for k8 to report job as success/failed and evaluates the nextflow trace file
        ## Returns a PipelineResult object indicating overall success/failure of a pipelines
        job_name = f"{pipeline.config.name}-{job_uuid}"
        samplesheet_path = None
        errors = []

        for retry_attempt in range(pipeline.config.max_retries + 1):
            if retry_attempt > 0:
                logger.info(
                    "Retrying pipeline %s for sample %s after timeout %d seconds (attempt %d/%d)",
                    pipeline.config.name,
                    sample_id,
                    pipeline.config.retry_timeout,
                    retry_attempt + 1,
                    pipeline.config.max_retries + 1,
                )
                time.sleep(pipeline.config.retry_timeout)

            try:
                job_manifest = pipeline.create_job_manifest(
                    samplesheet_path=samplesheet_path,
                    job_id=job_uuid,
                )

                logger.info(
                    "Creating job %s for pipeline %s",
                    job_name,
                    pipeline.config.name,
                )
                self.k8_api.create_namespaced_job(
                    body=job_manifest,
                    namespace=pipeline.config.namespace,
                )

                job_completed = False

                while not job_completed:
                    resp = self.k8_api.read_namespaced_job_status(
                        name=job_name,
                        namespace=pipeline.config.namespace,
                    )

                    if resp.status.succeeded:  # type: ignore
                        logger.info("k8 job %s completed", job_name)
                        job_completed = True

                        trace_file = Path(pipeline.config.output_dir) / job_uuid / "pipeline_trace.txt"
                        success = pipeline.evaluate_exit_status(trace_file)

                        if success:
                            logger.info(
                                "Pipeline %s for sample %s completed successfully",
                                pipeline.config.name,
                                sample_id,
                            )
                            return True, errors
                        else:
                            error_msg = f"trace_evaluation_failure: Pipeline {pipeline.config.name} processes failed"
                            errors.append(error_msg)
                            logger.error(
                                "Pipeline %s for sample %s failed trace evaluation",
                                pipeline.config.name,
                                sample_id,
                            )
                            if retry_attempt == pipeline.config.max_retries:
                                return False, errors
                            break

                    if resp.status.failed and resp.status.failed >= pipeline.config.backoff_limit:  # type: ignore
                        logger.error("k8 job %s exhausted backoff limit", job_name)
                        self.k8_api.delete_namespaced_job(
                            name=job_name,
                            namespace=pipeline.config.namespace,
                            propagation_policy="Foreground",
                        )
                        error_msg = (
                            f"pod_failure: Job {job_name} exhausted backoff limit "
                            f"({pipeline.config.backoff_limit} attempts)"
                        )
                        errors.append(error_msg)
                        job_completed = True
                        if retry_attempt == pipeline.config.max_retries:
                            return False, errors
                        break

                    if (
                        resp.status.start_time  # type: ignore
                        and time.time() - resp.status.start_time.timestamp() > pipeline.config.job_timeout  # type: ignore
                    ):
                        logger.error("k8 job %s timed out", job_name)
                        self.k8_api.delete_namespaced_job(
                            name=job_name,
                            namespace=pipeline.config.namespace,
                            propagation_policy="Foreground",
                        )
                        error_msg = f"pod_failure: Job {job_name} timed out after {pipeline.config.job_timeout} seconds"
                        errors.append(error_msg)
                        job_completed = True
                        if retry_attempt == pipeline.config.max_retries:
                            return False, errors
                        break

                    logger.debug("k8 job %s still running...", job_name)
                    time.sleep(10)

            except Exception as e:
                error_msg = f"exception: {str(e)}"
                errors.append(error_msg)
                logger.exception("Exception running pipeline %s for sample %s", pipeline.config.name, sample_id)
                if retry_attempt == pipeline.config.max_retries:
                    return False, errors

        return False, errors

    def submit_sample(
        self,
        *,
        sample_id: str,
        job_uuid: str,
        pipelines: Sequence[BasePipeline],
    ) -> SampleRun:
        """
        Entry point to submit a sample to the orchestrator. Submits each pipeline to the thread pool, and returns a
        `SampleRun` object to track the status of the sample.

        Args:
            sample_id: Samples CLIMB ID
            job_uuid: UUID to identify a sample
            pipelines: List of Pipeline objects to run for the sample
        Returns:
            SampleRun object, or None if no pipelines were selected.
        """
        pipeline_futures = []
        for pipeline in pipelines:
            pipeline.validate()
            future = self._executor.submit(self._execute_pipeline, pipeline, sample_id, job_uuid)
            pipeline_futures.append((pipeline.config.name, future))

        run = SampleRun(
            sample_id=sample_id,
            futures=pipeline_futures,
            pipelines=[p.config.name for p in pipelines],
            sample_log=self._sample_log,
        )

        logger.info(
            "Submitted sample %s to pipelines: %s",
            sample_id,
            "|".join(p.config.name for p in pipelines),
        )

        return run

    def shutdown(self, *, cancel_k8s: bool = False) -> None:
        logger.info("Shutting down orchestrator thread pool")
        self._executor.shutdown(wait=True, cancel_futures=True)
        # TODO: add k8s job cancel logic when cancel_k8s is True
