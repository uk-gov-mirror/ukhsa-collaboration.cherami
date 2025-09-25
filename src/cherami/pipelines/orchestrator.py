import logging
import threading
import time
import uuid
from collections.abc import Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path

from kubernetes.client.api import BatchV1Api

from cherami.pipelines.base import Pipeline, PipelineResult, SampleQC
from cherami.pipelines.sarscov2 import SARSCoV2Pipeline

logger = logging.getLogger(__name__)


def mock_sample_qc(sample_id: str) -> SampleQC:
    return SampleQC(
        sample_id=sample_id,
        qc_pass=True,
        total_reads=100,
        spike_reads=10,
        taxon_reads={"sarscov2_reads": 1000},
        genus_percentage=0.1,
        species_reads=10,
    )


class PipelineOrchestrator:
    def __init__(self, k8_api: BatchV1Api, max_jobs: int) -> None:
        self.k8_api = k8_api
        self.pipelines = self._init_pipelines()
        self._max_jobs = max_jobs
        self._executor = ThreadPoolExecutor(max_workers=max_jobs)
        self._capacity = threading.Semaphore(max_jobs)
        logger.info("Initialised pipeline worker pool with %s workers", max_jobs)

    def _init_pipelines(self) -> Sequence[Pipeline]:
        ## instantiates all available pipelines into a list of Pipeline objects
        pipelines = [
            SARSCoV2Pipeline(),
        ]
        return pipelines

    def _evaluate_sample(self, sample_id: str) -> list[Pipeline]:
        ## takes a sample id and loops through each pipeline to check if it meets a criteria for that pipeline
        ## returns a list of pipelines that the sample meets the criteria for

        ## TODO: replace this with onyx queries - currently mocked
        sample_qc = mock_sample_qc(sample_id)
        selected_pipelines = []

        for pipeline in self.pipelines:
            if pipeline.evaluate_sample(sample_qc):
                selected_pipelines.append(pipeline)

        return selected_pipelines

    def _execute_pipeline(self, pipeline: Pipeline, sample_id: str) -> PipelineResult:
        ## runs the pipeline logic for the job - creating samplesheet and job manifest
        ## submits this to k8s and waits until completion
        job_id = uuid.uuid4().hex[:8]
        job_name = f"{pipeline.config.name}-{job_id}"
        ## TODO: Implement samplesheet generation logic
        ## Also multi samples for one samplesheet?
        samplesheet_path = None

        try:
            job_manifest = pipeline.create_job_manifest(
                samplesheet_path=samplesheet_path,
                job_id=job_id,
            )

            logger.info(
                "Creating job %s for pipeline %s (sample %s)",
                job_name,
                pipeline.config.name,
                sample_id,
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

                    break
                if resp.status.failed:  # type: ignore
                    logger.error("k8 job %s failed", job_name)
                    job_completed = True
                    break
                logger.debug("k8 job %s still running...", job_name)
                time.sleep(10)
            ## TODO: Decide behaviour on job failure - 3 failure states:
            ## 1. k8 status is failed
            ## 2. pipleine crashes and a process has non-zero exit status (failng the trace check)
            ## 3. exception
            ## retry on failure?
            trace_file = Path(pipeline.config.output_dir) / job_id / "pipeline_trace.txt"
            success = pipeline.evaluate_exit_status(trace_file)
            if success:
                logger.info("Pipeline %s for sample %s completed successfully", pipeline.config.name, sample_id)
            else:
                logger.error("Pipeline %s for sample %s failed", pipeline.config.name, sample_id)
            return PipelineResult(
                pipeline_name=pipeline.config.name,
                job_id=job_id,
                ## be strict on setting success, this ACKS a message which is the point of no return
                success=success,
            )
        except Exception:
            logger.exception("Exception running pipeline %s for sample %s", pipeline.config.name, sample_id)
            return PipelineResult(
                pipeline_name=pipeline.config.name,
                job_id=job_id,
                success=False,
            )
        finally:
            self._capacity.release()

    def submit_job(self, sample_id: str) -> list[tuple[str, Future]] | None:
        """
        Submits a sample to the processing pool.

        Samples are evaluated agaisnt each pipelines criteria, and submites passing pipelines to the execution pool.

        Args:
            sample_id: The sample ID to be processed.

        Returns:
            A tuple containing the sample id and a list of (pipeline_name, future) pairs
            Returns None if no pipelines were selected for the sample.
        """
        selected_pipelines = self._evaluate_sample(sample_id)

        if not selected_pipelines:
            return None

        pipeline_futures = []
        for pipeline in selected_pipelines:
            self._capacity.acquire()
            future = self._executor.submit(self._execute_pipeline, pipeline, sample_id)
            pipeline_futures.append((pipeline.config.name, future))

        logger.info(
            "Submitted sample %s to pipelines: %s",
            sample_id,
            "|".join(p.config.name for p in selected_pipelines),
        )

        return pipeline_futures

    def shutdown(self, *, cancel_k8s: bool = False) -> None:
        """
        Shuts down the orchestrator.

        Args:
            cancel_k8s: If True, will cancel running k8s jobs.
        """
        logger.info("Shutting down pipeline worker pool")
        self._executor.shutdown(wait=True, cancel_futures=True)
        ## TODO: k8s job cancel logic
