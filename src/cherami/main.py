import json
import logging
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import click

from cherami import utils
from cherami.pipeline_orchestrator import PipelineOrchestrator, SampleRun
from cherami.profiles import get_profile

logger = logging.getLogger(__name__)


def parse_varys_message(varys_client: Any, message: Any) -> tuple[str, str] | None:
    """Parses a varys message, returning the sample ID and job UUID. Returns None if the message is malformed."""
    try:
        deserialised_message = json.loads(message.body)
    except json.JSONDecodeError:
        logger.error("Failed to load message: %s", message)
        ## TODO: decide what to do with bad messages - do we NACK them or just discard?
        ## I guess nacking wont really help here if its a permanent issue with the message
        ## So do we acknowledge and just log?
        logger.error("Acknowledging malformed message")
        varys_client.acknowledge_message(message)
        return None
    sample_id = deserialised_message.get("sample_id")
    job_uuid = deserialised_message.get("uuid")
    return sample_id, job_uuid


def poll_pending_runs(
    *,
    varys_client: Any | None,
    pending_runs: list[tuple[SampleRun, Any | None]],
    on_complete: Callable[[SampleRun, Any | None, Any | None], None],
) -> None:
    """Polls the pending runs, removing any that are complete."""
    for run, varys_message in pending_runs[:]:
        if run.check_complete():
            run.log_sample()
            on_complete(run, varys_client, varys_message)
            pending_runs.remove((run, varys_message))


@click.group()
def cli() -> None:
    pass


@cli.command()
@click.option(
    "--max-samples",
    help="Maximum number of concurrent samples",
    default=2,
    type=int,
)
@click.option(
    "--profile",
    help="Execution profile to use",
    type=str,
    required=True,
)
@click.option("--varys-log", help="Path to varys log", type=click.Path(dir_okay=False), default=Path("./varys.log"))
@click.option(
    "--sample-log",
    help="Path to JSONL file for per-sample results",
    type=click.Path(dir_okay=False, path_type=Path),
    default="./sample_log.jsonl",
)
def watch(max_samples: int, profile: str, varys_log: Path, sample_log: Path) -> None:
    selected_profile = get_profile(profile)
    varys_client = utils.init_varys(selected_profile.varys_config_path, varys_log)
    api_instance = utils.init_kubernetes()
    orchestrator = PipelineOrchestrator(k8_api=api_instance, sample_log=sample_log)

    sample_log.parent.mkdir(parents=True, exist_ok=True)
    pending_runs = []

    try:
        while True:
            ## the basic flow is check first for any completed runs, and handle them first, as listening to varys is
            ## blocking. If there are no messages after the timeout, poll again and wait. If there is a message,
            # parse it and select the pipelines to run based on the profile. Selected pipelines are then submitted to
            # the orchestrator, and are polled for completion in the next loop iteration.

            ## profiles can define custom logic to run when a sample is complete, which is passed to `poll_pending_runs`
            ## as a callable to be run once a sample completes.

            poll_pending_runs(
                varys_client=varys_client, pending_runs=pending_runs, on_complete=selected_profile.on_sample_complete
            )

            ## this is blocking, so loop only continues when a message is received or the timeout is reached
            varys_message = varys_client.receive(
                exchange=selected_profile.exchange,
                queue_suffix=selected_profile.queue_suffix,  # type: ignore
                prefetch_count=max_samples,
                timeout=5,
            )

            if not varys_message:
                continue

            parsed_message = parse_varys_message(varys_client, varys_message)
            if parsed_message is None:
                continue
            sample_id, job_uuid = parsed_message

            selected = selected_profile.select_pipelines(sample_id)
            if not selected:
                logger.info("No pipelines selected for sample %s", sample_id)
                varys_client.acknowledge_message(varys_message)
                continue

            run = orchestrator.submit_sample(sample_id=sample_id, job_uuid=job_uuid, pipelines=selected)
            pending_runs.append((run, varys_message))

    except KeyboardInterrupt:
        logger.info("shutdown")
        orchestrator.shutdown()


@cli.command()
@click.option(
    "--max-samples",
    help="Maximum number of concurrent samples",
    default=2,
    type=int,
)
@click.option(
    "--profile",
    help="Execution profile to use",
    type=str,
    required=True,
)
@click.option(
    "--sample-log",
    help="Path to JSONL file for per-sample results",
    type=click.Path(dir_okay=False, path_type=Path),
    default="./sample_log.jsonl",
)
@click.argument("sample_ids", nargs=-1, required=True, type=list[str])
def run(max_samples: int, profile: str, sample_log: Path, sample_ids: list[str]) -> None:
    selected_profile = get_profile(profile)
    api_instance = utils.init_kubernetes()
    orchestrator = PipelineOrchestrator(k8_api=api_instance, sample_log=sample_log)

    sample_log.parent.mkdir(parents=True, exist_ok=True)
    pending_runs = []
    sample_queue = list(sample_ids)

    try:
        while sample_queue or pending_runs:
            while len(pending_runs) < max_samples and sample_queue:
                sample_id = sample_queue.pop(0)
                sample_uuid = str(uuid.uuid4())

                selected = selected_profile.select_pipelines(sample_id)
                if not selected:
                    logger.info("No pipelines selected for sample %s", sample_id)
                    continue

                run = orchestrator.submit_sample(sample_id=sample_id, job_uuid=sample_uuid, pipelines=selected)
                pending_runs.append((run, None))

            poll_pending_runs(
                varys_client=None, pending_runs=pending_runs, on_complete=selected_profile.on_sample_complete
            )

            if pending_runs or sample_queue:
                time.sleep(2)

    except KeyboardInterrupt:
        logger.info("shutdown")
        orchestrator.shutdown()


@cli.command()
@click.option(
    "--profile",
    help="Execution profile to use",
    type=str,
    required=True,
)
@click.argument("sample_ids", nargs=-1, required=True)
def evaluate(profile: str, sample_ids: list[str]) -> None:
    selected_profile = get_profile(profile)

    for sample_id in sample_ids:
        selected_pipelines = selected_profile.select_pipelines(sample_id)

        if not selected_pipelines:
            logger.info("No pipelines selected for sample %s", sample_id)
        else:
            pipeline_names = [p.config.name for p in selected_pipelines]
            logger.info("Pipelines selected for sample %s: %s", sample_id, "|".join(pipeline_names))
