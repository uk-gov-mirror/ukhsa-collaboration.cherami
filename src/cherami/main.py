import json
import logging
from pathlib import Path

import click

from cherami import utils
from cherami.pipelines import PipelineOrchestrator

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--max-jobs",
    help="Maximum number of concurrent pipeline jobs",
    default=2,
    type=int,
)
@click.option(
    "--varys-config",
    help="Path to varys config file",
    type=click.Path(exists=True, dir_okay=False),
    required=True,
)
@click.option("--varys-log", help="Path to varys log", type=click.Path(dir_okay=False), default=Path("./varys.log"))
@click.option(
    "--exchange",
    help="RabbitMQ exchange to consume messages from",
    required=True,
    type=str,
)
def main(max_jobs: int, varys_config: Path, varys_log: Path, exchange: str) -> None:
    varys_client = utils.init_varys(varys_config, varys_log)
    api_instance = utils.setup_k8()
    orchestrator = PipelineOrchestrator(api_instance, max_jobs)
    pending_samples = []

    try:
        while True:
            for sample in pending_samples:
                futures, varys_message = sample
                if all(future.done() for _, future in futures):
                    #'# results is a list of PipelineResult objects which will contain sucess/fail
                    ## current behaviour is to only ACK if all pipelines were successful, indicatted by success=True
                    ## in the returned PipelineResult object
                    ## we might want to consider partial success and requeue failed pipelines only?
                    ## ACKing a message will remove it from queue so should only be done once happy
                    results = [future.result() for _, future in futures]
                    if results and all(result.success for result in results):
                        varys_client.acknowledge_message(varys_message)
                    pending_samples.remove(sample)

            varys_message = varys_client.receive(
                exchange=exchange,
                queue_suffix="cherami",  # type: ignore
            )

            if not varys_message:
                continue

            try:
                deserialised_message = json.loads(varys_message.body)
            except json.JSONDecodeError:
                ## TODO: decide what to do with bad messages - do we NACK them or just discard?
                ## I guess nacking wont really help here if its a permanent issue with the message
                ## So do we acknowledge and just log?
                logger.exception("Malformed message: %s", varys_message.body)
                continue

            sample_id = deserialised_message.get("sample_id")

            job = orchestrator.submit_job(sample_id)

            if job is None:
                ## nothing passed the QC so ack and log
                logger.info("No pipelines selected for sample %s", sample_id)
                varys_client.acknowledge_message(varys_message)
                continue

            ## append the original message so we can ACK/NACK it later
            pending_samples.append((job, varys_message))

    except KeyboardInterrupt:
        logger.info("shutdown")
        orchestrator.shutdown()
