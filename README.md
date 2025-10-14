[![Docker](https://github.com/ukhsa-collaboration/cherami/actions/workflows/docker-build.yml/badge.svg)](https://github.com/ukhsa-collaboration/cherami/actions/workflows/docker-build.yml)
[![Tests](https://github.com/ukhsa-collaboration/cherami/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/ukhsa-collaboration/cherami/actions/workflows/ci.yml)

# cherami

cherami is the mSCAPE orchestration module for pathogen pipelines.

## Usage

```
Usage: cherami [OPTIONS] COMMAND [ARGS]...

Commands:
  evaluate
  run
  watch
```

### Watch
```
Usage: cherami watch [OPTIONS]

Options:
  --max-samples INTEGER  Maximum number of concurrent samples
  --profile TEXT         Execution profile to use  [required]
  --varys-log FILE       Path to varys log
  --sample-log FILE      Path to JSONL file for per-sample results
  --help                 Show this message and exit.
  ```


## Development

### Installation for development
The reccomended way of installing this repo is using uv:

```bash
git clone https://github.com/ukhsa-collaboration/cherami
cd cherami
uv run pre-commit install
uv run pytest
```

However other methods (such as conda or venv) will work:

```bash
git clone https://github.com/ukhsa-collaboration/cherami
cd cherami
conda create -n cherami python=3.12 "pip>=25.1"
conda activate cherami
pip install --group dev
pre-commit install
pytest
```

This repo uses ruff for formatting and linting, enforced via CI and a pre-commit hook both of which are included in the dev dependencies.

### Setting up a local RabbitMQ server for development

A RabbitMQ pod can be created using `deploy_rabbitmq.sh` helper in `./scripts`. This creates a kubernetes pod running a RabbitMQ server and prints its IP address.

To create a new exchange you can use the CLI tool `rabbitmqadmin` from the container:

```bash
kubectl exec -it rabbitmq -- /bin/bash 
rabbitmqadmin -u admin -p password declare exchange name=cherami_test type=fanout durable=true
```

You will need to update the varys config file to point to the IP of the local pod.

An example varys config file for this configuration:
```json
{
  "version": "0.1",
  "profiles": {
    "cherami": {
      "username": "admin",
      "password": "password",
      "amqp_url": "10.0.0.1",
      "port": 5672,
      "use_tls": false
    }
  }
}
```

Then run `cherami` to listen to messages sent on the created exchange

An example helper script to test payloads is included in `./scripts/send.py` e.g:
```bash
uv run scripts/send.py
```

### Pipelines and Profiles

cherami can be broken down into 2 major components that interact with eachother, Pipelines and Profiles.

#### Profiles

Profiles control what pipelines to run for a given sample, as well as other things like the RabbitMQ message queue to listen to or what should happen once a sample completes. Profiles allow pipelines to run with different message queues, or different pipeline selection criteria depending on the use case. They are implemented in the profiles subpackage (`src/cherami/profiles`). 

Each profile should inherit from the profile base class, which defines key things each profile requires. The 2 important abstract methods are `select_pipelines` which should return a list of `Pipeline` objects to be run on a given sample, and `on_sample_complete` which should implement any actions to run once a sample completes, e.g. Log a new message to a "completed" message queue. As these are abstract it is up to the developer of the profile to provide the actual logic for both of these.

##### Adding a new Profile

###### 1. Defining the profile

Profiles should be added as a new module in the `profiles` subpackage with an appropriate name. For example `decision_logic.py`

All profiles must inherit the base `BaseProfile` class, and thus a basic implementation would look like so:
```python
from cherami.profiles.base import BaseProfile
from cherami.profiles.registry import register_profile

@register_profile
class DecisionLogicProfile(BaseProfile): ...
```

The `BaseProfile` class requires implementation of each of its abstract methods/properties.

Extending the class above, a full implementation would look like so:
```python
from cherami.profiles.base import BaseProfile
from cherami.profiles.registry import register_profile
from cherami.pipelines import available_pipelines

@register_profile
class DecisionLogicProfile(BaseProfile):
    def __init__(self) -> None:
        self._available_pipelines = available_pipelines()

    def select_pipelines(self, sample_id: str) -> Sequence[BasePipeline] | None:
        ## Fetch QC data from Onyx and evaluate against some criteria
        ## Return list of pipelines that should run
        ...

    def on_sample_complete(self, run: SampleRun, varys_client, varys_message) -> None:
        ## Handle completion e.g
        if run.success:
          varys_client.acknowledge_message(varys_message)
        ...

    @property
    def name(self) -> str:
        return "decision_logic"

    @property
    def exchange(self) -> str:
        return "cherami_decision_logic"

    @property
    def queue_suffix(self) -> str:
        return "cherami"

    @property
    def varys_config_path(self) -> Path:
        return Path("./varys.config")
```

###### 2. Registering the profile

Profiles are automatically registered using the `@register_profile` decorator. To make the profile available, import it in `src/cherami/profiles/__init__.py`:

```python
from cherami.profiles.decision_logic import DecisionLogicProfile
```

The profile can then be selected using the `--profile` flag when running cherami.

###### 3. Add tests for the new profile

Each profile should be tested in the `./tests` directory. Tests should be written for the profile's implementation of `select_pipelines` to ensure samples are correctly evaluated against criteria.

#### Pipelines

If Profiles define when to run a sample, Pipelines define how to run them (via Kubernetes). Pipelines act as a template to define the configuration required to run a sample by specifying things like the compute limits, output paths etc. The ultimate end product of a Pipeline is a [Job Manifest](https://kubernetes.io/docs/concepts/workloads/controllers/job/). When a profile selects a pipeline for a sample, the orchestrator uses the pipeline template to constuct a kubernetes job and submit it.

Pipelines are implemented in the `pipelines` subpackage (`src/cherami/pipelines`). Each pipeline should inherit from the pipeline base class, which defines key things each pipeline requires. It implements 1 abstract method `generate_samplesheet` as each samplesheet might be bespoke to each pipeline. As these are abstract it is up to the developer of the profile to provide the actual logic for both of these.

##### Adding a new Pipeline

###### 1. Defining the pipeline template

`BasePipeline` classes act as templates for a pipeline and define all relevant information, as such all pipelines must inherit the base `BasePipeline` class, and thus a basic implementation would look like so:
```python
from cherami.pipelines.base import BasePipeline, PipelineConfig
from cherami.pipelines.registry import register_pipeline

@register_pipeline
class MpoxPipeline(BasePipeline): ...
```

The base `BasePipeline` class provides methods for things such as filepath validation, completion status checking, and creating job manifests to submit to k8. To facilitate this a `BasePipeline` expects a `PipelineConfig` dataclass (defined in `base.py`), provided when the class is initialized.

Each pipeline should be added as a new module with an appropriate name. For example `mpox.py`

The `PipelineConfig` dataclass contains properties relating to the execution of the pipeline, e.g CPU and memory requirements, and the path to the nextflow pipeline.

These should be defined in the class constructor like so (truncated - check the class definition in `base.py` for all the fields):

```python
from cherami.pipelines.base import BasePipeline, PipelineConfig
from cherami.pipelines.registry import register_pipeline

@register_pipeline
class MpoxPipeline(BasePipeline):
    def __init__(self):
        config = PipelineConfig(
            name="mpox",
            version="0.1.0",
            path="gpha/nf-mpox",
            cpus=4,
            mem="8G",
            ...
        )
        super().__init__(config=config)
```

Additionally the `BasePipeline` class implements an abstract method - `generate_samplesheet` which MUST be implemented by the child class. These are abstract as each pipeline may want to implement custom logic for this i.e. non-standard samplesheets across each pipeline.

Extending the class above, these should be implemented like so:
```python
from cherami.pipelines.base import BasePipeline, PipelineConfig
from cherami.pipelines.registry import register_pipeline

@register_pipeline
class MpoxPipeline(BasePipeline):
    def __init__(self): ...

    def generate_samplesheet(self, samples: list[str], job_id: str) -> str | None:
        ## TODO: Implement the construction logic here
        return
```

###### 2. Registering the pipeline

Pipelines are automatically registered using the `@register_pipeline` decorator. To make the pipeline available, import it in `src/cherami/pipelines/__init__.py`:

```python
from cherami.pipelines.mpox import MpoxPipeline
```

###### 3. Add tests for the new pipeline.

Each pipeline is tested in the `./tests` directory. Tests should be written for the pipelines implementation of `generate_samplesheet`.