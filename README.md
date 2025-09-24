# cherami

cherami is the mSCAPE orchestration module for pathogen pipelines.

## Usage
```
Usage: cherami [OPTIONS]

Options:
  --max-jobs INTEGER   Maximum number of concurrent pipeline jobs
  --varys-config FILE  Path to varys config file
  --varys-log FILE     Path to varys log
  --help               Show this message and exit.
```


## Development

### Installation for development
The reccomended way of installing this repo is using uv:

```bash
git clone <repo>
cd <repo>
uv run pre-commit install
uv run pytest
```

However other methods (such as conda or venv) will work:

```bash
git clone <repo>
cd <repo>
conda create -n cherami python=3.12 "pip>=25.1"
conda activate cherami
pip install --group dev
pre-commit install
```

This repo uses ruff for formatting and linting, enforced via CI and a pre-commit hook both of which are included in the dev dependencies.

### Adding a new pipeline

#### 1. Defining the pipeline template

Pipelines are implemented in the `pipelines` subpackage (`src/cherami/pipelines`). Each pipeline should be added as a new module with an appropriate name. For example `mpox.py`

`Pipeline` classess act as templates for a pipeline and define all relevant information, as such all pipelines must inherit the base `Pipeline` class, and thus a basic implementation would look like so:
```python
from cherami.pipelines.base import Pipeline
class MpoxPipeline(Pipeline): ...
```

The base `Pipeline` class provides methods for things such as filepath and completion status validation and creating job manifests to submit to k8. To faciliate this a `Pipeline` expects 2 properties - a `PipelineConfig` and `PipelineCriteria` dataclass (defined in `base.py`), provided when the class is initalised.

The `PipelineConfig` dataclass contains properties relating to the execution of the pipeline, e.g CPU and memory requirements, and the path to the nextflow pipeline.

The `PipelineCriteria` dataclass contains porperties relating to the decision logic, and define a criteria for that pipeline to be run, e.g. minimum total reads, percenatge of genus level classifications etc.

These should be defined in the class constructor like so (truncated - check the class definition in `base.py` for all the fields):

```python
from cherami.pipelines.base import Pipeline
class MpoxPipeline(Pipeline):
    def __init__(self):
        config = PipelineConfig(
            name="mpox",
            version="0.1.0",
            path="gpha/nf-mpox",
            cpus=4,
            mem="8G",
            ...
        )
        criteria = PipelineCriteria(
            min_taxon_reads=1000,
            min_total_reads=10,
            ...
        )
        super().__init__(
            config=config,
            criteria=criteria,
        )
```

Additionaly the `Pipline` class implements 2 abstract methods - `evaluate_sample` and `generate_samplesheet` which MUST be implemented by the child class. These are abstract as each pipeline may want to implement custom logic for each of these, i.e. non-standard samplesheets across each pipeline, or evaluation criteria specific to each pipeline.

Extending the class above, these should be implemneted like so:
```python
from cherami.pipelines.base import Pipeline
class MpoxPipeline(Pipeline):
    def __init__(self): ...

    def evaluate_sample(self, sample_qc: SampleQC) -> bool:
        return sample_qc.genus_percentage >= self.critera.percentage_genus

    def generate_samplesheet(self, samples: list[str]):
        ## TODO: Implement the construction logic here
        return
```

NOTE: The `evaluate_sample` method expects a `SampleQC` dataclass. This class is a counterpart to `SampleCriteria` and stores the QC results returned from Onyx for easy comparison.

#### 2. Adding the pipeline to the orchestrator.

Each pipeline is instantiated centerally in the `PipelineOrchestrator` in the `_init_pipelines()` function, returning a list of pipeline objects. New pipelines should be added to this function like so:
```python
## import the new pipeline
from cherami.pipelines.mpox import MpoxPipeline
## add to the orchestrator
class PipelineOrchestrator:
    ...
    def _init_pipelines(self) -> Sequence[Pipeline]:
        pipelines = [
            SARSCoV2Pipeline(),
            MpoxPipeline(),
        ]
        return pipelines

```

The orchestrator will pick up the new pipeline and will now include it in future samples.