from collections.abc import Sequence

from cherami.pipelines.base import BasePipeline

_PIPELINE_REGISTRY: dict[str, BasePipeline] = {}


def register_pipeline(cls) -> type[BasePipeline]:  # noqa: ANN001
    instance = cls()
    _PIPELINE_REGISTRY[instance.config.name] = instance
    return cls


def get_pipeline(name: str) -> BasePipeline:
    if name not in _PIPELINE_REGISTRY:
        raise ValueError(f"Pipeline '{name}' not found.")
    return _PIPELINE_REGISTRY[name]


def available_pipelines() -> Sequence[BasePipeline]:
    return list(_PIPELINE_REGISTRY.values())
