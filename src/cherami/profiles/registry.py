from cherami.profiles.base import BaseProfile

_PROFILE_REGISTRY: dict[str, type[BaseProfile]] = {}


def register_profile(cls: type[BaseProfile]) -> type[BaseProfile]:
    _PROFILE_REGISTRY[cls.profile_name] = cls
    return cls


def get_profile(name: str) -> BaseProfile:
    if name not in _PROFILE_REGISTRY:
        raise ValueError(f"Profile '{name}' not found.")
    return _PROFILE_REGISTRY[name]()
