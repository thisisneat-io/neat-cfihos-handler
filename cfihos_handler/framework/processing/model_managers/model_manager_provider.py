"""CFIHOS manager provider module.

This module provides the CfihosManagerProvider, which dynamically resolves and instantiates
the appropriate CFIHOS manager class based on a registry and configuration. It supports
extensible manager of CFIHOS data models by mapping manager names to their implementation classes.
"""
from importlib import import_module

from cognite.neat.core._issues.errors import NeatValueError

from cfihos_handler.framework.common.constants import CFIHOS_PROCESSOR_REGISTRY
from cfihos_handler.framework.processing.model_managers.base_cfihos_manager import (
    BaseCfihosManager,
)


def _resolve_manager_class(path: str) -> type[BaseCfihosManager]:
    module_path, _, class_name = path.rpartition(".")
    if not module_path or not class_name:
        raise NeatValueError(f"Invalid manager path: {path}")
    module = import_module(module_path)
    cls = getattr(module, class_name)
    if not issubclass(cls, BaseCfihosManager):
        raise NeatValueError(f"Manager '{class_name}' is not a BaseCfihosManager")
    return cls


class CfihosManagerProvider:
    """Provider for CFIHOS manager instances."""

    _registry: dict[str, type[BaseCfihosManager]] = {
        name: _resolve_manager_class(path)
        for name, path in CFIHOS_PROCESSOR_REGISTRY.items()
    }

    def __init__(self, manager_name: str, processor_config: dict, **kwargs) -> None:
        """Initialize the CFIHOS manager provider."""
        if manager_name not in self._registry:
            available = ", ".join(sorted(self._registry.keys()))
            raise NeatValueError(
                f"Unknown CFIHOS manager: {manager_name}. Available: {available}"
            )
        manager_cls = self._registry[manager_name]
        self.manager: BaseCfihosManager = manager_cls(processor_config, **kwargs)

    def get_manager(self) -> BaseCfihosManager:
        """Get the configured manager instance."""
        return self.manager
