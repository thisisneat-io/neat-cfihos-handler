"""This module provides an importer for CFIHOS definitions.

This module loads and parses CFIHOS data from configuration files and associated CSV/Excel sources.
It instantiates the appropriate CFIHOS manager based on the configuration, validates the configuration,
and produces an UnverifiedPhysicalDataModel for further processing in the NEAT framework.
"""

import pathlib
from pathlib import Path
from typing import Any, cast

from cognite.neat.core._data_model._shared import (
    ImportedDataModel,
    UnverifiedPhysicalDataModel,
)
from cognite.neat.core._data_model.importers import BaseImporter
from cognite.neat.core._issues import IssueList, MultiValueError
from cognite.neat.core._issues.errors import (
    FileNotFoundNeatError,
    FileReadError,
    NeatValueError,
)

from cognite.neat_cfihos_handler.framework.common.log import log_init
from cognite.neat_cfihos_handler.framework.common.reader import read_yaml
from cognite.neat_cfihos_handler.framework.processing.model_managers.base_cfihos_manager import (
    BaseCfihosManager,
)
from cognite.neat_cfihos_handler.framework.processing.model_managers.model_manager_provider import (
    CfihosManagerProvider,
)

LOG_LEVEL = "i"  # Define a constant for the log level
logging = log_init(f"{__name__}", LOG_LEVEL)


class CFIHOSImporter(BaseImporter[UnverifiedPhysicalDataModel]):
    """Import CFIHOS definitions from configuration files.

    Args:
        configFilePath (Path): The path to the configuration file.
    """

    def __init__(self, configFilePath: Path, **kwargs: Any) -> None:
        """Initialize the CFIHOS importer with a configuration file path.

        Args:
            configFilePath (Path): The path to the configuration file.
            **kwargs: Additional keyword arguments passed to the cfihos manager.

        Raises:
            TypeError: If configFilePath is not a Path object.
            NeatValueError: If the configuration file is empty or does not contain the 'processor_type' key.
        """
        if not isinstance(configFilePath, Path):
            raise TypeError(
                f"Expected 'configFilePath' to be of type 'Path', got {type(configFilePath).__name__}"
            )
        self.configFilePath = configFilePath
        self.kwargs = kwargs
        self.manager: BaseCfihosManager = self._get_cfihos_manager()

    def _get_cfihos_manager(self) -> BaseCfihosManager:
        """Return the CFIHOS Manager based on the configuration file."""
        model_config = self._get_model_config(self.configFilePath)
        if not model_config:
            raise NeatValueError(
                f"Configuration file '{self.configFilePath}' is empty or not found."
            )
        if "processor_type" not in model_config:
            raise NeatValueError(
                f"Configuration file '{self.configFilePath}' does not contain 'processor_type' key."
            )

        managerProvider: CfihosManagerProvider = CfihosManagerProvider(
            manager_name=model_config["processor_type"],
            processor_config=model_config,
            **self.kwargs,
        )
        T_Manager = managerProvider.get_manager()
        return T_Manager

    def _get_model_config(self, path_to_config: Path) -> dict:
        """Return the configuration information related to a domain model."""
        config_file_exists = pathlib.Path.is_file(path_to_config)

        if not config_file_exists:
            raise FileNotFoundError(f"Could not find '{str(path_to_config)}'")

        return read_yaml(str(path_to_config))

    def to_data_model(self) -> ImportedDataModel[UnverifiedPhysicalDataModel]:
        """Convert CFIHOS configuration to an unverified physical data model.

        Returns:
            ImportedDataModel[UnverifiedPhysicalDataModel]: The imported data model.

        Raises:
            FileNotFoundNeatError: If the configuration file doesn't exist.
            MultiValueError: If there are processing errors.
        """
        issue_list = IssueList(title=f"'{self.configFilePath.name}'")
        issue_list.extend(self.manager.processor_issue_list)
        if not self.configFilePath.exists():
            raise FileNotFoundNeatError(self.configFilePath)

        cfihos_read = self.manager.read_model()

        issue_list.trigger_warnings()
        if issue_list.has_errors:
            raise MultiValueError(issue_list.errors)

        if cfihos_read is None:
            return ImportedDataModel(None, {})

        data_model_cls = UnverifiedPhysicalDataModel
        data_model = cast(
            UnverifiedPhysicalDataModel, data_model_cls.load(cfihos_read.__dict__)
        )

        # Delete the temporary file if it was created
        if "temp_neat_file" in self.configFilePath.name:
            try:
                self.configFilePath.unlink()
            except Exception as e:
                issue_list.append(
                    FileReadError(
                        self.filepath, f"Failed to delete temporary file: {e}"
                    )
                )

        return ImportedDataModel(data_model, {})
