from pathlib import Path
from typing import Any

from cognite.neat.core._data_model._shared import ImportedDataModel
from cognite.neat.core._data_model.importers import BaseImporter
from cognite.neat.core._data_model.models import UnverifiedPhysicalDataModel
from cognite.neat.core._utils.reader import NeatReader

from ._reader import CFIHOSReader


class CFIHOSProcessor(BaseImporter[UnverifiedPhysicalDataModel]):
    """Processes CFIHOS standard and creates an unverified conceptual data model.

    Args:
        model_type: str
            The type of model to be processed (e.g., "containers", or "views").
        scope: str
            The cfihos scope to be used in buliding the view data model.
        configurationDir: Path, optional
            The directory containing the configuration file.
    """

    def __init__(self, configurationDir: Path = None, **kwargs: Any) -> None:
        """Initialize the CFIHOSProcessor.

        Args:
            model_type (str): The type of model to be processed (e.g., "containers", or "views").
            scope (str): The cfihos scope to be used in building the view data model.
            configurationDir (Path): The directory containing the configuration file.
        """
        self.configurationPath = NeatReader.create(configurationDir).materialize_path()
        self.addtional_parameters_dict = kwargs

    def to_data_model(self) -> ImportedDataModel[UnverifiedPhysicalDataModel]:
        """Convert the CFIHOS standard to a data model."""
        return CFIHOSReader(
            filepath=self.configurationPath, **self.addtional_parameters_dict
        ).read()

    @property
    def description(self) -> str:
        """A brief description of the CFIHOS standard processor."""
        return "CFIHOS Standard Processor"
