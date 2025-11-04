from pathlib import Path
from typing import Any

from cognite.neat.core._data_model.models import UnverifiedPhysicalDataModel

from .framework.importer._cfihos2data_model import CFIHOSImporter


class CFIHOSReader:
    """A reader class for processing CFIHOS configuration files and generating an unverified physical data model.

    This class initializes with a configuration file path and provides a method to read and convert
    the CFIHOS model into a physical data model using the specified importer.

    Args:
        filepath (Path): The path to the configuration file.
        **kwargs: Additional keyword arguments passed to the cfihos manager.
    """

    def __init__(self, filepath: Path, **kwargs: Any) -> None:
        """Initialize the CFIHOSReader.

        Args:
            filepath (Path): The path to the configuration file.
            **kwargs: Additional keyword arguments passed to the cfihos manager.
        """
        self.addtional_parameters_dict = kwargs
        self.config = filepath
        self.importer = CFIHOSImporter(configFilePath=self.config, **kwargs)

    def read(self) -> UnverifiedPhysicalDataModel:
        """Read the CFIHOS model and return a PhysicalDataModel."""
        return self.importer.to_data_model()
