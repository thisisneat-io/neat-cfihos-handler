"""CFIHOS data model importer plugin."""
from pathlib import Path
from typing import Any

from cognite.neat.plugins.data_model.importers import DataModelImporterPlugin

from ._processor import CFIHOSProcessor

__all__ = ["CFIHOS_2DataModelImporter"]


class CFIHOS_2DataModelImporter(DataModelImporterPlugin):
    """CFIHOS data model importer plugin."""

    def configure(
        self,
        io: Path | None = None,
        configurationDir: str = None,
        **kwargs: Any,
    ) -> CFIHOSProcessor:
        """Import data model from CFIHOS processor.

        Args:
            io (Path): Path to the input file. If None, the default path will be used.

        Keyword Args:
            model_type (str): The type of model to be processed (e.g., "containers", or "views").
            scope (str): The cfihos scope to be used in building the view data model.
            configurationDir (str, optional): The directory containing the configuration file.

        Returns:
            Configured CFIHOSProcessor instance.

        ! note "Why io? when the path is not used"
            The io parameter is not used in this implementation, but it is included
            to maintain consistency with the DataModelImporter interface.
        """
        return CFIHOSProcessor(
            configurationDir=Path(configurationDir) if configurationDir else None,
            **kwargs,
        )
