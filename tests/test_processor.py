"""Unit tests for _processor.py."""

from pathlib import Path
from unittest.mock import Mock, patch

from cognite.neat.core._data_model._shared import ImportedDataModel
from cognite.neat.core._data_model.models import UnverifiedPhysicalDataModel

from cognite.neat_cfihos_handler._processor import CFIHOSProcessor


class TestCFIHOSProcessor:
    """Test suite for CFIHOSProcessor."""

    @patch("cognite.neat_cfihos_handler._processor.NeatReader")
    @patch("cognite.neat_cfihos_handler._reader.CFIHOSReader")
    def test_init_with_configuration_dir(self, mock_reader, mock_neat_reader):
        """Test initialization with configurationDir."""
        config_dir = Path("/test/config")
        mock_neat_reader_instance = Mock()
        mock_neat_reader_instance.materialize_path.return_value = Path(
            "/materialized/path"
        )
        mock_neat_reader.create.return_value = mock_neat_reader_instance

        processor = CFIHOSProcessor(
            configurationDir=config_dir, model_type="containers"
        )

        mock_neat_reader.create.assert_called_once_with(config_dir)
        mock_neat_reader_instance.materialize_path.assert_called_once()
        assert processor.configurationPath == Path("/materialized/path")
        assert processor.addtional_parameters_dict == {"model_type": "containers"}

    @patch("cognite.neat_cfihos_handler._processor.NeatReader")
    def test_init_without_configuration_dir(self, mock_neat_reader):
        """Test initialization without configurationDir."""
        mock_neat_reader_instance = Mock()
        mock_neat_reader_instance.materialize_path.return_value = Path("/default/path")
        mock_neat_reader.create.return_value = mock_neat_reader_instance

        processor = CFIHOSProcessor(model_type="containers")

        mock_neat_reader.create.assert_called_once_with(None)
        assert processor.configurationPath == Path("/default/path")

    @patch("cognite.neat_cfihos_handler._processor.CFIHOSReader")
    @patch("cognite.neat_cfihos_handler._processor.NeatReader")
    def test_to_data_model(self, mock_neat_reader, mock_reader):
        """Test to_data_model method."""
        mock_neat_reader_instance = Mock()
        # Use an arbitrary path that doesn't need to exist
        arbitrary_path = Path("/arbitrary/test/path")
        mock_neat_reader_instance.materialize_path.return_value = arbitrary_path
        mock_neat_reader.create.return_value = mock_neat_reader_instance

        mock_reader_instance = Mock()
        mock_data_model = Mock(spec=UnverifiedPhysicalDataModel)
        mock_imported = ImportedDataModel(mock_data_model, {})
        mock_reader_instance.read.return_value = mock_imported
        mock_reader.return_value = mock_reader_instance

        processor = CFIHOSProcessor(
            configurationDir=Path("/test"), model_type="containers", scope="test"
        )
        result = processor.to_data_model()

        mock_reader.assert_called_once_with(
            filepath=arbitrary_path, model_type="containers", scope="test"
        )
        mock_reader_instance.read.assert_called_once()
        assert result == mock_imported
