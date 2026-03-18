import pytest
from unittest.mock import MagicMock, patch
from src.loaders.s3_loader import S3Loader


@pytest.fixture
def mock_s3_loader():
    with patch("src.loaders.s3_loader.boto3") as mock_boto3:
        mock_resource = MagicMock()
        mock_boto3.resource.return_value = mock_resource
        loader = S3Loader(
            endpoint_url="http://localhost:9000",
            access_key="test",
            secret_key="test"
        )
        loader.s3 = mock_resource
        loader.client = mock_resource.meta.client
        yield loader, mock_resource


class TestSave:
    def test_save_dict_returns_true(self, mock_s3_loader):
        loader, mock_resource = mock_s3_loader
        mock_resource.Object.return_value.put = MagicMock()
        result = loader.save({"key": "value"}, "bronze", "test/path.json")
        assert result is True

    def test_save_none_returns_false(self, mock_s3_loader):
        loader, _ = mock_s3_loader
        result = loader.save(None, "bronze", "test/path.json")
        assert result is False

    def test_save_empty_bytes_returns_false(self, mock_s3_loader):
        loader, _ = mock_s3_loader
        result = loader.save(b"", "silver", "test/path.parquet")
        assert result is False


class TestExists:
    def test_existing_key_returns_true(self, mock_s3_loader):
        loader, mock_resource = mock_s3_loader
        mock_resource.Object.return_value.load = MagicMock()
        result = loader.exists("silver", "instrument=test/data.parquet")
        assert result is True

    def test_missing_key_returns_false(self, mock_s3_loader):
        loader, mock_resource = mock_s3_loader
        from botocore.exceptions import ClientError
        mock_resource.Object.return_value.load.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "HeadObject"
        )
        loader.client.exceptions.ClientError = ClientError
        result = loader.exists("silver", "instrument=missing/data.parquet")
        assert result is False


class TestLoad:
    def test_load_returns_bytes(self, mock_s3_loader):
        loader, mock_resource = mock_s3_loader
        mock_resource.Object.return_value.get.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=b"parquet_data"))
        }
        result = loader.load("silver", "test/path.parquet")
        assert result == b"parquet_data"

    def test_load_missing_returns_none(self, mock_s3_loader):
        loader, mock_resource = mock_s3_loader
        mock_resource.Object.return_value.get.side_effect = Exception("Not found")
        result = loader.load("silver", "missing/path.parquet")
        assert result is None