import unittest
from unittest.mock import patch, MagicMock
from src.aws_helpers import get_ssm_parameter, get_raw_bucket_name, get_stream_name

class TestAwsHelpers(unittest.TestCase):
    @patch('boto3.client')
    def test_get_ssm_parameter(self, mock_boto_client):
        # Arrange
        mock_ssm = MagicMock()
        mock_boto_client.return_value = mock_ssm
        mock_ssm.get_parameter.return_value = {
            "Parameter": {"Value": "mocked_value"}
        }

        # Act
        result = get_ssm_parameter("/my/param/path")

        # Assert
        mock_ssm.get_parameter.assert_called_once_with(Name="/my/param/path")
        self.assertEqual(result, "mocked_value")

    @patch('src.aws_helpers.get_ssm_parameter')
    def test_get_raw_bucket_name(self, mock_get_ssm):
        # Arrange
        mock_get_ssm.return_value = "my-bucket"

        # Act
        result = get_raw_bucket_name("my_app", "prod")

        # Assert
        mock_get_ssm.assert_called_once_with("/my_app/prod/storage/RawAwid3BucketName")
        self.assertEqual(result, "my-bucket")

    @patch('src.aws_helpers.get_ssm_parameter')
    def test_get_stream_name(self, mock_get_ssm):
        # Arrange
        mock_get_ssm.return_value = "my-stream"

        # Act
        result = get_stream_name("my_app", "test")

        # Assert
        mock_get_ssm.assert_called_once_with("/my_app/test/streaming/Awid3StreamName")
        self.assertEqual(result, "my-stream")

if __name__ == '__main__':
    unittest.main()

