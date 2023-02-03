import os
from unittest.mock import patch, MagicMock

import pytest

from ucs.drivers.amazon.exceptions import S3FileUploadError, S3FileDeleteError
from ucs.drivers.amazon.s3 import AmazonS3


@patch.object(os, 'getenv')
def test_initiate_driver(mock_getenv):
    mock_getenv.return_value = False
    key = 'test_key'
    secret = 'test_secret'
    region = 'test_region'
    bucket = 'test_bucket'
    s3 = AmazonS3(key, secret, region, bucket)
    assert s3.bucket == bucket
    expected_client_params = {
        'region_name': region,
        'aws_secret_access_key': secret,
        'aws_access_key_id': key,
    }
    assert s3.client_params == expected_client_params


@pytest.mark.asyncio
@patch('ucs.drivers.amazon.s3.get_session')
@patch.object(os, 'getenv', return_value=False)
async def test_amazon_s3_driver_upload_file__success(mock_getenv, mock_get_session):
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session
    mock_put_object = mock_session.create_client.return_value.__aenter__.return_value.put_object
    mock_put_object.return_value = {
        'ResponseMetadata': {
            'HTTPStatusCode': 200
        }
    }
    key = 'test_key'
    secret = 'test_secret'
    region = 'test_region'
    bucket = 'test_bucket'
    client_params = {
        'region_name': region,
        'aws_secret_access_key': secret,
        'aws_access_key_id': key,
    }
    s3 = AmazonS3(key, secret, region, bucket)
    example_file = b'\x01'*64
    example_file_path = 'test111/test222.png'
    await s3.upload_file(example_file, example_file_path)
    mock_session.create_client.assert_called_once_with('s3', **client_params)
    mock_put_object.assert_called_once_with(
        ACL="public-read",
        Bucket=bucket,
        Key=example_file_path,
        Body=example_file
    )


@pytest.mark.asyncio
@patch('ucs.drivers.amazon.s3.get_session')
@patch.object(os, 'getenv', return_value=False)
async def test_amazon_s3_driver_upload_file__error_status_code(mock_getenv, mock_get_session):
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session
    mock_put_object = mock_session.create_client.return_value.__aenter__.return_value.put_object
    mock_put_object.return_value = {
        'ResponseMetadata': {
            'HTTPStatusCode': 500
        }
    }
    s3 = AmazonS3('test_key', 'test_secret', 'test_region', 'test_bucket')
    with pytest.raises(S3FileUploadError) as exception:
        await s3.upload_file(b'\x01'*64, 'test111/test222.png')
    assert "It was not possible to upload the file on S3" == str(exception.value)


@pytest.mark.asyncio
@patch('ucs.drivers.amazon.s3.get_session')
@patch.object(os, 'getenv', return_value=False)
async def test_amazon_s3_driver_upload_file__exception_error(mock_getenv, mock_get_session):
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session
    mock_put_object = mock_session.create_client.return_value.__aenter__.return_value.put_object
    mock_put_object.side_effect = Exception
    s3 = AmazonS3('test_key', 'test_secret', 'test_region', 'test_bucket')
    with pytest.raises(S3FileUploadError) as exception:
        await s3.upload_file(b'\x01'*64, 'test111/test222.png')
    assert "It was not possible to upload the file on S3" == str(exception.value)


@pytest.mark.asyncio
@patch('ucs.drivers.amazon.s3.get_session')
@patch.object(os, 'getenv', return_value=False)
async def test_amazon_s3_driver_delete_file__success(mock_getenv, mock_get_session):
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session
    mock_delete_object = mock_session.create_client.return_value.__aenter__.return_value.delete_object
    mock_delete_object.return_value = {
        'ResponseMetadata': {
            'HTTPStatusCode': 204
        }
    }
    key = 'test_key'
    secret = 'test_secret'
    region = 'test_region'
    bucket = 'test_bucket'
    client_params = {
        'region_name': region,
        'aws_secret_access_key': secret,
        'aws_access_key_id': key,
    }
    s3 = AmazonS3(key, secret, region, bucket)
    example_file_path = 'test111/test222.png'
    await s3.delete_file(example_file_path)
    mock_session.create_client.assert_called_once_with('s3', **client_params)
    mock_delete_object.assert_called_once_with(Bucket=bucket, Key=example_file_path)


@pytest.mark.asyncio
@patch('ucs.drivers.amazon.s3.get_session')
@patch.object(os, 'getenv', return_value=False)
async def test_amazon_s3_driver_delete_file__status_code_error(mock_getenv, mock_get_session):
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session
    mock_delete_object = mock_session.create_client.return_value.__aenter__.return_value.delete_object
    mock_delete_object.return_value = {
        'ResponseMetadata': {
            'HTTPStatusCode': 500
        }
    }
    s3 = AmazonS3('test_key', 'test_secret', 'test_region', 'test_bucket')
    with pytest.raises(S3FileDeleteError) as exception:
        await s3.delete_file('test111/test222.png')
    assert "It was not possible to delete the file on S3" == str(exception.value)


@pytest.mark.asyncio
@patch('ucs.drivers.amazon.s3.get_session')
@patch.object(os, 'getenv', return_value=False)
async def test_amazon_s3_driver_delete_file__status_code_error(mock_getenv, mock_get_session):
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session
    mock_delete_object = mock_session.create_client.return_value.__aenter__.return_value.delete_object
    mock_delete_object.side_effect = Exception
    s3 = AmazonS3('test_key', 'test_secret', 'test_region', 'test_bucket')
    with pytest.raises(S3FileDeleteError) as exception:
        await s3.delete_file('test111/test222.png')
    assert "It was not possible to delete the file on S3" == str(exception.value)

