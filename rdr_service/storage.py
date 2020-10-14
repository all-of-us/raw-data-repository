import io
import logging
import os
import glob
import shutil
import datetime
import hashlib
import pathlib
import tempfile

from contextlib import ContextDecorator
from abc import ABC, abstractmethod

from google.api_core.exceptions import RequestRangeNotSatisfiable
from google.cloud import storage
from google.cloud.exceptions import GatewayTimeout
from google.cloud.storage import Blob
from google.cloud._helpers import UTC
from google.cloud._helpers import _RFC3339_MICROS
from rdr_service.provider import Provider


class StorageProvider(Provider, ABC):
    environment_variable_name = 'RDR_STORAGE_PROVIDER'

    @abstractmethod
    def open(self, path, mode):
        pass

    @abstractmethod
    def lookup(self, bucket_name):
        pass

    @abstractmethod
    def list(self, bucket_name, prefix):
        pass

    @abstractmethod
    def get_blob(self, bucket_name, blob_name):
        pass

    @abstractmethod
    def upload_from_file(self, source_file, path):
        pass

    @abstractmethod
    def upload_from_string(self, contents, path):
        pass

    @abstractmethod
    def delete(self, path):
        pass

    @abstractmethod
    def copy_blob(self, source_path, destination_path):
        pass

    @abstractmethod
    def exists(self, path):
        pass


class LocalFilesystemStorageProvider(StorageProvider):
    DEFAULT_STORAGE_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'tests', '.test_storage'))

    def get_storage_root(self):
        root = os.environ.get("RDR_STORAGE_ROOT", self.DEFAULT_STORAGE_ROOT)
        return root if root[-1:] == os.sep else root + os.sep

    def _get_local_path(self, path):
        path = path if path[0:1] != os.sep else path[1:]
        return os.path.join(self.get_storage_root(), path)

    def _get_blob_name_from_local_path(self, local_path):
        # pylint: disable=unused-variable
        cloud_path = local_path.replace(self.get_storage_root(), '')
        path = cloud_path if cloud_path[0:1] != '/' else cloud_path[1:]
        bucket_name, _, blob_name = path.partition('/')
        return blob_name

    def open(self, path, mode):
        local_path = self._get_local_path(path)
        directory = os.path.dirname(local_path)
        if directory:
            pathlib.Path(directory).mkdir(parents=True, exist_ok=True)

        return open(local_path, mode)

    def lookup(self, bucket_name):
        path = self._get_local_path(bucket_name)
        if os.path.exists(path):
            return bucket_name
        else:
            return None

    exists = lookup

    def get_blob(self, bucket_name, blob_name):
        file_path = os.path.normpath(self._get_local_path(bucket_name) + os.sep + blob_name)
        if not os.path.exists(file_path):
            return None

        updated = datetime.datetime.utcfromtimestamp(os.path.getmtime(file_path)).replace(tzinfo=UTC)
        updated = updated.strftime(_RFC3339_MICROS)
        properties = {
            'updated': updated,
            'etag': self.md5_checksum(file_path)
        }
        blob = self._make_blob(blob_name, bucket=None, properties=properties)
        return blob

    def list(self, bucket_name, prefix):
        path = self._get_local_path(bucket_name)
        if prefix is not None:
            prefix = prefix[:-1] if prefix[-1:] == '/' else prefix
            path = os.path.normpath(path + os.sep + prefix)
        files = filter(os.path.isfile, glob.glob(path + os.sep + '**' + os.sep + '*', recursive=True))

        blob_list = []
        for file in files:
            blob_name = self._get_blob_name_from_local_path(os.path.abspath(file))
            updated = datetime.datetime.utcfromtimestamp(os.path.getmtime(file)).replace(tzinfo=UTC)
            updated = updated.strftime(_RFC3339_MICROS)
            properties = {
                'updated': updated
            }
            blob = self._make_blob(blob_name, bucket=None, properties=properties)
            blob_list.append(blob)
        return iter(blob_list)

    def upload_from_file(self, source_file, path):
        path = self._get_local_path(path)
        shutil.copyfile(source_file, path)

    def upload_from_string(self, contents, path):
        path = self._get_local_path(path)
        with open(path, "w") as bucket_file:
            bucket_file.write(contents)

    def delete(self, path):
        path = self._get_local_path(path)
        os.remove(path)

    def copy_blob(self, source_path, destination_path):
        source_path = self._get_local_path(source_path)
        destination_path = self._get_local_path(destination_path)
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        shutil.copy(source_path, destination_path)

    def get_local_path(self, path):
        return self._get_local_path(path)

    @staticmethod
    def _make_blob(*args, **kw):
        properties = kw.pop("properties", {})
        blob = Blob(*args, **kw)
        blob._properties.update(properties)
        return blob

    @staticmethod
    def md5_checksum(file_path):
        with open(file_path, 'rb') as fh:
            m = hashlib.md5()
            while True:
                data = fh.read(8192)
                if not data:
                    break
                m.update(data)
            return m.hexdigest()


class GoogleCloudStorageFile(ContextDecorator):

    _lines = None
    _line = 0
    _w_temp_file = None

    def __init__(self, provider=None, blob=None):
        self.provider = provider
        self.blob = blob
        self.position = 0
        self.dirty = False

    def read(self, size=None):
        kwargs = {'start': self.position}
        if size is not None:
            kwargs['end'] = self.position + size

        data = self.blob.download_as_string(**kwargs)
        self.position += len(data)
        return data

    def write(self, content):
        self.dirty = True
        if self._w_temp_file is None:
            self._w_temp_file = tempfile.NamedTemporaryFile(delete=False)
        if isinstance(content, str):
            content = content.encode()
        self._w_temp_file.write(content)
        self._w_temp_file.flush()

    def flush(self):
        self._w_temp_file.flush()

    def seek(self, offset=0, whence=0):
        if whence == 0:
            self.position = offset
        elif whence == 1:
            self.position += offset
        elif whence == 2:
            self.position = self.blob.size + offset

    def close(self):
        if self._w_temp_file is not None:
            self._w_temp_file.close()
            self.blob.upload_from_filename(self._w_temp_file.name)
            os.unlink(self._w_temp_file.name)
            self._w_temp_file = None
        self.dirty = False

    def __next__(self):
        if not self._lines:
            self._filedata = self.read()
            self._lines = self._filedata.decode('utf-8').splitlines()

        if self._line < len(self._lines):
            data = self._lines[self._line]
            self._line += 1
            return data
        raise StopIteration

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def __iter__(self):
        return self.iter_lines()

    def iter_chunks(self, chunk_size=1024):
        i = 0
        while True:
            try:
                chunk = self.blob.download_as_string(start=i, end=i+chunk_size)
            except RequestRangeNotSatisfiable:
                break
            if chunk:
                yield chunk
                i += len(chunk)
            else:
                break

    def iter_lines(self):
        buffer = io.StringIO()
        chunks = self.iter_chunks()
        for chunk in chunks:
            for character in chunk.decode('utf-8'):
                if character == '\n':
                    buffer.seek(0)
                    yield buffer.read()
                    buffer.seek(0)
                    buffer.truncate(0)
                else:
                    buffer.write(character)
        if buffer.tell() > 0:
            buffer.seek(0)
            yield buffer.read()


class GoogleCloudStorageProvider(StorageProvider):

    def open(self, path, mode):
        client = storage.Client()
        bucket_name, blob_name = self._parse_path(path)
        bucket = client.get_bucket(bucket_name)
        blob = storage.blob.Blob(blob_name, bucket)
        return GoogleCloudStorageFile(self, blob)

    def lookup(self, bucket_name):
        client = storage.Client()
        _bucket_name = self._parse_bucket(bucket_name)
        return client.lookup_bucket(_bucket_name)

    def list(self, bucket_name, prefix):
        client = storage.Client()
        _bucket_name = self._parse_bucket(bucket_name)
        return client.list_blobs(_bucket_name, prefix=prefix)

    def get_blob(self, bucket_name, blob_name):
        client = storage.Client()
        _bucket_name = self._parse_bucket(bucket_name)
        bucket = client.get_bucket(_bucket_name)
        return bucket.get_blob(blob_name)

    def upload_from_file(self, source_file, path):
        client = storage.Client()
        bucket_name, blob_name = self._parse_path(path)
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(source_file)

    def upload_from_string(self, contents, path):
        client = storage.Client()
        bucket_name, blob_name = self._parse_path(path)
        bucket = client.get_bucket(bucket_name)
        blob = Blob(blob_name, bucket)
        blob.upload_from_string(contents)

    def delete(self, path):
        client = storage.Client()
        bucket_name, blob_name = self._parse_path(path)
        bucket = client.get_bucket(bucket_name)
        blob = storage.blob.Blob(blob_name, bucket)
        blob.delete()

    def copy_blob(self, source_path, destination_path):
        source_bucket_name, source_blob_name = self._parse_path(source_path)
        destination_bucket_name, destination_blob_name = self._parse_path(destination_path)
        storage_client = storage.Client()
        source_bucket = storage_client.get_bucket(source_bucket_name)
        source_blob = source_bucket.blob(source_blob_name)
        destination_bucket = storage_client.get_bucket(destination_bucket_name)

        source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)

    def download_blob(self, source_path, destination_path):
        source_bucket_name, source_blob_name = self._parse_path(source_path)
        storage_client = storage.Client()
        source_bucket = storage_client.get_bucket(source_bucket_name)
        source_blob = source_bucket.blob(source_blob_name)
        source_blob.download_to_filename(destination_path)

    def exists(self, path):
        bucket_name, blob_name = self._parse_path(path)
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = storage.Blob(bucket=bucket, name=blob_name)

        retry = 3
        while retry:
            try:
                gcs_stat = blob.exists(client)
                return gcs_stat
            except GatewayTimeout:
                retry -= 1
                logging.warning(f"Google Storage timeout error, {retry} retry attempts left.")

        raise ConnectionRefusedError(f"Connection to Google Storage failed. ({path})")

    @staticmethod
    def _parse_path(path):
        path = path if path[0:1] != '/' else path[1:]
        bucket_name, _, blob_name = path.partition('/')
        return bucket_name, blob_name

    @staticmethod
    def _parse_bucket(bucket):
        bucket = bucket if bucket[0:1] != '/' else bucket[1:]
        return bucket


def get_storage_provider():
    # Set a good default and let the environment var be the override.
    if os.getenv('GAE_ENV', '').startswith('standard'):
        default_provider = GoogleCloudStorageProvider
    else:
        default_provider = LocalFilesystemStorageProvider
    provider_class = StorageProvider.get_provider(default=default_provider)
    return provider_class()

