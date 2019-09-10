import os
import glob
import shutil
import datetime
import hashlib
import pathlib

from contextlib import ContextDecorator
from tempfile import mkstemp
from abc import ABC, abstractmethod
from google.cloud import storage
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
    def __init__(self, provider, blob):
        self.provider = provider
        self.blob = blob
        self.position = 0
        self.dirty = False
        self.temp_file = None
        self.temp_file_path = None

    def read(self, size=None):
        kwargs = {'start': self.position}
        if size is not None:
            kwargs['end'] = self.position + size

        data = self.blob.download_as_string(**kwargs)
        self.position += len(data)
        return data

    def write(self, content):
        self.dirty = True
        if self.temp_file is None:
            _, path = mkstemp()
            self.temp_file_path = path
            self.temp_file = open(path, 'w')

        self.temp_file.write(content)

    def seek(self, offset=0, whence=0):
        if whence == 0:
            self.position = offset
        elif whence == 1:
            self.position += offset
        elif whence == 2:
            self.position = self.blob.size + offset

    def close(self):
        if self.temp_file is not None:
            self.blob.upload_from_filename(self.temp_file_path)
            self.temp_file.close()


    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


class GoogleCloudStorageProvider(StorageProvider):

    def open(self, path, mode):
        client = storage.Client()
        bucket_name, blob_name = self._parse_path(path)
        bucket = client.get_bucket(bucket_name)
        blob = storage.blob.Blob(blob_name, bucket)

        return GoogleCloudStorageFile(self, blob)

    def lookup(self, bucket_name):
        client = storage.Client()
        return client.lookup_bucket(bucket_name)

    def list(self, bucket_name, prefix):
        client = storage.Client()
        return client.list_blobs(bucket_name, prefix=prefix)

    def get_blob(self, bucket_name, blob_name):
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        return bucket.get_blob(blob_name)

    def upload_from_file(self, source_file, path):
        client = storage.Client()
        bucket_name, blob_name = self._parse_path(path)
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name, bucket)
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

    def exists(self, path):
        bucket_path = path.split(os.sep)
        bucket_name = bucket_path[0]
        file_name = bucket_path[-1]
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        gcs_stat = storage.Blob(bucket=bucket, name=file_name).exists(storage_client)
        return gcs_stat is not None

    @staticmethod
    def _parse_path(path):
        path = path if path[0:1] != '/' else path[1:]
        bucket_name, _, blob_name = path.partition('/')
        return bucket_name, blob_name


def get_storage_provider():
    provider_class = StorageProvider.get_provider(default=LocalFilesystemStorageProvider)
    return provider_class()

