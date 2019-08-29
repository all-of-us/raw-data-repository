import os
import glob
import shutil
import datetime

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
    def list(self, bucket_name):
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


class LocalFilesystemStorageProvider(StorageProvider):
    DEFAULT_STORAGE_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'tests', '.test_storage'))

    def get_storage_root(self):
        root = os.environ.get("RDR_STORAGE_ROOT", self.DEFAULT_STORAGE_ROOT)
        return root if root[-1:] == os.sep else root + os.sep

    def _get_local_path(self, path):
        path = path if path[0:1] != os.sep else path[1:]
        return os.path.join(self.get_storage_root(), path)

    def _get_blob_name_from_local_path(self, local_path):
        cloud_path = local_path.replace(self.get_storage_root(), '')
        path = cloud_path if cloud_path[0:1] != '/' else cloud_path[1:]
        bucket_name, _, blob_name = path.partition('/')
        return blob_name

    def open(self, path, mode):
        return open(self._get_local_path(path), mode)

    def lookup(self, bucket_name):
        path = self._get_local_path(bucket_name)
        if os.path.exists(path):
            return bucket_name
        else:
            return None

    def list(self, bucket_name):
        path = self._get_local_path(bucket_name)

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

    @staticmethod
    def _make_blob(*args, **kw):
        properties = kw.pop("properties", {})
        blob = Blob(*args, **kw)
        blob._properties.update(properties)
        return blob


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

    def list(self, bucket_name):
        client = storage.Client()
        return client.list_blobs(bucket_name)

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

    @staticmethod
    def _parse_path(path):
        path = path if path[0:1] != '/' else path[1:]
        bucket_name, _, blob_name = path.partition('/')
        return bucket_name, blob_name


def get_storage_provider():
    provider_class = StorageProvider.get_provider(default=LocalFilesystemStorageProvider)
    return provider_class()

