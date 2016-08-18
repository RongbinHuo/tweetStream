"""S3 related utility functions."""
import logging
import boto3
import yaml
import re
from multiprocessing.pool import ThreadPool
import os
import tempfile
import uuid

LOGGER = logging.getLogger(__name__)
PARALLEL_PROCESSES = 10  # Number of processes to use for parallel code.


class S3Connection(object):
    """Represents a connection to an S3 bucket."""

    @classmethod
    def from_yaml(cls, file_path, *yaml_scope):
        """Returns a new S3Connection instance from the yaml file.
        Required values: bucket_name
        Optional values: aws_access_key_id, aws_secret_access_key
            * If both are not specified, boto3 will attempt to resolve it.
        :param file_path: path to the yaml configuration file.
        :param yaml_scope: the section of the yaml file to look into.
        :return: a new S3Connection instance.
        """
        LOGGER.debug('Creating S3Connection from {}[{}]'.format(file_path, '->'.join(yaml_scope)))
        info = yaml.safe_load(file(file_path))
        for scope in yaml_scope:
            info = info[scope]
        return S3Connection(info['bucket_name'], info.get('aws_access_key_id', None),
                            info.get('aws_secret_access_key', None))

    def __init__(self, bucket_name, aws_access_key_id=None, aws_secret_access_key=None):
        self.session = boto3.Session(aws_access_key_id, aws_secret_access_key)
        self.s3_client = self.session.client('s3')
        self.s3_resource = self.session.resource('s3')
        self.s3_bucket = self.s3_resource.Bucket(name=bucket_name)
        self.bucket_name = bucket_name

    def get_aws_access_key_id(self):
        """Gets the access key that is currently being used."""
        return self.session._session.get_credentials().access_key

    def get_aws_secret_access_key(self):
        """Gets the secret key that is currently being used."""
        return self.session._session.get_credentials().secret_key

    def upload_file(self, filename, s3_key=None):
        """Upload local filename to this bucket under the s3_key.
        :param filename: local file path to upload.
        :param s3_key: key that the file will be uploaded to s3 as. None to use a randomly generated key.
        """
        s3_key = '{}_{}'.format(uuid.uuid4(), os.path.basename(filename)) if s3_key is None else s3_key
        LOGGER.info('Uploading {} to s3://{}/{}'.format(filename, self.s3_bucket.name, s3_key))
        self.s3_client.upload_file(filename, self.s3_bucket.name, s3_key)

    def upload_files(self, filepath_list, s3_key_list=None):
        """Parallel upload a list of files to S3.
        :param filepath_list: list of filepaths to upload.
        :param s3_key_list: list of corresponding s3 keys for each filepath. None to use randomly generated keys.
        :return list of s3 keys that were uploaded. Note: not necessarily in the same order as filepath_list
        """
        s3_key_list = ['{}_{}'.format(uuid.uuid4(), os.path.basename(f)) for f in
                       filepath_list] if s3_key_list is None else s3_key_list
        access_key = self.get_aws_access_key_id()
        secret_key = self.get_aws_secret_access_key()
        upload_info = [(filepath_list[i], self.bucket_name, s3_key_list[i], access_key, secret_key) for i, _ in
                       enumerate(filepath_list)]
        LOGGER.info('Uploading {} files to S3'.format(len(filepath_list)))
        pool = ThreadPool(processes=PARALLEL_PROCESSES)
        uploaded_files = pool.map(_upload_file, upload_info)
        pool.close()
        pool.join()
        return uploaded_files

    def download_key(self, s3_key, filename=None):
        """Download s3_key from this bucket to the specified file path.
        :param s3_key: key of file to download.
        :param filename: target file path to download to. None to use a temporary file.
        """
        filename = tempfile.NamedTemporaryFile(delete=False).name if filename is None else filename
        LOGGER.info('Downloading {} to {}'.format(s3_key, filename))
        self.s3_client.download_file(self.s3_bucket.name, s3_key, filename)

    # TODO: rename to download_keys
    def download_parallel(self, s3_key_list, file_list=None):
        """Parallel download a list of files from S3.
        :param s3_key_list: list of S3 keys to download.
        :param file_list: list of corresponding filepaths for each S3 key. None to use temporary files.
        :return: list of filepaths that were downloaded. Note: not necessarily in the same order as s3_key_list
        """
        file_list = [tempfile.NamedTemporaryFile(suffix=key.split('/')[-1], delete=False).name for key in
                     s3_key_list] if file_list is None else file_list
        access_key = self.get_aws_access_key_id()
        secret_key = self.get_aws_secret_access_key()
        download_info = [(self.bucket_name, s3_key_list[i], file_list[i], access_key, secret_key) for i, _ in
                         enumerate(s3_key_list)]
        LOGGER.info('Downloading {} keys from S3'.format(len(s3_key_list)))
        pool = ThreadPool(processes=PARALLEL_PROCESSES)
        downloaded_files = pool.map(_download_file, download_info)
        pool.close()
        pool.join()
        return downloaded_files

    def get_key_contents(self, s3_key, num_bytes=-1):
        """Get the contents of the s3_key.
        :param s3_key: key of the file to get contents for.
        :param num_bytes: max bytes to read of the contents. -1 for all contents.
        :return: the data contents of the key.
        """
        return self.s3_bucket.Object(key=s3_key).get()['Body'].read(num_bytes)

    # TODO: rename method to something else
    def download_keys(self, s3_prefix, local_dir=None, file_regex=None, ignored_s3_files=(), max_num_to_pull=None):
        """Downloads multiple S3 key values.
        Important: filename refers to the right-most portion of the key. test/file.csv --> file.csv
        :param s3_prefix: prefix of files to get.
        :param local_dir: local directory to download the S3 files to. None to use a temporary system directory.
        :param file_regex: only download files such that the S3 filename matches this pattern.
        :param ignored_s3_files: iterable of s3 filenames to ignore.
        :param max_num_to_pull: maximum number of files to download for this call.
        :return: list of local filepaths downloaded to.
        """
        local_dir = tempfile.mkdtemp() if local_dir is None else local_dir
        s3_objects = self.s3_bucket.objects.filter(Prefix=s3_prefix)
        downloaded_local_files = []
        for obj in s3_objects:
            if max_num_to_pull is not None and len(downloaded_local_files) >= max_num_to_pull:
                break
            filename = os.path.basename(obj.key)
            does_match_regex = re.search(file_regex, filename) if file_regex is not None else True
            if does_match_regex and filename not in ignored_s3_files:
                local_file_path = os.path.join(local_dir, filename)
                self.download_key(obj.key, local_file_path)
                downloaded_local_files.append(local_file_path)
        return downloaded_local_files

    def copy_keys(self, from_prefix, to_prefix, file_regex=None, ignored_s3_files=(), to_other_bucket=None,
                  max_num_to_pull=None):
        """Copies keys from this bucket to another location on S3.
        :param from_prefix: copy files matching this s3 prefix.
        :param to_prefix: new prefix for the copied filenames.
        :param file_regex: only copy files matching this pattern.
        :param ignored_s3_files: iterable of s3 filenames to ignore.
        :param to_other_bucket: the target bucket to copy files to. If None, will copy to this same bucket.
        :param max_num_to_pull: maximum number of keys to copy for this call.
        :return: list of the new s3 keys.
        """
        to_bucket = to_other_bucket if to_other_bucket is not None else self.bucket_name  # default to same bucket.
        s3_objects = self.s3_bucket.objects.filter(Prefix=from_prefix)
        new_keys = []
        for obj in s3_objects:
            if max_num_to_pull is not None and len(new_keys) >= max_num_to_pull:
                break
            filename = os.path.basename(obj.key)
            does_match_regex = re.search(file_regex, filename) if file_regex is not None else True
            if does_match_regex and filename not in ignored_s3_files:
                copy_from = '{}/{}'.format(self.bucket_name, obj.key)  # copy_from requires bucket name.
                to_key = to_prefix + filename
                self.s3_resource.Object(to_bucket, to_key).copy_from(CopySource=copy_from)
                new_keys.append(to_key)
        if len(new_keys) == 0:
            LOGGER.info('No keys were copied.')
        return new_keys

    def delete_key(self, s3_key):
        """Deletes a specific s3 key from this bucket.
        :param s3_key: s3 key string to delete.
        :return: the s3 key that was deleted.
        """
        self.s3_resource.Object(self.s3_bucket.name, s3_key).delete()
        return s3_key

    def delete_keys(self, s3_prefix):
        """Deletes all s3 keys matching a prefix.
        :param s3_prefix: s3 key prefix to match for deletion. Warning: if '', will delete everything in the bucket.
        :return: a list of s3 keys that were deleted.
        """
        s3_objects = self.s3_bucket.objects.filter(Prefix=s3_prefix)
        deleted_keys = []
        for obj in s3_objects:
            deleted_keys.append(obj.key)
        s3_objects.delete()
        if len(deleted_keys) == 0:
            LOGGER.info('No keys were deleted.')
        return deleted_keys

    def list_keys(self, s3_prefix, s3_suffix='', ignored_s3_files=()):
        """List available s3 keys in this bucket.
        :param s3_prefix: s3 key prefix to filter on.
        :param s3_suffix: s3 keys must end with this to be matched.
        :param ignored_s3_files: iterable of s3 filenames to not include in the result.
        :return: list of s3 keys matching the criteria.
        """
        s3_keys = []
        s3_objects = self.s3_bucket.objects.filter(Prefix=s3_prefix)
        for obj in s3_objects:
            key = obj.key
            filename = os.path.basename(key)
            if filename not in ignored_s3_files and key.endswith(s3_suffix):
                s3_keys.append(key)
        return s3_keys

    def s3_string(self, key):
        """Gets the full S3 path string of the key in this bucket.
        :param key: key to get full path for.
        :return: full S3 path in format: s3://bucket/path/to/key
        """
        return 's3://{}/{}'.format(self.bucket_name, key)


def _upload_file(upload_tuple):
    """Helper method for parallel S3 upload.
    :param upload_tuple: tuple containing: (filename, bucket, s3_key, aws_access_key_id, aws_secret_access_key)
    """
    filename, bucket, s3_key, aws_access_key_id, aws_secret_access_key = upload_tuple
    LOGGER.info('[Parallel] Uploading {} to s3://{}/{}'.format(filename, bucket, s3_key))
    # Create new boto session for each thread.
    session = boto3.session.Session(aws_access_key_id, aws_secret_access_key)
    session.client('s3').upload_file(filename, bucket, s3_key)
    return s3_key


def _download_file(download_tuple):
    """Helper method for parallel S3 download.
    :param download_tuple: tuple containing: bucket, s3_key, filepath, aws_access_key_id, aws_secret_access_key
    """
    bucket, s3_key, filepath, aws_access_key_id, aws_secret_access_key = download_tuple
    LOGGER.info('[Parallel] Downloading s3://{}/{} to {}'.format(bucket, s3_key, filepath))
    # Create new boto session for each thread.
    session = boto3.session.Session(aws_access_key_id, aws_secret_access_key)
    session.client('s3').download_file(bucket, s3_key, filepath)
    return filepath
