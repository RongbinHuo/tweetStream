"""Various utility functions."""
import gzip
import hashlib
import inspect
import logging
import os
import random
import tempfile
import shutil
import yaml
import zipfile
import datetime
import uuid

LOGGER = logging.getLogger(__name__)


class DataPipelineException(Exception):
    """Generic exception for the DataPipeline."""
    pass


def rel_path(relative_filename):
    """Returns the full path of the file relative to the caller module.
    :param relative_filename: target filename relative to the caller's containing folder.
    :return: the full path of the target relative file.
    """
    # Extract the filename of the caller's stack frame.
    caller_frame = inspect.stack()[1]
    try:
        caller_filepath = inspect.getabsfile(caller_frame[0])
    finally:
        del caller_frame  # Force remove frame reference to prevent garbage-collection issues.
    return os.path.join(os.path.dirname(os.path.realpath(caller_filepath)), relative_filename)


def compress_to_gz(filename, zipped_filename=None, delete_original=False):
    """Compresses a file using gzip.
    :param filename: filename to gzip
    :param zipped_filename: zipped filename to output. defaults to filename + .gz
    :param delete_original: delete filename after successfully gzipping?
    :return: the final zipped filename that was written to.
    """
    # Warn if may be already compressed.
    if os.path.splitext(filename)[-1] == '.gz':
        LOGGER.warning('Filename {} may already by compressed'.format(filename))
    # Default to appending .gz to filename.
    if zipped_filename is None:
        zipped_filename = filename + '.gz'
    # Gzipped files should end with extension .gz
    if os.path.splitext(zipped_filename)[-1] != '.gz':
        LOGGER.warning('GZIP file {} does not have .gz extension'.format(zipped_filename))
    LOGGER.debug('Compressing {} to {}'.format(filename, zipped_filename))
    with open(filename, 'rb') as f_src, gzip.open(zipped_filename, 'wb') as f_dest:
        shutil.copyfileobj(f_src, f_dest)
    if delete_original:
        os.remove(filename)
    return zipped_filename


def decompress(filepath, contents_dir=None, delete_original=False):
    """Generic decompression that handles zip and gz files.
    :param filepath: compressed file to decompress.
    :param contents_dir: directory to extract files to. If None, will use a temporary system directory.
    :param delete_original: delete file after successfully decompressing?
    :return: list of filepaths that were extracted.
    """
    contents_dir = tempfile.mkdtemp() if contents_dir is None else contents_dir
    if zipfile.is_zipfile(filepath):
        return unzip(filepath, contents_dir, delete_original)
    else:
        return [decompress_gz(filepath, delete_gzip=delete_original)]


def decompress_gz(gzip_filename, target_filename=None, delete_gzip=False):
    """Decompresses a gzip file into a different file.
    :param gzip_filename: gzip file path to gunzip.
    :param target_filename: file path of unzipped contents. defaults to gzip_filename - .gz
    :param delete_gzip: delete gzipped file after successfully decompressing?
    :return: the final decompressed filename that was written to.
    """
    # Warn if may be uncompressed.
    if os.path.splitext(gzip_filename)[-1] != '.gz':
        LOGGER.warning('Filename {} may not be in gzip format'.format(gzip_filename))
    # Default target filename to removing .gz
    if target_filename is None:
        target_filename = os.path.splitext(gzip_filename)[0]
    LOGGER.debug('Decompressing {} to {}'.format(gzip_filename, target_filename))
    with gzip.open(gzip_filename, 'rb') as f_src, open(target_filename, 'wb') as f_dest:
        shutil.copyfileobj(f_src, f_dest)
    if delete_gzip:
        os.remove(gzip_filename)
    return target_filename


def unzip(filename, contents_path=None, delete_original=False):
    """Unzips contents inside a .zip file.
    :param filename: path to the zip file.
    :param contents_path: directory to unzip to. If None, unzips to working directory.
    :param delete_original: delete zip file after successfully decompressing?
    :return: a list of file paths that were unzipped.
    """
    if os.path.splitext(filename)[-1] != '.zip':
        LOGGER.warning('Filename {} may not be a zipped file'.format(filename))
    with zipfile.ZipFile(filename, 'r') as zip_file:
        unzipped_files = zip_file.namelist()
        zip_file.extractall(path=contents_path)
    if delete_original:
        os.remove(filename)
    return [os.path.join(contents_path, f) for f in unzipped_files]


def read_yaml(file_path):
    """Returns the dictionary representation of the yaml file.
    :param file_path: file path to the yaml file.
    :return: dictionary representing the contents of the file.
    """
    return yaml.safe_load(file(file_path))


def hash_string_to_int(string_to_hash, max_int_size=2147483647):
    """Computes the integer SHA1 hash of an arbitrary string.
    :param string_to_hash: string that will be hashed.
    :param max_int_size: the maximum size of the output digest.
    :return: integer hexdigest of the string.
    """
    hash_val = hashlib.sha1()
    hash_val.update(string_to_hash)
    return int(hash_val.hexdigest(), 16) % max_int_size


def partition_list(iterable, partition_size):
    """Splits a list into several lists each up to partition_size.
    :param iterable: the iterable to partition.
    :param partition_size: num elements in each partition. the final partition may have less.
    :return: a list of lists containing the partitions.
    """
    return [iterable[i:i + partition_size] for i in range(0, len(iterable), partition_size)]


def reset_dir(dir_path):
    """Removes all files in directory and recreate just the directory path.
    :param dir_path: directory path to reset.
    """
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
    os.makedirs(dir_path)


def unique_str():
    """Returns a string that is VERY likely to be unique globally."""
    return '{:%Y_%m_%d_%H_%M_%S}__{}'.format(datetime.datetime.now(), uuid.uuid4().hex)

