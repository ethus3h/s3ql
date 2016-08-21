'''
ia.py - this file is part of S3QL.

Copyright © 2016 Nikolaus Rath <Nikolaus@rath.org> / Ember

This work can be distributed under the terms of the GNU GPLv3.
'''

from ..logging import logging # Ensure use of custom logger class
from .. import BUFSIZE
from ..inherit_docstrings import (copy_ancestor_docstring, ABCDocstMeta)
from .common import (AbstractBackend, DanglingStorageURLError, NoSuchObject,
                     CorruptedObjectError)
from ..common import ThawError, freeze_basic_mapping, thaw_basic_mapping
import _thread
import struct
import io
import internetarchive
import os
import shutil

log = logging.getLogger(__name__)

class Backend(AbstractBackend, metaclass=ABCDocstMeta):
    '''
    A backend that stores data using https://pypi.python.org/pypi/internetarchive
    '''

    needs_login = False
    known_options = set()

    def __init__(self, storage_url, backend_login=None, backend_pw=None,
                 options=None):
        '''Initialize ia backend

        Login and password are ignored.
        '''
        # Unused argument
        #pylint: disable=W0613

        super().__init__()
        self.prefix = storage_url[len('ia://'):].rstrip('/')

        self.iasession = get_session()
        if not self.iasession.get_item(self.prefix).exists:
            raise DanglingStorageURLError(self.prefix)

    @property
    @copy_ancestor_docstring
    def has_native_rename(self):
        return False

    def __str__(self):
        return 'ia directory %s' % self.prefix

    @copy_ancestor_docstring
    def is_temp_failure(self, exc): #IGNORE:W0613
        return False

    @copy_ancestor_docstring
    def lookup(self, key):
        iafile = self.iasession.get_files(self.prefix,key)
        if iafile.exists:
            return iafile.metadata
        else:
            raise NoSuchObject(key)

    @copy_ancestor_docstring
    def get_size(self, key):
        return self.iasession.get_files(self.prefix,key)[0].size

    @copy_ancestor_docstring
    def open_read(self, key):
        iafile = self.iasession.get_files(self.prefix,key)
        if iafile.exists:
            iafile.download
            fh = ObjectR(iafile.name)
        else:
            raise NoSuchObject(key)

        try:
            fh.metadata = _read_meta(fh)
        except ThawError:
            fh.close()
            raise CorruptedObjectError('Invalid metadata')
        return fh

    @copy_ancestor_docstring
    def open_write(self, key, metadata=None, is_compressed=False):
        if metadata is None:
            metadata = dict()
        elif not isinstance(metadata, dict):
            raise TypeError('*metadata*: expected dict or None, got %s' % type(metadata))

        path = self._key_to_path(key)
        buf = freeze_basic_mapping(metadata)
        if len(buf).bit_length() > 16:
            raise ValueError('Metadata too large')

        # By renaming, we make sure that there are no
        # conflicts between parallel reads, the last one wins
        tmpname = '%s#%d-%d.tmp' % (path, os.getpid(), _thread.get_ident())

        dest = ObjectW(tmpname)
        os.rename(tmpname, path)

        dest.write(b's3ql_1\n')
        dest.write(struct.pack('<H', len(buf)))
        dest.write(buf)

        return dest

    @copy_ancestor_docstring
    def clear(self):
        log.warning('clear method is not implemented by ia backend')
        pass

    @copy_ancestor_docstring
    def contains(self, key):
        iafile = self.iasession.get_files(self.prefix,key)
        if iafile.exists:
            return True
        else:
            return False

    @copy_ancestor_docstring
    def delete(self, key, force=False):
        iafile = self.iasession.get_files(self.prefix,key)
        if iafile.exists:
            iafile.delete
        else:
            if force:
                pass
            else:
                raise NoSuchObject(key)

    @copy_ancestor_docstring
    def list(self, prefix=''):

        iafiles = self.iasession.get_files(self.prefix, prefix)

        for iafile in iafiles:
            # Skip temporary files
            if '#' in iafile.name:
                continue

            key = unescape(iafile.name)
            #TODO: Should these next 3 lines happen?
            if not prefix or key.startswith(prefix):
                yield key

    @copy_ancestor_docstring
    def update_meta(self, key, metadata):
        if not isinstance(metadata, dict):
            raise TypeError('*metadata*: expected dict, got %s' % type(metadata))
        self.copy(key, key, metadata)

    @copy_ancestor_docstring
    def copy(self, src, dest, metadata=None):
        if not (metadata is None or isinstance(metadata, dict)):
            raise TypeError('*metadata*: expected dict or None, got %s' % type(metadata))
        elif metadata is not None:
            buf = freeze_basic_mapping(metadata)
            if len(buf).bit_length() > 16:
                raise ValueError('Metadata too large')

        path_src = self._key_to_path(src)
        path_dest = self._key_to_path(dest)

        dest = None
        self.iasession.get_files(self.prefix,src)
        if iafile.exists:
            iafile.download
            self.iasession.upload({dest: src})
        else:
            raise NoSuchObject(src)

    def _key_to_path(self, key):
        '''Return path for given key'''

        # NOTE: We must not split the path in the middle of an
        # escape sequence, or list() will fail to work.

        key = escape(key)

        if not key.startswith('s3ql_data_'):
            return os.path.join(self.prefix, key)

        no = key[10:]
        path = [ self.prefix, 's3ql_data_']
        for i in range(0, len(no), 3):
            path.append(no[:i])
        path.append(key)

        return os.path.join(*path)

def _read_meta(fh):
    buf = fh.read(9)
    if not buf.startswith(b's3ql_1\n'):
        raise CorruptedObjectError('Invalid object header: %r' % buf)

    len_ = struct.unpack('<H', buf[-2:])[0]
    try:
        return thaw_basic_mapping(fh.read(len_))
    except ThawError:
        raise CorruptedObjectError('Invalid metadata')

def escape(s):
    '''Escape '/', '=' and '.' in s'''

    s = s.replace('=', '=3D')
    s = s.replace('/', '=2F')
    s = s.replace('#', '=23')

    return s

def unescape(s):
    '''Un-Escape '/', '=' and '.' in s'''

    s = s.replace('=2F', '/')
    s = s.replace('=23', '#')
    s = s.replace('=3D', '=')

    return s


# Inherit from io.FileIO rather than io.BufferedReader to disable buffering. Default buffer size is
# ~8 kB (http://docs.python.org/3/library/functions.html#open), but backends are almost always only
# accessed by block_cache and stream_read_bz2/stream_write_bz2, which all use the much larger
# s3ql.common.BUFSIZE
class ObjectR(io.FileIO):
    '''A local storage object opened for reading'''


    def __init__(self, name, metadata=None):
        super().__init__(name)
        self.metadata = metadata

    def close(self, checksum_warning=True):
        '''Close object

        The *checksum_warning* parameter is ignored.
        '''
        # TODO: is it necessary to delete file afterward?
        super().close()

class ObjectW(object):
    '''A local storage object opened for writing'''

    def __init__(self, name):
        super().__init__()

        # Default buffer size is ~8 kB
        # (http://docs.python.org/3/library/functions.html#open), but backends
        # are almost always only accessed by block_cache and
        # stream_read_bz2/stream_write_bz2, which all use the much larger
        # s3ql.common.BUFSIZE - so we may just as well disable buffering.

        # Create parent directories as needed
        try:
            self.fh = open(name, 'wb', buffering=0)
        except FileNotFoundError:
            try:
                os.makedirs(os.path.dirname(name))
            except FileExistsError:
                # Another thread may have created the directory already
                pass
            self.fh = open(name, 'wb', buffering=0)

        self.obj_size = 0
        self.closed = False

    def write(self, buf):
        '''Write object data'''

        self.fh.write(buf)
        self.obj_size += len(buf)

    def close(self):
        '''Close object and upload data'''
        #TODO: upload data
        self.fh.close()
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False

    def get_obj_size(self):
        if not self.closed:
            raise RuntimeError('Object must be closed first.')
        return self.obj_size
