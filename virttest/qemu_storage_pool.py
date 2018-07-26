"""
classes and functions to handle storage pools for QEMU.
"""

import os
import re
import uuid
import stat
import logging

from abc import ABCMeta
from six import add_metaclass
from collections import defaultdict
# from virttest.utils_numeric import format_size_human_readable


def format_size_human_readable(value, binary=False, format='%.1f'):
    suffixes = {
        'decimal': ('B', 'kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'),
        'binary': ('B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB')
    }
    suffix = suffixes['binary'] if binary else suffixes['decimal']
    base = 1024 if binary else 1000

    value = float(value)
    for i, s in enumerate(suffix):
        unit = base ** (i + 1)
        if value < unit:
            break
    value = value * base / unit
    format_str = ('%d' if value.is_integer() else format) + ' %s'
    return format_str % (value, s)


class PoolError(Exception):
    def __init__(self, *args, **kargs):
        super(PoolError, self).__init__(*args, **kargs)


class PoolConfigError(PoolError):
    pass


class PoolVolNotExistError(PoolError):
    def __init__(self, pool_name, vol_name):
        super(PoolVolNotExistError, self).__init__(pool_name, vol_name)
        self.pool_name = pool_name
        self.vol_name = vol_name

    def __str__(self):
        msg = "failed to get vol '%s' in pool '%s'"
        return msg % (self.vol_name, self.pool_name)


class DirectoryPoolConfigError(PoolConfigError):
    def __init__(self, err_msg):
        super(DirectoryPoolConfigError, self).__init__(err_msg)
        self.err_msg = err_msg

    def __str__(self):
        msg = "required field: path\nbut %s"
        return msg % self.err_msg


class DirectoryPoolInactiveError(PoolError):
    def __init__(self, pool_name, func_name):
        self.pool_name = pool_name
        self.func_name = func_name

    def __str__(self):
        msg = "DirectoryPool %s is inactive, unable to call %s."
        return msg % (self.pool_name, self.func_name)


@add_metaclass(ABCMeta)
class PoolBase(object):
    """
    abstract base pool used to define common interfaces for other pools.
    """

    def __init__(self, name, config, source_list=[], target_list=[]):
        """
        Initialize a new pool object

        :param name: pool name
        :param config: dictionary of pool configurations
        :param source: the source of storage pool
        :param target: the mapping target of the storage pool into the host FS
        """
        self.name = name
        self.uuid = uuid.uuid4()
        self.config = self._parse_config(config)
        self.source_list = source_list
        self.target_list = target_list
        self.active = False

        # stores volume's path, url and json.
        self.volumes = defaultdict(lambda: ['', '', ''])

    def _parse_config(self, config):
        """
        Validate pool configuration.
        """
        return config

    def pool_info(self):
        """
        Return a dict of pool details.
        """
        raise NotImplementedError

    def get_vol_info(self, volume):
        """
        Get info of specific volume.
        """
        raise NotImplementedError

    def pool_list_vols(self):
        """
        Return a dict with key as volume name and value as volume details.
        """
        raise NotImplementedError

    def _pool_build(self):
        """
        Build from source and config.
        """
        raise NotImplementedError

    def pool_start(self):
        """
        Mark the pool as active.
        """
        raise NotImplementedError

    def pool_destroy(self):
        """
        Mark the pool as inactive.
        """
        raise NotImplementedError

    def pool_delete(self):
        """
        Unlink from source and cleanup config.
        """
        raise NotImplementedError

    def is_pool_active(self):
        """
        Check if pool is activated.
        """
        raise NotImplementedError

    def is_volumes_existed(self, name):
        """
        Check if a volume is existed.
        """
        raise NotImplementedError

    def create_volume(self, name):
        """
        Create a volume in pool.
        """
        raise NotImplementedError

    def delete_volume(self, name):
        """
        Delete a volume if existed.
        """
        raise NotImplementedError

    def clone_volume(self, source_vol, dest_vol):
        """
        Clone a volume from this pool to dest pool.
        """
        raise NotImplementedError


class DirectoryPool(PoolBase):
    """
    Pool to manage files within a directory.
    """
    json_template = ('{"driver": "%s", '
                     '"file": {"driver": "file", '
                     '"filename": "%s"}}')

    def __init__(self, name, config):
        """
        Initialize a new pool object

        :param name: pool name
        :param config: dictory path
        """
        super(DirectoryPool, self).__init__(name, config)

    def _parse_config(self, config):
        """
        Check if required configuration fields are filled in config.

        :param config: pool configuration dictionary
        :raise DirectoryPoolConfigError: invalid key in config dictionary
        """
        def parser(path):
            return locals()

        try:
            return parser(**config)
        except TypeError as e:
            raise DirectoryPoolConfigError(re.sub(r'parser\(\)',
                                                  'DirectoryPool',
                                                  str(e)))

    def _pool_build(self):
        """
        Build the directory if neccessary.
        """
        path = os.path.abspath(self.config['path'])
        if not os.path.isdir(path):
            os.mkdir(path)
        self.config['path'] = path

    def _format_vol_path(self, vol_name):
        """
        Get volume path.
        """
        return os.path.join(self.config['path'], vol_name)

    def _format_as_url(self, vol_name):
        """
        Return an URL of the volume to be created.
        """
        return 'file:%s' % self._format_vol_path(vol_name)

    def _format_as_json(self, vol_path, vol_format):
        """
        Return json of the volume that created.
        """
        return self.json_template % (vol_format, vol_path)

    # divider

    def pool_start(self):
        """
        Mark the pool as active.
        """
        self._pool_build()
        self.active = True

    def pool_stat(self, human=True):
        """
        Return a dict of pool's capacity, allocated and available space.

        param human: print sizes in human readable format, default yes
        """
        stat = os.statvfs(self.config['path'])
        stat_dict = dict(capacity=stat.f_bsize*stat.f_blocks,
                         available=stat.f_bsize*stat.f_bavail,
                         allocation=stat.f_bsize*(stat.f_blocks-stat.f_bavail))
        if human:
            return {k: format_size_human_readable(stat_dict[k], binary=True)
                    for k in stat_dict.keys()}
        else:
            return stat_dict

    @property
    def capacity(self, human=True):
        return self.pool_stat(human=human)['capacity']

    @property
    def allocation(self, human=True):
        return self.pool_stat(human=human)['allocation']

    @property
    def available(self, human=True):
        return self.pool_stat(human=human)['available']

    def is_pool_active(self):
        return self.active

    def pool_info(self):
        """
        Return a dict of pool's info.

        raise DirectoryPoolInactiveError: if pool is not active
        """
        if not self.is_pool_active():
            raise DirectoryPoolInactiveError(self.name, 'pool_info')

        info_dict = self.pool_stat()
        keys = ['name', 'uuid', 'config']
        for key in keys:
            info_dict[key] = getattr(self, key)
        info_dict['state'] = 'active' if self.active else 'inactive'
        return info_dict

    def get_vol_info(self, vol_name, verbose=False, human=True):
        path = self.volumes[vol_name][0]
        if not (self.is_volumes_existed and os.path.isfile(path)):
            raise PoolVolNotExistError(self.name, vol_name)

        info_dict = {'path': path}
        if not verbose:
            return info_dict

        vol_stat = os.stat(path)
        info_dict['size'] = vol_stat.st_size
        if hasattr(vol_stat, 'st_blocks'):
            info_dict['allocated_size'] = vol_stat.st_blocks * 512
        info_dict['mode'] = stat.S_IMODE(vol_stat.st_mode)
        info_dict['owner'] = vol_stat.st_uid
        info_dict['group'] = vol_stat.st_gid
        info_dict['timestamp'] = dict()
        for label in ['atime', 'mtime', 'ctime']:
            info_dict['timestamp'][label] = getattr(vol_stat, 'st_%s' % label)

        if human:
            info_dict['size'] = format_size_human_readable(
                info_dict['size'],
                binary=True
                )
            if info_dict.get('allocated_size'):
                info_dict['allocated_size'] = format_size_human_readable(
                    info_dict['allocated_size'],
                    binary=True
                    )

        return info_dict

    def pool_list_vols(self, verbose=False, human=True):
        """
        List out volumes contained in this pool.

        param verbose: verbose output
        param human: human friendly size output if verbose=True
        """
        info_dict = dict()
        for vol_name in self.volumes:
            info_dict[vol_name] = self.get_vol_info(vol_name,
                                                    verbose=verbose,
                                                    human=human)
        return info_dict

    def is_volumes_existed(self, name):
        return name in self.volumes

    def create_volume(self, vol_name, vol_format):
        """
        Return an URL of the volume to be created.

        :param name: name of volume to be created
        """
        if self.is_pool_active() and self.is_volumes_existed(vol_name):
                return self.volumes[vol_name][1]

        url = self._format_as_url(vol_name)
        if self.is_pool_active():
            self.volumes[vol_name][0] = self._format_vol_path(vol_name)
            self.volumes[vol_name][1] = self._format_as_url(vol_name)
            self.volumes[vol_name][2] = self._format_as_json(self.volumes[
                vol_name
                ][0], vol_format)
        return url

    def delete_volume(self, vol_name):
        """
        Delete volume.

        raise DirectoryPoolInactiveError: if the pool is inactive
        """
        if not self.is_pool_active():
            raise DirectoryPoolInactiveError(self.name, 'delete_volume')

        if vol_name not in self.volumes:
            raise PoolVolNotExistError(self.name, vol_name)

        vol_path = self.volumes[vol_name][0]
        if os.path.isfile(vol_path):
            os.remove(vol_path)
        del self.volumes[vol_name]

    def clone_volume(self, source_vol, dest_vol):
        """
        clone a volume.

        raise DirectoryPoolInactiveError: if the pool is inactive
        """
        if not self.is_pool_active():
            raise DirectoryPoolInactiveError(self.name, 'clone_volume')


if __name__ == '__main__':
    sep = '-' * 20
    d = DirectoryPool('demo', dict(path='/home/test/'))
    d.pool_start()
    print(d.pool_info())
    print(sep)
    print(d.pool_stat(human=False))
    print(sep)
    d.create_volume('test.qcow2', 'qcow2')
    print(sep)
    cmd = "qemu-img create -f qcow2 /home/test/test.qcow2 20G"
    os.system(cmd)
    print(sep)
    d.create_volume('test.raw', 'raw')
    cmd = "qemu-img create -f raw -o preallocation=full /home/test/test.raw 1G"
    os.system(cmd)
    print(d.volumes)
    print(sep)
    print(d.pool_list_vols(verbose=True))
