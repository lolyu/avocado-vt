"""
Shared classes and functions (exceptions, ...)

:copyright: 2013 Red Hat Inc.
"""

import six
import heapq

#
# Exceptions
#
class DeviceError(Exception):

    """ General device exception """
    pass


class DeviceInsertError(DeviceError):

    """ Fail to insert device """

    def __init__(self, device, reason, vmdev):
        self.device = device
        self.reason = reason
        self.vmdev = vmdev
        self.issue = "insert"

    def __str__(self):
        return ("Failed to %s device:\n%s\nBecause:\n%s\nList of VM devices:\n"
                "%s\n%s" % (self.issue, self.device.str_long(), self.reason,
                            self.vmdev.str_short(), self.vmdev.str_bus_long()))


class DeviceRemoveError(DeviceInsertError):

    """ Fail to remove device """

    def __init__(self, device, reason, vmdev):
        DeviceInsertError.__init__(self, device, reason, vmdev)
        self.issue = "remove"


class DeviceHotplugError(DeviceInsertError):

    """ Fail to hotplug device """

    def __init__(self, device, reason, vmdev, ver_out=None):
        DeviceInsertError.__init__(self, device, reason, vmdev)
        self.issue = "hotplug"
        self.ver_out = ver_out  # Output of device.verify_hotplug (optionally)


class DeviceUnplugError(DeviceHotplugError):

    """ Fail to unplug device """

    def __init__(self, device, reason, vmdev):
        DeviceHotplugError.__init__(self, device, reason, vmdev)
        self.issue = "unplug"


#
# Utilities
#
def none_or_int(value):
    """ Helper fction which returns None or int() """
    if isinstance(value, int):
        return value
    elif not value:   # "", None, False
        return None
    elif isinstance(value, six.string_types) and value.isdigit():
        return int(value)
    else:
        raise TypeError("This parameter has to be int or none")


def _parse_extra_params(extra_params):
    """Transform param into dictionary."""
    return dict(_.split("=", 1) for _ in extra_params.split(",") if _)


class MinQueue(object):
    def __init__(self, item_list):
        self._queue = list(item_list)
        heapq.heapify(self._queue)

    def __iter__(self):
        return iter(self._queue)

    def __getitem__(self, pos):
        return self._queue[pos]

    def __setitem__(self, pos, value):
        self._queue[pos] = value

    def siftup(self, pos):
        heapq._siftup(self._queue, pos)

    def siftdown(self, pos, startpos=0):
        heapq._siftdown(self._queue, startpos, pos)

    def push(self, item):
        heapq.heappush(self._queue, item)

    def pop(self):
        return heapq.heappop(self._queue)

    def get_min(self):
        return self._queue[0]
