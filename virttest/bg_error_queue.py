"""global background error queue for vt test object."""
import logging

from six.moves import queue


background_errors = queue.Queue()


def clear_bg_errors():
    """Clear all errors in the background error queue"""
    logging.debug("Clearing all background errors.")
    with background_errors.mutex:
        background_errors.queue.clear()
        background_errors.all_tasks_done.notify_all()
        background_errors.unfinished_tasks = 0
        background_errors.not_full.notify_all()
