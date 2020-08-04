"""Classes for monitoring of test harness.
"""

from abc import ABC, abstractmethod
import logging
import time


_LOG = logging.getLogger(__name__)


class Monitor(ABC):
    """Interface definition for monitoring.
    """

    @abstractmethod
    def add_metrics(self, tags={}, **kw):
        """Add more metrics to the monitor.

        Parameters
        ----------
        tags : `dict` [`str`, `Any`]
            Dictionary with tags and tag values for the new measurement, tag
            is a name, it's better to have tag names in the form of regular
            identifiers. Tag value can be a string or an integer number.
        **kw :
            All measurements as dictionary of measurement name and its value.
        """
        raise NotImplementedError()


class LogMonitor(Monitor):
    """Monitor which dumps everything to a specified logger.

    Parameters
    ----------
    logger : `str` or `logging.Logger`
        Name of the logger or logger instance.
    level : `int`, optional
        Logging level for messages, default is INFO.
    prefix : `str`, optional
        Text to add to each message.
    """
    def __init__(self, logger, level=logging.INFO, prefix="monitor"):
        if not isinstance(logger, logging.Logger):
            logger = logging.getLogger(logger)
        self._logger = logger
        self._level = level
        self._prefix = prefix

    def add_metrics(self, tags={}, **kw):

        values = ", ".join(f"{k}={v}" for k, v in kw.items())
        tags = ", ".join(f"{k}={v}" for k, v in tags.items())

        # add explicit timestamp in microces since epoch
        timestamp = int(round(time.time() * 1e6))

        self._logger.log(self._level, "%s: measurements={%s} tags={%s} time=%s",
                         self._prefix, values, tags, timestamp)
