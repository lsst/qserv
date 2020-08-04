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
    def add_metrics(self, name, tags={}, **kw):
        """Add more metrics to the monitor.

        Parameters
        ----------
        name : `str`
            Metric name.
        tags : `dict` [`str`, `Any`]
            Dictionary with tags and tag values for the new measurement, tag
            is a name, it's better to have tag names in the form of regular
            identifiers. Tag value can be a string or an integer number.
        **kw :
            All measurements as dictionary of measurement name and its value.
        """
        raise NotImplementedError()

    @abstractmethod
    def close(self):
        """Flush all data and close destination.
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

    def add_metrics(self, name, tags={}, **kw):

        values = ", ".join(f"{k}={v}" for k, v in kw.items())
        tags = ", ".join(f"{k}={v}" for k, v in tags.items())

        # add explicit timestamp in microces since epoch
        timestamp = int(round(time.time() * 1e6))

        self._logger.log(self._level, "%s: %s {%s} tags={%s} time=%s",
                         self._prefix, name, values, tags, timestamp)

    def close(self):
        pass


class InfluxDBFileMonitor(Monitor):
    """Monitor which dumps everything to a file in InfluxDB format.

    It supports rollover after specified period of time.

    Parameters
    ----------
    path : `str`
        Name of the output file which can contain "%T" pattern to be replaced
        with the actual timestamp in the format YYMMDDTHHMMSS. If ``perodSec``
        is not None and "%T" is missing from the path then it will be
        implicitly added as a suffix of the file name.
    periodSec : `int`, optional
        Rollover period in seconds. If not specified then there is no
        rollover.
    dbname : `str`, optional
        Name of InfluxDB database to add to the files.
    """
    def __init__(self, path, periodSec=None, dbname=None):
        self._path = path
        self._period = periodSec
        self._dbname = dbname

        # add %T if needed
        if self._period is not None and "%T" not in self._path:
            self._path += ".%T"

        self._openTime = None
        self._file = None
        self.path = None

        self._open()

    def add_metrics(self, name, tags={}, **kw):

        now = time.time()

        # rollover to a new file if needed
        if self._period is not None and now - self._openTime >= self._period:
            self._open()

        tags = [name] + [f"{k}={v}" for k, v in tags.items()]
        tags = ",".join(tags)
        values = ",".join(f"{k}={v}" for k, v in kw.items())
        ts = int(now * 1e6)
        print(f"{tags} {values} {ts}", file=self._file)

    def close(self):
        """Close output file"""
        if self._file:
            self._file.close()

    def _open(self):
        """Open next file
        """
        if self._file:
            self._file.close()

        self._openTime = time.time()
        self.path = self._path
        if "%T" in self.path:
            ts = time.strftime("%Y%m%dT%H%M%S", time.localtime(self._openTime))
            self.path = self.path.replace("%T", ts)

        _LOG.debug("Opening InfluxDB output file %s", self.path)
        self._file = open(self.path, "w")

        # starts with header
        print("# DML", file=self._file)
        if self._dbname:
            print(f"# CONTEXT-DATABASE: {self._dbname}", file=self._file)


class AddTagsMonitor(Monitor):
    """Monitors that adds a set of tags to metrics.

    Parameters
    ----------
    monitor : `Monitor`
        Other monitor that alldata is sent to.
    tags : `dict` [`str`, `Any`]
        Dictionary with tags and tag values to add to all metrics.
        These tags can overwrite tags sent with metrics.
    """
    def __init__(self, monitor, tags):
        self._monitor = monitor
        self._tags = tags

    def add_metrics(self, name, tags={}, **kw):
        updated_tags = tags.copy()
        updated_tags.update(self._tags)
        self._monitor.add_metrics(name, tags=updated_tags, **kw)

    def close(self):
        self._monitor.close()
