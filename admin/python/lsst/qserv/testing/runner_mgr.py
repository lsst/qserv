"""Class which manages concurrent execution of queries.
"""

__all__ = ["RunnerManager"]

import logging
from multiprocessing import Process
import time

from .query_runner import QueryRunner

_LOG = logging.getLogger(__name__)


class RunnerManager:
    """Class which manages execution of multiple QueryRunners.

    Parameters
    ----------
    config : `Config`
        Configuration object.
    connectionFactory : callable
        Function or object that makes database connection, mock or real.
    slot : `int`, optional
        Slot number for this manager, can be None.
    queryCountLimit : `int`, option
        Limit number of queries to run, by default run indefinitely.
    runTimeLimit : `float`, option
        Limit for how long to run, in seconds, by default run indefinitely.
    monitor : `Monitor`
        Monitoring instance
    """

    def __init__(self, config, connectionFactory, slot, runTimeLimit=None, monitor=None):
        self._config = config
        self._connectionFactory = connectionFactory
        self._slot = slot
        self._runTimeLimit = runTimeLimit
        self._monitor = monitor

    def run(self):
        """Start all runners and wait until they are done.
        """

        # instantiate all query runners
        runners = {}
        for qclass in self._config.classes():

            n_runners = self._config.concurrentQueries(qclass)
            queries = self._config.queries(qclass)
            _LOG.debug("%s and %s queries for class %s", n_runners, len(queries), qclass)
            maxRate = self._config.maxRate(qclass)
            arraysize = self._config.arraysize(qclass)
            for i_runner in range(n_runners):

                if self._slot is not None:
                    runnerId = f"{qclass}-{self._slot}-{i_runner}"
                else:
                    runnerId = f"{qclass}-{i_runner}"

                _LOG.debug("Creating runner %s", runnerId)
                runner = QueryRunner(queries, maxRate, self._connectionFactory,
                                     runnerId, arraysize, queryCountLimit=None,
                                     runTimeLimit=self._runTimeLimit,
                                     monitor=self._monitor)
                runners[runnerId] = runner

        # start all of them
        processes = {}
        for runnerId, runner in runners.items():
            _LOG.info("Starting runner %s", runnerId)
            proc = Process(target=runner, name=runnerId, daemon=True)
            proc.start()
            processes[runnerId] = proc

        # Now wait until they finish, there is a smarter ways to wait
        # but this is good enough for now.
        while processes:
            for runnerId, proc in processes.items():
                if not proc.is_alive():
                    _LOG.info("Runner %s finished", runnerId)
                    del processes[runnerId]
                    break
            else:
                # don't get too tired
                time.sleep(1)

        _LOG.info("All runners have finished")
