# LSST Data Management System
# Copyright 2014 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.

"""
Module defining ChunkMapping class and related methods.

ChunkMapping class is used to manage information about which chunk
exists on which worker. It stores, retrieves and updates this info
in CSS.

@author  Andy Salnikov, SLAC
"""

from __future__ import absolute_import, division, print_function

# --------------------------------
#  Imports of standard modules --
# --------------------------------
import logging

# -----------------------------
# Imports for other modules --
# -----------------------------
from lsst.qserv import css
# ----------------------------------
# Local non-exported definitions --
# ----------------------------------

# ------------------------
# Exported definitions --
# ------------------------


class ChunkMapping(object):
    """
    Instances of ChunkMapping class represent updatable mapping between
    chunk numbers and worker names (host names).
    """

    def __init__(self, workers, database, table, css=None):
        """
        @param workers:      List of worker names, new chunks will be assigned
                             to one of the workers in this list.
        @param database:     Database name.
        @param table:        Table name.
        @param css:          Instance of CssAccess class, CSS interface. This
                             can be None, but this is only useful for testing.
        """
        self.css = css
        self.workers = workers[:]   # need a copy, will modify
        self.database = database
        self.table = table

        self.allChunks = {}  # maps chunk number to the set of workers (for all tables)
        self.chunks = {}  # maps chunk number to the set of workers (for current table)
        self.newChunks = {}     # maps chunk number to the worker name for new chunks

        self._log = logging.getLogger("chunk_map")

        self._getChunks()

    def _getChunks(self):
        """
        Load existing chunk mapping from CSS.

        Currently there is no existing global chunk mapping in CSS, only
        per-table. We load chunk mappings from all existing tables and
        merge them.
        """

        if self.css is None:
            return

        try:
            tables = self.css.getTableNames(self.database, False)
        except css.NoSuchDb:
            return

        for table in tables:

            try:
                chunks = self.css.getChunks(self.database, table)
            except css.NoSuchTable:
                return

            if chunks:
                self._log.debug('Loading mapping for table %r', table)

            for chunk, hosts in chunks.items():
                self.allChunks.setdefault(chunk, set()).update(hosts)
                if table == self.table:
                    self.chunks.setdefault(chunk, set()).update(hosts)

    def worker(self, chunk):
        """
        Returns worker name for specified chunk number.

        If there is no existing mapping for this chunk yet, then new mapping
        will be created (currently in round-robin mode from the set of known
        worker names). New mappings have to be saved later by calling save()
        method.
        """

        self._log.debug('Find worker for chunk %r', chunk)

        # try newly-created mapping first
        worker = self.newChunks.get(chunk)
        if worker is not None:
            self._log.debug('Chunk found in new chunks')
            return worker

        # try mapping for this table
        workers = self.chunks.get(chunk)
        if workers is not None:
            self._log.debug('Chunk found in existing table chunks')
            # try to chose one worker which is also in self.workers
            match = set(self.workers) & workers
            if match:
                worker = next(iter(match))
            else:
                # return one random
                worker = next(iter(workers))
            return worker

        # see if other tables define mapping for this chunk
        workers = self.allChunks.get(chunk)
        if workers is not None:
            self._log.debug('Chunk found in existing other-table chunks')
            # try to chose one worker which is also in self.workers
            match = set(self.workers) & workers
            if match:
                worker = next(iter(match))
            else:
                # return one random
                worker = next(iter(workers))
            self.newChunks[chunk] = worker
            return worker

        # will need at least one worker at this point
        if not self.workers:
            raise ValueError('ChunkMapping: Worker list is empty')

        # need to find new "best" location for a chunk
        worker = self._getWorker()
        self.newChunks[chunk] = worker
        return worker

    def save(self):
        """
        Save all new mappings generated for current table.
        """
        if self.css is not None:
            for chunk, worker in self.newChunks.items():
                self.css.addChunk(self.database, self.table, chunk, [worker])

    def _getWorker(self):
        """
        Get next "best" available worker, for now do round-robin.
        """
        worker = self.workers.pop(0)
        self.workers.append(worker)
        return worker
