// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2015 LSST Corporation.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */
#ifndef LSST_QSERV_CONSTANTS_H
#define LSST_QSERV_CONSTANTS_H
 /**
  * @brief  Global constants.
  *
  */

namespace lsst {
namespace qserv {
const char CHUNK_COLUMN[] = "chunkId";
const char SUB_CHUNK_COLUMN[] = "subChunkId";
const int DUMMY_CHUNK = 1234567890;

const char SEC_INDEX_DB[] = "qservMeta";

const char SUBCHUNKDB_PREFIX[] = "Subchunks_";
const char SCISQLDB_PREFIX[] = "scisql_";

const char MEMLOCKDB[] = "q_memoryLockDb";
const char MEMLOCKTBL[] = "memoryLockTbl";

/// `SUBCHUNK_TAG` is a pattern that is replaced with a subchunk number
/// when generating concrete query text from a template.
const char SUBCHUNK_TAG[] = "%S\007S%";
/// `CHUNK_TAG` is a pattern that is replaced with a chunk number
/// when generating concrete query text from a template.
const char CHUNK_TAG[] = "%C\007C%";

/**
 * The absolute maximum number of job attempts. The number
 * of attempts before cancelling a query can (and probably should)
 * be smaller than this.
 * This is value is used for encoding jobId and attemptCount.
 * For readability values should be 10, 100, 1000, etc.
 */
const int MAX_JOB_ATTEMPTS = 100;

/// Used for undefined variable which should contain positive integer
const int NOTSET = -1;

/** Allow to classify messages stored in qdisp::MessageStore
 *
 * In mysql-proxy MSG_INFO message will go to logfile,
 * whereas MSG_ERROR message will be logged to console
 *
 * @warning mysql enum index start from 1
 */
enum MessageSeverity { MSG_INFO=1, MSG_ERROR } ;

}}
#endif // LSST_QSERV_CONSTANTS_H
