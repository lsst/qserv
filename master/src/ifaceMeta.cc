// -*- LSST-C++ -*-

/* 
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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

/**
  * @file ifaceMeta.xx
  *
  * @brief Interface used for exporting C++ metadata cache to python through swig.
  *
  * @Author Jacek Becla, SLAC
  */


#include "lsst/qserv/master/ifaceMeta.h"
#include "lsst/qserv/master/MetadataCache.h"
#include "lsst/qserv/master/SessionManager.h"

#include <boost/make_shared.hpp>

namespace qMaster = lsst::qserv::master;

// ============================================================================
// ===== session related
// ============================================================================
namespace lsst {
namespace qserv {
namespace master {
        
using lsst::qserv::master::SessionManager;
typedef SessionManager<qMaster::MetadataCache::Ptr> SessionMgr;
typedef boost::shared_ptr<SessionMgr> SessionMgrPtr;
SessionMgr&
getSessionManager() {
    static SessionMgrPtr sm;
    if(sm.get() == NULL) {
        sm = boost::make_shared<SessionMgr>();
    }
    assert(sm.get() != NULL);
    return *sm;
}

typedef boost::shared_ptr<qMaster::MetadataCache> MetaCachePtr;
MetaCachePtr
getMetadataCache(int session) {
    MetaCachePtr x = getSessionManager().getSession(session);
    assert(x != NULL); // if you get an assert here, you passed an invalid session
    return x;
}
}}}

/** Creates a new metadata session
  */
int
qMaster::newMetadataSession() {
    MetadataCache::Ptr m = 
        boost::make_shared<qMaster::MetadataCache>();
    return getSessionManager().newSession(m);
}

/** Destroys existing metadata session.
  * @param metaSessionId id of the metadata session
  */
void
qMaster::discardMetadataSession(int metaSessionId) {
    getSessionManager().discardSession(metaSessionId);
}

// ============================================================================
// ===== real work done here
// ============================================================================

/** Adds database information for a non-partitioned database.
  * @param metaSessionId id of the metadata session
  * @param dbName database name
  */
int
qMaster::addDbInfoNonPartitioned(int metaSessionId,
                                 const char* dbName) {
    return getMetadataCache(metaSessionId)->addDbInfoNonPartitioned(dbName);
}

/** Adds database information for a partitioned database,
  * which use spherical partitioning mode.
  *
  * @param metaSessionId id of the metadata session
  * @param dbName database name
  * @param nStripes number of stripes
  * @param nSubStripes number of sub-stripes
  * @param defOverlapF default overlap for 'fuzziness'
  * @param defOverlapNN default overlap for 'near-neighbor'-type queries
  *
  * @return returns status (0 on success)
  */
int
qMaster::addDbInfoPartitionedSphBox(int metaSessionId,
                                    const char* dbName,
                                    int nStripes,
                                    int nSubStripes,
                                    float defOverlapF,
                                    float defOverlapNN) {
    return getMetadataCache(metaSessionId)->addDbInfoPartitionedSphBox(dbName,
                       nStripes, nSubStripes, defOverlapF, defOverlapNN);
}

/** Adds information about a non-partitioned table.
  *
  * @param metaSessionId id of the metadata session
  * @param dbName database name
  * @param tableName table name
  *
  * @return returns status (0 on success)
  */
int
qMaster::addTbInfoNonPartitioned(int metaSessionId,
                                 const char* dbName,
                                 const char* tbName) {
    return getMetadataCache(metaSessionId)->addTbInfoNonPartitioned(dbName, tbName);
}

/** Adds database information for a partitioned table,
  * which use spherical partitioning mode.
  *
  * @param metaSessionId id of the metadata session
  * @param dbName database name
  * @param tableName table name
  * @param overlap used for this table (overwrites overlaps from dbInfo)
  * @param phiCol name of the phi col (right ascention)
  * @param thetaCol name of the theta col (declination)
  * @param phiColNo position of the phi col in the table, counting from zero
  * @param thetaColNo position of the theta col in the table, counting from zero
  * @param logicalPart definition how the table is partitioned logically
  * @param physChunking definition how the table is chunked physically
  *
  * @return returns status (0 on success)
  */
int
qMaster::addTbInfoPartitionedSphBox(int metaSessionId,
                                    const char* dbName,
                                    const char* tbName,
                                    float overlap,
                                    const char* phiCol,
                                    const char* thetaCol,
                                    int phiColNo,
                                    int thetaColNo,
                                    int logicalPart,
                                    int physChunking) {
    return getMetadataCache(metaSessionId)->addTbInfoPartitionedSphBox(
                 dbName, tbName, overlap, phiCol, thetaCol, phiColNo, 
                 thetaColNo, logicalPart, physChunking);
}

/** Prints the contents of the qserv metadata cache. This is
  * handy for debugging.
  */
void
qMaster::printMetadataCache(int metaSessionId) {
    getMetadataCache(metaSessionId)->printSelf();
}
