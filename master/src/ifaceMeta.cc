// -*- LSST-C++ -*-

/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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

#include "lsst/qserv/master/ifaceMeta.h"
#include "lsst/qserv/master/MetadataCache.h"
#include "lsst/qserv/master/SessionManager.h"

#include <boost/make_shared.hpp>

namespace qMaster = lsst::qserv::master;

// ============================================================================
// ===== session related
// ============================================================================
using lsst::qserv::master::SessionManager;
typedef SessionManager<qMaster::MetadataCache::Ptr> SessionMgr;
typedef boost::shared_ptr<SessionMgr> SessionMgrPtr;
namespace {
    SessionMgr& getSessionManager() {
        static SessionMgrPtr sm;
        if(sm.get() == NULL) {
            sm = boost::make_shared<SessionMgr>();
        }
        assert(sm.get() != NULL);
        return *sm;
    }

    qMaster::MetadataCache& getMetadataCache(int session) {
        return *(getSessionManager().getSession(session));
    }
}

int
qMaster::newMetadataSession() {
    MetadataCache::Ptr m = 
        boost::make_shared<qMaster::MetadataCache>();
    return getSessionManager().newSession(m);
}

void
qMaster::resetMetadataSession(int metaSessionId) {
    getMetadataCache(metaSessionId).resetSelf();
}

// ============================================================================
// ===== real work done here
// ============================================================================
int
qMaster::addDbInfoNonPartitioned(int metaSessionId,
                                 char* dbName) {
    return getMetadataCache(metaSessionId).addDbInfoNonPartitioned(dbName);
}

int
qMaster::addDbInfoPartitionedSphBox(int metaSessionId,
                                    char* dbName,
                                    int nStripes,
                                    int nSubStripes,
                                    float defOverlapF,
                                    float defOverlapNN) {
    return getMetadataCache(metaSessionId).addDbInfoPartitionedSphBox(dbName,
                       nStripes, nSubStripes, defOverlapF, defOverlapNN);
}

int
qMaster::addTbInfoNonPartitioned(int metaSessionId,
                                 char* dbName,
                                 char* tbName) {
    return getMetadataCache(metaSessionId).addTbInfoNonPartitioned(dbName, tbName);
}

int
qMaster::addTbInfoPartitionedSphBox(int metaSessionId,
                                    char* dbName,
                                    char* tbName,
                                    float overlap,
                                    char* phiCol,
                                    char* thetaCol,
                                    int phiColNo,
                                    int thetaColNo,
                                    int logicalPart,
                                    int physChunking) {
    return getMetadataCache(metaSessionId).addTbInfoPartitionedSphBox(
                 dbName, tbName, overlap, phiCol, thetaCol, phiColNo, 
                 thetaColNo, logicalPart, physChunking);
}

void
qMaster::resetMetadataCache(int metaSessionId) {
    getMetadataCache(metaSessionId).resetSelf();
}

void
qMaster::printMetadataCache(int metaSessionId) {
    getMetadataCache(metaSessionId).printSelf();
}
