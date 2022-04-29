// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2016 AURA/LSST.
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

// Class header
#include "css/EmptyChunks.h"

// System headers
#include <algorithm>
#include <iterator>
#include <fstream>
#include <functional>
#include <memory>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "css/CssError.h"
#include "css/DbInterfaceMySql.h"
#include "global/ConfigError.h"
#include "global/stringUtil.h"

using lsst::qserv::ConfigError;
using lsst::qserv::IntSet;

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.css.EmptyChunks");

string makeFilename(string const& db) { return "empty_" + lsst::qserv::sanitizeName(db) + ".txt"; }

}  // anonymous namespace

namespace lsst::qserv::css {

shared_ptr<IntSet const> EmptyChunks::getEmpty(string const& db) {
    lock_guard<mutex> lock(_setsMutex);
    IntSetMap::const_iterator i = _sets.find(db);
    if (i != _sets.end()) {
        IntSetConstPtr readOnly = i->second;
        return readOnly;
    }
    IntSetPtr newSet = make_shared<IntSet>();
    *newSet = _populate(db);
    _sets.insert(IntSetMap::value_type(db, newSet));
    return IntSetConstPtr(newSet);
}

bool EmptyChunks::isEmpty(string const& db, int chunk) {
    IntSetConstPtr s = getEmpty(db);
    return s->end() != s->find(chunk);
}

void EmptyChunks::clearCache(string const& db) const {
    if (db.empty()) {
        LOGS(_log, LOG_LVL_DEBUG, "Clearing empty chunks cache for all databases");
        _sets.clear();
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "Clearing empty chunks cache for database " << db);
        _sets.erase(db);
    }
}

IntSet EmptyChunks::_populate(string const& db) {
    // Try to open database table
    LOGS(_log, LOG_LVL_DEBUG, "populate " << db);
    IntSet s;
    try {
        if (_dbI == nullptr) throw CssError(ERR_LOC, "database==nullptr");
        s = _dbI->getEmptyChunks(db);
        return s;
    } catch (CssError const& e) {
        LOGS(_log, LOG_LVL_WARN,
             " failed to read empty chunks from table. Trying file. " + db + " " + e.what());
    }

    // Since the table wasn't found, use the empty chunks file
    // TODO: Once everything is using the table, this should be deleted and the error above should be thrown.
    string const best = _path + "/" + makeFilename(db);
    LOGS(_log, LOG_LVL_WARN, "Trying path:" << _path << " emptyChunk file:" << best);
    string fileName = best;
    ifstream rawStream(best.c_str());
    if (!rawStream.good()) {  // On error, try using default filename
        rawStream.close();
        rawStream.open(_fallbackFile.c_str());
        fileName = _fallbackFile;
    }
    LOGS(_log, LOG_LVL_DEBUG, "Reading empty chunks for db " << db << " from file " << fileName);
    if (!rawStream.good()) {
        string eMsg(string("No such empty chunks file: ") + best + " or " + _fallbackFile);
        LOGS(_log, LOG_LVL_ERROR, eMsg);
        throw CssError(ERR_LOC, eMsg);
    }
    istream_iterator<int> chunkStream(rawStream);
    istream_iterator<int> eos;
    copy(chunkStream, eos, insert_iterator<IntSet>(s, s.begin()));
    return s;
}

}  // namespace lsst::qserv::css
