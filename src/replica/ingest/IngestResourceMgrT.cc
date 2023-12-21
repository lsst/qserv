/*
 * LSST Data Management System
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
#include "replica/ingest/IngestResourceMgrT.h"

using namespace std;

namespace lsst::qserv::replica {

shared_ptr<IngestResourceMgrT> IngestResourceMgrT::create() {
    return shared_ptr<IngestResourceMgrT>(new IngestResourceMgrT());
}

unsigned int IngestResourceMgrT::asyncProcLimit(string const& databaseName) const {
    throwIfEmpty(__func__, databaseName);
    unique_lock<mutex> lock(_mtx);
    if (auto const itr = _asyncProcLimit.find(databaseName); itr != _asyncProcLimit.end()) {
        return itr->second;
    }
    return 0;
}

void IngestResourceMgrT::setAsyncProcLimit(string const& databaseName, unsigned int limit) {
    throwIfEmpty(__func__, databaseName);
    unique_lock<mutex> lock(_mtx);
    if (auto itr = _asyncProcLimit.find(databaseName); itr != _asyncProcLimit.end()) {
        if (limit == 0) {
            _asyncProcLimit.erase(itr);
        } else {
            itr->second = limit;
        }
    } else {
        if (limit != 0) _asyncProcLimit[databaseName] = limit;
    }
}

}  // namespace lsst::qserv::replica
