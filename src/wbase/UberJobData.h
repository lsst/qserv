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

#ifndef LSST_QSERV_WBASE_UBERJOBDATA_H
#define LSST_QSERV_WBASE_UBERJOBDATA_H

// System headers
#include <atomic>
#include <fstream>
#include <memory>
#include <mutex>
#include <vector>

// Third-party headers

// Qserv headers
#include "global/intTypes.h"
#include "qmeta/types.h"
#include "wbase/SendChannel.h"

namespace lsst::qserv::wbase {

// &&&uj doc
class UberJobData {
public:
    using Ptr = std::shared_ptr<UberJobData>;

    UberJobData() = delete;
    UberJobData(UberJobData const&) = delete;

    static Ptr create(UberJobId uberJobId, qmeta::CzarId czarId, std::string const& czarHost, int czarPort,
                      uint64_t queryId, std::string const& workerId) {
        return Ptr(new UberJobData(uberJobId, czarId, czarHost, czarPort, queryId, workerId));
    }

    UberJobId getUberJobId() const { return _uberJobId; }
    qmeta::CzarId getCzarId() const { return _czarId; }
    std::string getCzarHost() const { return _czarHost; }
    int getCzarPort() const { return _czarPort; }
    uint64_t getQueryId() const { return _queryId; }
    std::string getWorkerId() const { return _workerId; }

private:
    UberJobData(UberJobId uberJobId, qmeta::CzarId czarId, std::string czarHost, int czarPort,
                uint64_t queryId, std::string const& workerId);

    UberJobId const _uberJobId;
    qmeta::CzarId const _czarId;
    std::string const _czarHost;
    int const _czarPort;
    uint64_t const _queryId;
    std::string const& _workerId;  //&&&uj should be able to get this from the worker in a reasonable way.
};

}  // namespace lsst::qserv::wbase

#endif  // LSST_QSERV_WBASE_UBERJOBDATA_H
