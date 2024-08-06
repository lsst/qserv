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
#ifndef LSST_QSERV_CZAR_ACTIVEWORKER_H
#define LSST_QSERV_CZAR_ACTIVEWORKER_H

// System headers
#include <chrono>
#include <map>
#include <memory>
#include <set>
#include <string>

// qserv headers
#include "global/clock_defs.h"
#include "global/intTypes.h"

// This header declarations
namespace lsst::qserv::czar {

class WorkerContactInfo {
public:
    using Ptr = std::shared_ptr<WorkerContactInfo>;

    WorkerContactInfo(std::string const& wId_, std::string const& wHost_,
            std::string const& wManagementHost_, int wPort_, TIMEPOINT updateTime_)
    : wId(wId_),
      wHost(wHost_),
      wManagementHost(wManagementHost_),
      wPort(wPort_) {
        touchedChanged(updateTime_, false);
    }
    std::string const wId;              ///< key
    std::string const wHost;            ///< "host-addr" entry.
    std::string const wManagementHost;  ///< "management-host-name" entry.
    int const wPort;                    ///< "management-port" entry.


    /// Return true if all members, aside from updateTime, are equal.
    bool sameContactInfo(WorkerContactInfo const& other) const {
        return (wId == other.wId && wHost == other.wHost && wManagementHost == other.wManagementHost &&
                wPort == other.wPort);
    }

    /// To be called when the worker list was updated and there was a change.
    void touchedChanged(TIMEPOINT updateTime, bool missing) {
        _missing = missing;
        touchedNoChange(updateTime);
    }

    /// To be called when the worker list was updated and there was no change.
    void touchedNoChange(TIMEPOINT updateTime = CLOCK::now()) {
        if (!_missing) {
            _lastTouch = updateTime;
        }
    }

    double timeSinceTouchSeconds() {
        double secs = std::chrono::duration_cast<std::chrono::seconds>(CLOCK::now() - _lastTouch).count();
        return secs;
    }

    std::string dump() const;

private:
    TIMEPOINT _lastTouch;  ///< Last time this worker believed to be active.
    bool _missing = false; ///< True if the worker was missing after the last change.
};

/// &&& doc  - maintain list of done/cancelled queries for an active worker, and send that
///            list to the worker. Once the worker has accepted the list, remove all
///            of those queryId's from the list.
class ActiveWorker {
public:
    using Ptr = std::shared_ptr<ActiveWorker>;

    ActiveWorker() = delete;
    ActiveWorker(ActiveWorker const&) = delete;
    ActiveWorker& operator=(ActiveWorker const&) = delete;

    static Ptr create(WorkerContactInfo::Ptr const& wInfo) {
        return Ptr(new ActiveWorker(wInfo));
    }

    ~ActiveWorker() = default;

private:
    ActiveWorker(WorkerContactInfo::Ptr const& wInfo) : _wInfo(wInfo) {}

    std::set<QueryId> _qIdDoneKeepFiles;  ///< &&& doc - limit reached
    std::set<QueryId> _qIdDoneDeleteFiles;  ///< &&& doc -cancelled/finished

    WorkerContactInfo::Ptr const _wInfo;
};

/// &&& doc
class ActiveWorkerMap {
public:
    ActiveWorkerMap() = default;

    void updateMap(std::string const& wId);

private:
    std::map<std::string, ActiveWorker::Ptr> _awMap;
};

}  // namespace lsst::qserv::czar

#endif  // LSST_QSERV_CZAR_ACTIVEWORKER_H
