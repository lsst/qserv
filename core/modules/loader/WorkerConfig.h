// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
 *
 */
#ifndef LSST_QSERV_LOADER_WORKERCONFIG_H
#define LSST_QSERV_LOADER_WORKERCONFIG_H

// Qserv headers
#include "loader/ConfigBase.h"

namespace lsst {
namespace qserv {
namespace loader {

/// A class for reading the configuration file for the worker which consists of
/// a collection of key-value pairs and provide access functions for those values.
///
class WorkerConfig : public ConfigBase {
public:
    WorkerConfig(std::string configFileName)
        : WorkerConfig(util::ConfigStore(configFileName)) {}

    WorkerConfig() = delete;
    WorkerConfig(WorkerConfig const&) = delete;
    WorkerConfig& operator=(WorkerConfig const&) = delete;

    std::string getMasterHost() { return _masterHost->getValue(); }
    int getMasterPortUdp() { return std::stoi(_masterPortUdp->getValue()); }
    int getWPortUdp() { return std::stoi(_wPortUdp->getValue()); }
    int getWPortTcp() { return std::stoi(_wPortTcp->getValue()); }
    int getThreadPoolSize() { return std::stoi(_threadPoolSize->getValue()); }
    int getRecentAddLimit() { return std::stoi(_recentAddLimit->getValue()); }
    double getThresholdNeighborShift() { return std::stod(_thresholdNeighborShift->getValue()); }
    int getMaxKeysToShift() { return std::stoi(_maxKeysToShift->getValue()); }
    int getLoopSleepTime() { return std::stoi(_loopSleepTime->getValue()); }

    std::ostream& dump(std::ostream &os) const override;

    std::string const header{"worker"};
private:
    WorkerConfig(util::ConfigStore const& configStore);

    /// Master host name
    ConfigElement::Ptr _masterHost{ConfigElement::create(_list, header, "masterHost", true)};
    /// Master UDP port
    ConfigElement::Ptr _masterPortUdp{ConfigElement::create(_list, header, "masterPortUdp", true)};
    /// UDP port for this worker. Reasonable value - 9876
    ConfigElement::Ptr _wPortUdp{ConfigElement::create(_list, header, "wPortUdp", true)};
    /// TCP port for this worker. Reasonable value - 9877
    ConfigElement::Ptr _wPortTcp{ConfigElement::create(_list, header, "wPortTcp", true)};
    /// Size of the thread pool. Reasonable value - 10
    ConfigElement::Ptr _threadPoolSize{ConfigElement::create(_list, header, "threadPoolSize", true)};
    /// Time limit for for a key added to the system to be considered recent seconds - 60000 = 1 minute
    ConfigElement::Ptr _recentAddLimit{ConfigElement::create(_list, header, "recentAddLimit", true)};
    /// If a worker has this many times the number of keys as the neighbor, keys should be shifted to
    /// the neighbor. "1.10" indicates keys should be shifted if one worker has 10% or more keys
    /// than the other.
    ConfigElement::Ptr _thresholdNeighborShift{ConfigElement::create(_list, header,
                                                                     "thresholdNeighborShift", true)};
    /// The maximum number of keys to shift in a single iteration. During a shift iteration,
    /// there are no new key inserts or lookups. 10000 may be a reasonable value.
    ConfigElement::Ptr _maxKeysToShift{ConfigElement::create(_list, header, "maxKeysToShift", true)};
    /// Time spent sleeping between checking elements in the DoList in micro seconds. 100000
    ConfigElement::Ptr _loopSleepTime{ConfigElement::create(_list, header, "loopSleepTime", false, "100000")};
};


}}} // namespace lsst::qserv::loader

#endif // LSST_QSERV_LOADER_WORKERCONFIG_H
