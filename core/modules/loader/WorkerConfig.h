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

    std::string getMasterHost() const { return _masterHost->getValue(); }
    int getMasterPortUdp() const { return std::stoi(_masterPortUdp->getValue()); }
    int getWPortUdp() const { return std::stoi(_wPortUdp->getValue()); }
    int getWPortTcp() const { return std::stoi(_wPortTcp->getValue()); }
    int getThreadPoolSize() const { return std::stoi(_threadPoolSize->getValue()); }
    int getRecentAddLimit() const { return std::stoi(_recentAddLimit->getValue()); }
    double getThresholdNeighborShift() const { return std::stod(_thresholdNeighborShift->getValue()); }
    int getMaxKeysToShift() const { return std::stoi(_maxKeysToShift->getValue()); }
    int getLoopSleepTime() const { return std::stoi(_loopSleepTime->getValue()); }

    std::ostream& dump(std::ostream &os) const override;

    std::string const header{"worker"};
private:
    WorkerConfig(util::ConfigStore const& configStore);

    /// Master host name
    ConfigElement::Ptr _masterHost{ConfigElement::create(cfgList, header, "masterHost", true)};
    /// Master UDP port
    ConfigElement::Ptr _masterPortUdp{ConfigElement::create(cfgList, header, "masterPortUdp", true)};
    /// UDP port for this worker. Reasonable value - 9876
    ConfigElement::Ptr _wPortUdp{ConfigElement::create(cfgList, header, "wPortUdp", true)};
    /// TCP port for this worker. Reasonable value - 9877
    ConfigElement::Ptr _wPortTcp{ConfigElement::create(cfgList, header, "wPortTcp", true)};
    /// Size of the thread pool. Reasonable value - 10
    ConfigElement::Ptr _threadPoolSize{ConfigElement::create(cfgList, header, "threadPoolSize", true)};
    /// Time limit for for a key added to the system to be considered recent seconds - 60000 = 1 minute
    ConfigElement::Ptr _recentAddLimit{ConfigElement::create(cfgList, header, "recentAddLimit", true)};
    /// If a worker has this many times the number of keys as the neighbor, keys should be shifted to
    /// the neighbor. "1.10" indicates keys should be shifted if one worker has 10% or more keys
    /// than the other.
    ConfigElement::Ptr _thresholdNeighborShift{ConfigElement::create(cfgList, header,
                                                                     "thresholdNeighborShift", true)};
    /// The maximum number of keys to shift in a single iteration. During a shift iteration,
    /// there are no new key inserts or lookups. 10000 may be a reasonable value.
    ConfigElement::Ptr _maxKeysToShift{ConfigElement::create(cfgList, header, "maxKeysToShift", true)};
    /// Time spent sleeping between checking elements in the DoList in micro seconds. 100000
    ConfigElement::Ptr _loopSleepTime{ConfigElement::create(cfgList, header, "loopSleepTime", false, "100000")};
};


}}} // namespace lsst::qserv::loader

#endif // LSST_QSERV_LOADER_WORKERCONFIG_H
