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
#ifndef LSST_QSERV_LOADER_CLIENTCONFIG_H
#define LSST_QSERV_LOADER_CLIENTCONFIG_H

// Qserv headers
#include "loader/ConfigBase.h"

namespace lsst {
namespace qserv {
namespace loader {

/// A class for reading the configuration file for the client which consists of
/// a collection of key-value pairs and provide access functions for those values.
///
class ClientConfig : public ConfigBase{
public:
    ClientConfig(std::string configFileName)
        : ClientConfig(util::ConfigStore(configFileName)) {}

    ClientConfig() = delete;
    ClientConfig(ClientConfig const&) = delete;
    ClientConfig& operator=(ClientConfig const&) = delete;

    std::string getMasterHost() const { return _masterHost->getValue(); }
    int getMasterPortUdp() const { return std::stoi(_masterPortUdp->getValue()); }
    int getDefWorkerPortUdp() const { return std::stoi(_defWorkerPortUdp->getValue()); }
    std::string getDefWorkerHost() const { return _defWorkerHost->getValue(); }
    int getClientPortUdp() const { return std::stoi(_clientPortUdp->getValue()); }
    int getThreadPoolSize() const { return std::stoi(_threadPoolSize->getValue()); }
    int getLoopSleepTime() const { return std::stoi(_loopSleepTime->getValue()); }
    int getMaxLookups() const { return std::stoi(_maxLookups->getValue()); }
    int getMaxInserts() const { return std::stoi(_maxInserts->getValue()); }

    std::ostream& dump(std::ostream &os) const override;

    std::string const header{"client"};
private:
    ClientConfig(util::ConfigStore const& configStore);

    /// Master host name
    ConfigElement::Ptr _masterHost{ConfigElement::create(cfgList, header, "masterHost", true)};
    /// Master UDP port
    ConfigElement::Ptr _masterPortUdp{ConfigElement::create(cfgList, header, "masterPortUdp", true)};
    /// UDP port for default worker. Reasonable value - 9876
    ConfigElement::Ptr _clientPortUdp{ConfigElement::create(cfgList, header, "clientPortUdp", true)};
    /// Default worker host name
    ConfigElement::Ptr _defWorkerHost{ConfigElement::create(cfgList, header, "defWorkerHost", true)};
    /// Default worker UDP port. Reasonable value - 9876
    ConfigElement::Ptr _defWorkerPortUdp{ConfigElement::create(cfgList, header, "defWorkerPortUdp", true)};
    /// Size of the thread pool. Reasonable value - 10
    ConfigElement::Ptr _threadPoolSize{ConfigElement::create(cfgList, header, "threadPoolSize", true)};
    /// Time spent sleeping between checking elements in the DoList in micro seconds. 100000
    ConfigElement::Ptr _loopSleepTime{
        ConfigElement::create(cfgList, header, "loopSleepTime", false, "100000")};
    /// Maximum number of lookup requests allowed in the DoList.
    ConfigElement::Ptr _maxLookups{
        ConfigElement::create(cfgList, header, "maxLookups", false, "90000")};
    /// Maximum number of insert requests allowed in the DoList.
    ConfigElement::Ptr _maxInserts{
        ConfigElement::create(cfgList, header, "maxInserts", false, "90000")};

};


}}} // namespace lsst::qserv::loader

#endif // LSST_QSERV_LOADER_ClientCONFIG_H
