// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 AURA/LSST.
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
#include "loader/ConfigBase.h"

// System headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "util/ConfigStore.h"
#include "util/ConfigStoreError.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.Config");
}


namespace lsst {
namespace qserv {
namespace loader {


void ConfigElement::setFromConfig(util::ConfigStore const& cfgStore) {
    if (_required) {
        _value = cfgStore.getRequired(getFullKey());
    } else {
        _value = cfgStore.get(getFullKey(), _default);
    }
}


std::string ConfigElement::getFullKey() const {
    if (_header.empty()) return std::string(_key);
    return std::string(_header + "." + _key); }


std::ostream& ConfigElement::dump(std::ostream &os) const {
    os << "(key=" << getFullKey()
       << " val=" << _value
       << " req=" << _required
       << " def=" << _default << ")";
    return os;
}


std::string ConfigElement::dump() const {
    std::stringstream os;
    dump(os);
    return os.str();
}


std::ostream& operator<<(std::ostream &os, ConfigElement const& elem) {
    return elem.dump(os);
}


void ConfigBase::setFromConfig(util::ConfigStore const& configStore) {
    for (auto& elem:_list) {
        elem->setFromConfig(configStore);
    }
}


std::ostream& ConfigBase::dump(std::ostream &os) const {
    os << "(ConfigBase: ";
    for (auto&& elem:_list) {
        os << *elem << " ";
    }
    os << ")";
    return os;
}


std::string ConfigBase::dump() const {
    std::stringstream os;
    dump(os);
    return os.str();
}


std::ostream& operator<<(std::ostream &os, ConfigBase const& cfg) {
    return cfg.dump(os);
}


}}} // namespace lsst::qserv::loader


