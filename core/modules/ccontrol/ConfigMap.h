// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

#ifndef LSST_QSERV_CCONTROL_CONFIGMAP_H
#define LSST_QSERV_CCONTROL_CONFIGMAP_H
/**
  * @file
  *
  * @brief Configuration handling for the Czar
   *
  * @author Daniel L. Wang, SLAC
  */

// Third-party headers
#include <boost/shared_ptr.hpp>
#include <boost/utility.hpp>
#include <boost/lexical_cast.hpp>

// LSST headers
#include "lsst/log/Log.h"

// Local headers
#include "global/stringTypes.h"

namespace lsst {
namespace qserv {
namespace ccontrol {

/// ConfigMap is a lightweight facade on top of a basic std::map that performs
/// type coercion. Qserv components querying configuration values use this class
/// to simplify code at the call site.
class ConfigMap {
public:
    typedef boost::shared_ptr<ConfigMap> Ptr;

    ConfigMap(StringMap const& m) : _m(m) {}

    /// @return the string value for a key, defaulting to defaultValue
    inline std::string get(std::string const& key,
                           std::string const& errorMsg,
                           std::string const& defaultValue) {
        StringMap::const_iterator i = _m.find(key);
        if(i != _m.end()) {
            return i->second;
        } else {
            LOGF_DEBUG("%1%" % errorMsg);
            return defaultValue;
        }
    }

    /// @return the typed value for a key, defaulting to defaultValue
    template <typename T>
    inline T getTyped(std::string const& key,
                      std::string const& errorMsg,
                      T const& defaultValue) {
        static std::string const sentinel(")))))))ConfigMap");
        std::string res = get(key, errorMsg, sentinel);
        if(res != sentinel) {
            return _coerce<T>(res, defaultValue);
        }
        return defaultValue;
    }

    StringMap const& getMap() const { return _m; }

private:
    template<typename T>
    inline T _coerce(std::string const& s, T defaultValue) {
        try {
            return boost::lexical_cast<T>(s);
        } catch (...) {
            return defaultValue;
        }
    }
private:
    StringMap _m;
};

}}} // namespace lsst::qserv:ccontrol

#endif // LSST_QSERV_CCONTROL_CONFIGMAP_H
