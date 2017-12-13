// -*- LSST-C++ -*-

/*
 * LSST Data Management System
 * Copyright 2014-2016 AURA/LSST.
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

/**
  * @file
  *
  * @brief Interface to the Common State System - transient memory-based implementation.
  *
  * @Author Jacek Becla, SLAC
  */

// Class header
#include "css/KvInterfaceImplMem.h"

// System headers
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>

// Third-party headers
#include "boost/algorithm/string.hpp"
#include "boost/algorithm/string/predicate.hpp"
#include "boost/property_tree/ptree.hpp"
#include "boost/property_tree/json_parser.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "css/CssError.h"
#include "util/IterableFormatter.h"

using std::map;
using std::string;
using std::vector;
namespace ptree = boost::property_tree;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.css.KvInterfaceImplMem");

// Normalizes key path, takes user-provided key and converts it into
// acceptable path for storage.
std::string norm_key(std::string const& key) {
    // root key is stored as empty string (if stored at all)
    std::string path(key == "/" ? "" : key);
    return path;
}

}

namespace lsst {
namespace qserv {
namespace css {

/**
  * Initialize the interface.
  *
  * @param mapPath path to the map dumped using ./admin/bin/qserv-admin.py
  *
  * To generate the key/value map, follow this recipe:
  * 1) cleanup everything in CSS. careful, this will wipe out
  *    everyting!
  *    ./admin/bin/qserv-admin.py "drop everything"
  * 2) generate the clean set:
  *    ./admin/bin/qserv-admin.py <  <commands>
  *    (example commands can be found in admin/examples/testMap_generateMap)
  * 3) then copy the generate file to final destination:
  *    mv /tmp/testMap.kvmap <destination>
  */
KvInterfaceImplMem::KvInterfaceImplMem(std::istream& mapStream, bool readOnly) {
    _init(mapStream);
    // read-only should only be set after _init
    _readOnly = readOnly;
}

KvInterfaceImplMem::KvInterfaceImplMem(string const& filename, bool readOnly) {
    std::ifstream f(filename.c_str());
    _init(f);
    // read-only should only be set after _init
    _readOnly = readOnly;
}

KvInterfaceImplMem::~KvInterfaceImplMem() {
}

std::string
KvInterfaceImplMem::create(string const& key, string const& value, bool unique) {
    LOGS(_log, LOG_LVL_DEBUG, "create(" << key << ", " << value << ", unique=" << int(unique));

    if (_readOnly) {
        throw ReadonlyCss();
    }

    std::string path = norm_key(key);
    if (unique) {
        // append unique suffix, in-memory KVI is not meant for large-scale
        // storage, so we can do dumb brute-force loop
        int seq = 0;
        do {
            std::ostringstream str;
            str << std::setfill('0') << std::setw(10) << ++seq;
            path = key + str.str();
        } while (_kvMap.count(path));
    }
    if (exists(path)) {
        throw KeyExistsError(path);
    }
    // create all parents
    string parent = path;
    for (string::size_type p = parent.rfind('/'); p != string::npos; p = parent.rfind('/')) {
        parent = parent.substr(0, p);
        if (_kvMap.find(parent) != _kvMap.end()) break;
        _kvMap.insert(std::make_pair(parent, string()));
    }
    // store the key with value
    _kvMap[path] = value;
    return path;
}

void
KvInterfaceImplMem::set(string const& key, string const& value) {
    // Should always succeed, as long as std::map works.
    LOGS(_log, LOG_LVL_DEBUG, "set(" << key << ", " << value << ")");

    if (_readOnly) {
        throw ReadonlyCss();
    }

    std::string path = norm_key(key);

    // create all parents
    string parent = path;
    for (string::size_type p = parent.rfind('/'); p != string::npos; p = parent.rfind('/')) {
        parent = parent.substr(0, p);
        if (_kvMap.find(parent) != _kvMap.end()) break;
        _kvMap.insert(std::make_pair(parent, string()));
    }

    _kvMap[path] = value;
}

bool
KvInterfaceImplMem::exists(string const& key) {
    std::string path = norm_key(key);
    bool ret = _kvMap.find(path) != _kvMap.end();
    LOGS(_log, LOG_LVL_DEBUG, "exists(" << key << "): " << (ret?"YES":"NO"));
    return ret;
}

std::map<std::string, std::string>
KvInterfaceImplMem::getMany(std::vector<std::string> const& keys) {
    std::map<std::string, std::string> result;
    for (auto& key: keys) {
        std::string path = norm_key(key);
        auto iter = _kvMap.find(path);
        if (iter != _kvMap.end()) {
            result.insert(*iter);
        }
    }
    return result;
}

string
KvInterfaceImplMem::_get(string const& key,
                         string const& defaultValue,
                         bool throwIfKeyNotFound) {
    LOGS(_log, LOG_LVL_DEBUG, "get(" << key << ")");
    std::string path = norm_key(key);
    if ( !exists(path) ) {
        if (throwIfKeyNotFound) {
            throw NoSuchKey(path);
        }
        return defaultValue;
    }
    string s = _kvMap[path];
    LOGS(_log, LOG_LVL_DEBUG, "got: '" << s << "'");
    return s;
}

vector<string>
KvInterfaceImplMem::getChildren(string const& key) {
    LOGS(_log, LOG_LVL_DEBUG, "getChildren(), key: " << key);
    std::string path = norm_key(key);
    if ( ! exists(path) ) {
        throw NoSuchKey(path);
    }
    const string pfx(path + "/");
    vector<string> retV;
    map<string, string>::const_iterator itrM;
    for (itrM=_kvMap.begin() ; itrM!=_kvMap.end() ; itrM++) {
        string fullKey = itrM->first;
        LOGS(_log, LOG_LVL_DEBUG, "fullKey: " << fullKey);
        if (boost::starts_with(fullKey, pfx)) {
            string theChild = fullKey.substr(pfx.length());
            if (!theChild.empty() && (theChild.find("/") == string::npos)) {
                LOGS(_log, LOG_LVL_DEBUG, "child: " << theChild);
                retV.push_back(theChild);
            }
        }
    }
    LOGS(_log, LOG_LVL_DEBUG, "got: " << retV.size() << " children: " << util::printable(retV));
    return retV;
}

std::map<std::string, std::string>
KvInterfaceImplMem::getChildrenValues(std::string const& key) {
    LOGS(_log, LOG_LVL_DEBUG, "getChildrenValues(), key: " << key);
    std::string path = norm_key(key);
    if ( ! exists(path) ) {
        throw NoSuchKey(path);
    }
    const string pfx(path + "/");
    std::map<std::string, std::string> retV;
    for (auto const& pair: _kvMap) {
        auto& fullKey = pair.first;
        LOGS(_log, LOG_LVL_DEBUG, "fullKey: " << fullKey);
        if (boost::starts_with(fullKey, pfx)) {
            string theChild(fullKey, pfx.length());
            if (!theChild.empty() && (theChild.find("/") == string::npos)) {
                LOGS(_log, LOG_LVL_DEBUG, "child: " << theChild);
                retV.insert(std::make_pair(theChild, pair.second));
            }
        }
    }
    LOGS(_log, LOG_LVL_DEBUG, "got: " << retV.size() << " children: " << util::printable(retV));
    return retV;
}

void
KvInterfaceImplMem::deleteKey(string const& key) {
    LOGS(_log, LOG_LVL_DEBUG, "deleteKey(" << key << ")");

    if (_readOnly) {
        throw ReadonlyCss();
    }

    std::string path = norm_key(key);

    auto iter = _kvMap.find(path);
    if (iter == _kvMap.end()) {
        throw NoSuchKey(path);
    }
    LOGS(_log, LOG_LVL_DEBUG, "deleteKey: erasing key " << path);
    _kvMap.erase(iter);
    // delete all children keys, not very efficient but we don't care
    std::string const keyPfx(path + "/");
    for (auto iter = _kvMap.begin(); iter != _kvMap.end(); ) {
        auto const& iterKey = iter->first;
        if (iterKey.size() > keyPfx.size() and iterKey.compare(0, keyPfx.size(), keyPfx) == 0) {
            LOGS(_log, LOG_LVL_DEBUG, "deleteKey: erasing child " << iterKey);
            iter = _kvMap.erase(iter);
        } else {
            ++ iter;
        }
    }
}

std::string KvInterfaceImplMem::dumpKV(std::string const& key) {
    const string pfx(norm_key(key) + "/");
    ptree::ptree tree;
    for (auto& pair: _kvMap) {
        // filter the key, note that root key which is empty string will
        // be filtered out because pfx is never empty
        if (boost::starts_with(pair.first, pfx)) {
            tree.push_back(ptree::ptree::value_type(pair.first, ptree::ptree(pair.second)));
        }
    }

    // format property tree into a string as JSON
    std::ostringstream str;
    ptree::write_json(str, tree);
    return str.str();
}

void KvInterfaceImplMem::_init(std::istream& mapStream) {
    if (mapStream.fail()) {
        throw ConnError();
    }

    ptree::ptree tree;
    try {
        ptree::read_json(mapStream, tree);
    } catch (ptree::json_parser_error const& exc) {
        throw CssError("KvInterfaceImplMem - failed to parse JSON file");
    }

    for (auto&& pair: tree) {
        set(pair.first, pair.second.data());
    }
}

std::shared_ptr<KvInterfaceImplMem>
KvInterfaceImplMem::clone() const {
    std::shared_ptr<KvInterfaceImplMem> newOne = std::make_shared<KvInterfaceImplMem>();
    newOne->_kvMap = _kvMap;
    return newOne;
}

}}} // namespace lsst::qserv::css
