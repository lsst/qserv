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

/**
  * @file KvInterfaceImplZoo.cc
  *
  * @brief Interface to the Common State System - zookeeper-based implementation.
  *
  * @Author Jacek Becla, SLAC
  */

/*
 * Based on:
 * http://zookeeper.apache.org/doc/r3.3.4/zookeeperProgrammers.html#ZooKeeper+C+client+API
 *
 * To do:
 *  - perhaps switch to async (seems to be recommended by zookeeper)
 */


// standard library imports
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string.h> // for memset

// local imports
#include "KvInterfaceImplZoo.h"
#include "CssException.h"
#include "log/Logger.h"

using std::endl;
using std::exception;
using std::ostringstream;
using std::string;
using std::vector;

namespace lsst {
namespace qserv {
namespace css {


/**
 * Initialize the interface.
 *
 * @param connInfo connection information
 */
KvInterfaceImplZoo::KvInterfaceImplZoo(string const& connInfo) {
    zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
    _zh = zookeeper_init(connInfo.c_str(), 0, 10000, 0, 0, 0);
    if ( !_zh ) {
        throw std::runtime_error("Failed to connect");
    }
}

KvInterfaceImplZoo::~KvInterfaceImplZoo() {
    try {    
        int rc = zookeeper_close(_zh);
        if ( rc != ZOK ) {
            LOGGER_ERR << "*** ~KvInterfaceImplZoo - zookeeper error " << rc
                       << "when closing connection" << endl;
        }
    } catch (...) {
        LOGGER_ERR << "*** ~KvInterfaceImplZoo - zookeeper exception "
                   << "when closing connection" << endl;
    }
}

void
KvInterfaceImplZoo::create(string const& key, string const& value) {
    LOGGER_INF << "*** KvInterfaceImplZoo::create(), " << key << " --> " 
               << value << endl;
    char buffer[512];
    int rc = zoo_create(_zh, key.c_str(), value.c_str(), value.length(), 
                        &ZOO_OPEN_ACL_UNSAFE, 0, buffer, sizeof(buffer)-1);
    if (rc!=ZOK) {
        _throwZooFailure(rc, "create");
    }
}

bool
KvInterfaceImplZoo::exists(string const& key) {
    LOGGER_INF << "*** KvInterfaceImplZoo::exist(), key: " << key << endl;
    struct Stat stat;
    memset(&stat, 0, sizeof(Stat));
    int rc = zoo_exists(_zh, key.c_str(), 0,  &stat);
    if (rc==ZOK) {
        return true;
    }
    if (rc==ZNONODE) {
        return false;
    }
    _throwZooFailure(rc, "exists", key);
    return false;
}

string
KvInterfaceImplZoo::get(string const& key) {
    LOGGER_INF << "*** KvInterfaceImplZoo::get(), key: " << key << endl;
    char buffer[512];
    int bufLen = static_cast<int>(sizeof(buffer));
    memset(buffer, 0, bufLen);
    struct Stat stat;
    memset(&stat, 0, sizeof(Stat));
    int rc = zoo_get(_zh, key.c_str(), 0, buffer, &bufLen, &stat);
    if (rc=ZOK) {
        _throwZooFailure(rc, "get", key);
    }
    LOGGER_INF << "*** got: '" << buffer << "'" << endl;
    return string(buffer);
}

vector<string> 
KvInterfaceImplZoo::getChildren(string const& key) {
    LOGGER_INF << "*** KvInterfaceImplZoo::getChildren(), key: " << key << endl;
    struct String_vector strings;
    memset(&strings, 0, sizeof(strings));
    int rc = zoo_get_children(_zh, key.c_str(), 0, &strings);
    if (rc!=ZOK) {
        _throwZooFailure(rc, "getChildren", key);
    }
    LOGGER_INF << "got " << strings.count << " children" << endl;
    vector<string> v;
    try {    
        int i;
        for (i=0 ; i<strings.count ; i++) {
            LOGGER_INF << "   " << i+1 << ": " << strings.data[i] << endl;
            v.push_back(strings.data[i]);
        }
        deallocate_String_vector(&strings);
    } catch (const std::bad_alloc& ba) {
        deallocate_String_vector(&strings);
    }
    return v;
}

void
KvInterfaceImplZoo::deleteKey(string const& key) {
    LOGGER_INF << "*** KvInterfaceImplZoo::deleteKey, key: " << key << endl;
    int rc = zoo_delete(_zh, key.c_str(), -1);
    if (rc!=ZOK) {
        _throwZooFailure(rc, "deleteKey", key);
    }   
}

/**
  * @param rc       return code returned by zookeeper
  * @param fName    function name where the error happened
  * @param extraMsg optional extra message to include in the error message
  */
void
KvInterfaceImplZoo::_throwZooFailure(int rc, string const& fName, 
                                     string const& extraMsg) {
    string ffName = "*** css::KvInterfaceImplZoo::" + fName + "(). ";
    if (rc==ZNONODE) {
        LOGGER_INF << ffName << "Key '" << extraMsg << "' does not exist." << endl;
        throw CssException_KeyDoesNotExist(extraMsg);
    }
    if (rc==ZCONNECTIONLOSS) {
        LOGGER_INF << ffName << "Can't connect to zookeeper." << endl;
        throw CssException_ConnFailure();
    }
    if (rc==ZNOAUTH) {
        LOGGER_INF << ffName << "Zookeeper authorization failure." << endl;
        throw CssException_AuthFailure();
    }
    ostringstream s;
    s << ffName << "Zookeeper error #" << rc << ".";
    if (extraMsg != "") {
        s << " (" << extraMsg << ")";
    }
    LOGGER_INF << s.str() << endl;
    throw CssException_InternalRunTimeError(s.str());
}

}}} // namespace lsst::qserv::css
