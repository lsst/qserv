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
  * @file
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

#include "css/KvInterfaceImplZoo.h"

// System headers
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string.h> // for memset

// Local headers
#include "css/CssError.h"
#include "log/Logger.h"

using std::endl;
using std::ostringstream;
using std::string;
using std::vector;


namespace {
    // the map stores information about connection to zoo (for each thread)
    std::map<boost::thread::id, bool> isConnectedMap;
    boost::mutex mapMutex;

    bool
    checkIsConnected(boost::thread::id tId) {
        boost::mutex::scoped_lock scoped_lock(mapMutex);
        return isConnectedMap[tId];
    }

    void addToMapIfNeeded(boost::thread::id tId) {
        boost::mutex::scoped_lock scoped_lock(mapMutex);
        if ( isConnectedMap.count(tId) == 0 ) {
            LOGGER_INF << "tid=" << tId << " adding to map" << endl;
            isConnectedMap[tId] = 0;
        }
    }

    static void
    connectionWatcher(zhandle_t *, int type, int state,
                      const char *path, void*v) {
        boost::mutex::scoped_lock scoped_lock(mapMutex);
        bool* isConnected = static_cast<bool*>(v);
        *isConnected = (state==ZOO_CONNECTED_STATE);
    }

    class Buffer {
    public:
        // reasonable defaults: size=64, incrLimit=5: 64, 1K, 16K, 256K, 4M, 64M
        Buffer(unsigned int size, int incrLimit)
            : _size(size),
              _incrCount(0),
              _incrLimit(incrLimit) {
            allocateBuffer();
        }
        ~Buffer() { deallocateBuffer(); }
        char* data() const { return _data; }
        std::string const dataAsString() const { return string(_data); }
        int size() const { return _size; }

        void incrSize(std::string const& key) {
            if (++_incrCount > _incrLimit) {
                std::stringstream os;
                os << _size;
                throw lsst::qserv::css::BadAllocError(key, os.str());
            }
            std::cout << "Increasing size: " << _size << "-->" << _size*16 << endl;
            _size *= 16;
            deallocateBuffer();
            allocateBuffer();
        }

    private:
        void allocateBuffer() {
            assert(_size>0 and _size<1024*1024*1024);
            _data = new char[_size];
            memset(_data, 0, _size);
            std::cout << "allocated " << _size << endl;
        }
        void deallocateBuffer() {
            delete [] _data;
            _data = 0;
        }
        // private members
        char* _data;
        int _size;
        int _incrCount;
        int _incrLimit;
    };
} // annonymous namespace


namespace lsst {
namespace qserv {
namespace css {

/**
 * Initialize the interface.
 *
 * @param connInfo      connection information
 * @param timeout_msec  connection timeout in msec
 */
KvInterfaceImplZoo::KvInterfaceImplZoo(string const& connInfo, int timeout_msec)
    : _zh(0),
      _connInfo(connInfo),
      _timeout(timeout_msec) {
    zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
    _doConnect();
}

KvInterfaceImplZoo::~KvInterfaceImplZoo() {
    _disconnect();
}

void
KvInterfaceImplZoo::create(string const& key, string const& value) {
    LOGGER_INF << "*** KvInterfaceImplZoo::create(), " << key << " --> "
               << value << endl;
    int rc=ZINVALIDSTATE, nAttempts=0;
    while (nAttempts++<2) {
        rc = zoo_create(_zh, key.data(), value.data(), value.size(),
                        &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
        if (rc==ZOK) {
            return;
        }
        LOGGER_WRN << "zoo_create failed (err:" << rc << "), "
                   << "attempting to reconnect" << endl;
        _doConnect();
    }
    _throwZooFailure(rc, "create", key);
}

bool
KvInterfaceImplZoo::exists(string const& key) {
    LOGGER_INF << "*** KvInterfaceImplZoo::exist(), key: " << key << endl;
    struct Stat stat;
    int rc=ZINVALIDSTATE, nAttempts=0;
    while (nAttempts++<2) {
        memset(&stat, 0, sizeof(Stat));
        rc = zoo_exists(_zh, key.data(), 0,  &stat);
        if (rc==ZOK) {
            return true;
        } else if (rc==ZNONODE) {
            return false;
        }
        LOGGER_WRN << "zoo_exists failed (err:" << rc << "), "
                   << "attempting to reconnect" << endl;
        _doConnect();
    }
    _throwZooFailure(rc, "exists", key);
    return false;
}

string
KvInterfaceImplZoo::_get(string const& key,
                         string const& defaultValue,
                         bool throwIfKeyNotFound) {
    LOGGER_INF << "*** KvInterfaceImplZoo::get(), key: " << key << endl;
    Buffer buffer(64, 5);
    struct Stat stat;
    int rc=ZINVALIDSTATE, nAttempts=0;
    while (nAttempts++<2) {
        memset(&stat, 0, sizeof(Stat));
        int rsvLen = buffer.size();
        rc = zoo_get(_zh, key.data(), 0, buffer.data(), &rsvLen, &stat);
        LOGGER_INF << "Got rc: " << rc << ", size: " << rsvLen << endl;
        if (rc==ZOK) {
            if (rsvLen >= buffer.size()) {
                buffer.incrSize(key);
                nAttempts--;
                continue;
            }
            LOGGER_INF << "Got: '" << buffer.data() << "'" << endl;
            return buffer.dataAsString();
        }
        if (rc==ZNONODE) {
            if (throwIfKeyNotFound) {
                throw NoSuchKey(key);
            }
            LOGGER_INF << "Returning default value: '"
                       << defaultValue << "'" << endl;
            return defaultValue;
        }
        LOGGER_WRN << "zoo_get failed (err:" << rc << "), for key: "
                   << key << ", attempting to reconnect" << endl;
        _doConnect();
    }
    _throwZooFailure(rc, "get", key);
    return string();
}

vector<string>
KvInterfaceImplZoo::getChildren(string const& key) {
    LOGGER_INF << "*** KvInterfaceImplZoo::getChildren(), key: " << key << endl;
    struct String_vector strings;
    vector<string> v;
    int rc=ZINVALIDSTATE, nAttempts=0;
    while (nAttempts++<2) {
        memset(&strings, 0, sizeof(strings));
        rc = zoo_get_children(_zh, key.data(), 0, &strings);
        if (rc==ZOK) {
            LOGGER_INF << "got " << strings.count << " children" << endl;
            int i;
            for (i=0 ; i<strings.count ; i++) {
                LOGGER_INF << "   " << i+1 << ": " << strings.data[i] << endl;
                v.push_back(strings.data[i]);
            }
            return v;
        }
        LOGGER_WRN << "zoo_get_children failed (err:" << rc << "), "
                   << "attempting to reconnect" << endl;
        _doConnect();
    }
    _throwZooFailure(rc, "getChildren", key);
    return v;
}

void
KvInterfaceImplZoo::deleteKey(string const& key) {
    LOGGER_INF << "*** KvInterfaceImplZoo::deleteKey, key: " << key << endl;
    int rc=ZINVALIDSTATE, nAttempts=0;
    while (nAttempts++<2) {
        rc = zoo_delete(_zh, key.data(), -1);
        if (rc==ZOK) {
            return;
        }
        LOGGER_WRN << "zoo_delete failed (err:" << rc << "), "
                   << "attempting to reconnect" << endl;
        _doConnect();
    }
    _throwZooFailure(rc, "deleteKey", key);
}

void
KvInterfaceImplZoo::_doConnect() {
    LOGGER_INF << "Connecting to zookeeper. " << _connInfo << ", " << _timeout
               << endl;
    if ( !_zh ) {
        _disconnect();
    }
    boost::thread::id tId = boost::this_thread::get_id();
    addToMapIfNeeded(tId);
    _zh = zookeeper_init(_connInfo.data(), connectionWatcher, _timeout,
                         0, &isConnectedMap[tId], 0);

    // wait up to _timeout time in short increments
    int waitT = 10;                  // wait 10 microsec at a time
    int reptN = 1000*_timeout/waitT; // 1000x because _timeout in mili, need micro
    while (reptN-- > 0) {
        if (checkIsConnected(tId)) {
            LOGGER_INF << "Connected" << endl;
            return;
        }
        usleep(waitT);
    }
    if ( !_zh ) {
        throw ConnError("Invalid handle");
    }
    if (zoo_state(_zh) != ZOO_CONNECTED_STATE) {
        ostringstream s;
        s << "Invalid state: " << zoo_state(_zh);
        throw ConnError(s.str());
    }
}

void
KvInterfaceImplZoo::_disconnect() {
    if (!_zh) {
        return;
    }
    LOGGER_INF << "Disconnecting from zookeeper." << endl;
    int rc = zookeeper_close(_zh);
    if ( rc != ZOK ) {
        LOGGER_ERR << "Zookeeper error " << rc << "when closing connection" << endl;
    }
    _zh = 0;
}

/**
  * @param rc       return code returned by zookeeper
  * @param fName    function name where the error happened
  * @param extraMsg optional extra message to include in the error message
  */
void
KvInterfaceImplZoo::_throwZooFailure(int rc, string const& fName,
                                     string const& key) {
    string ffName = "*** css::KvInterfaceImplZoo::" + fName + "(). ";
    if (rc==ZNONODE) {
        LOGGER_INF << ffName << "Key '" << key << "' does not exist." << endl;
        throw (key);
    } else if (rc==ZNODEEXISTS) {
        LOGGER_INF << ffName << "Key '" << key << "' already exists." << endl;
        throw KeyExistsError(key);
    } else if (rc==ZCONNECTIONLOSS) {
        LOGGER_INF << ffName << "Can't connect to zookeeper." << endl;
        throw ConnError();
    } else if (rc==ZNOAUTH) {
        LOGGER_INF << ffName << "Zookeeper authorization failure." << endl;
        throw AuthError();
    } else if (rc==ZBADARGUMENTS) {
        LOGGER_INF << ffName << "Invalid key passed to zookeeper." << endl;
        throw NoSuchKey(key);
    }
    ostringstream s;
    s << ffName << "Zookeeper error #" << rc << ". Key: '" << key << "'.";
    LOGGER_INF << s.str() << endl;
    throw CssError(s.str());
}

}}} // namespace lsst::qserv::css
