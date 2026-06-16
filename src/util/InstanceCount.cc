// -*- LSST-C++ -*-

// Class header
#include "util/InstanceCount.h"

// System Headers
#include <iostream>

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {  // File-scope helpers

LOG_LOGGER _log = LOG_GET("lsst.qserv.util.InstanceCount");

}  // namespace

namespace lsst::qserv::util {

InstanceCount::InstanceCountData InstanceCount::_icData;

InstanceCount::InstanceCountData::InstanceCountData() {
    std::cout << "InstanceCountData " << " mx=" << (void*)(&_mx) << " _inst=" << (void*)(&_instances)
              << " t=" << (void*)(this) << endl;
}

InstanceCount::InstanceCountData::~InstanceCountData() {
    cout << "~InstanceCountData " << " mx=" << (void*)(&_mx) << " _inst=" << (void*)(&_instances)
         << " t=" << (void*)(this) << endl;
}

InstanceCount::InstanceCount(std::string const& className) : _className{className} { _increment("con"); }

InstanceCount::InstanceCount(InstanceCount const& other) : _className{other._className} { _increment("cpy"); }

InstanceCount::InstanceCount(InstanceCount&& origin) : _className{origin._className} { _increment("mov"); }

void InstanceCount::_increment(std::string const& source) {
    std::lock_guard lg(_icData._mx);
    static std::atomic<bool> first = true;
    static InstanceCountData* icD = nullptr;
    if (first.exchange(false) == true) {
        icD = &_icData;
        LOGS(_log, LOG_LVL_DEBUG, "InstanceCount::_increment first icd changed to " << (void*)icD);
    } else {
        if (icD != &_icData) {
            LOGS(_log, LOG_LVL_ERROR,
                 "InstanceCount::_increment icd changed to " << (void*)&_icData << " from " << (void*)icD);
        }
    }
    std::pair<std::string const, int> entry(_className, 0);
    auto ret = _icData._instances.insert(entry);
    auto iter = ret.first;
    iter->second += 1;
    LOGS(_log, LOG_LVL_TRACE, "InstanceCount " << source << " " << iter->first << "=" << iter->second);
    if ((++(_icData._instanceLogLimiter)) % 10000 == 0) {
        LOGS(_log, LOG_LVL_DEBUG, "InstanceCount brief " << *this << " icD=" << (void*)(&_icData));
    }
}

InstanceCount::~InstanceCount() {
    std::lock_guard lg(_icData._mx);
    static std::atomic<bool> first = true;
    static InstanceCountData* icD = nullptr;
    if (first.exchange(false) == true) {
        icD = &_icData;
        LOGS(_log, LOG_LVL_DEBUG, "~InstanceCount first icd changed to " << (void*)icD);
    } else {
        if (icD != &_icData) {
            LOGS(_log, LOG_LVL_ERROR,
                 "~InstanceCount icd changed to " << (void*)&_icData << " from " << (void*)icD);
        }
    }
    auto iter = _icData._instances.find(_className);
    if (iter != _icData._instances.end()) {
        iter->second -= 1;
        LOGS(_log, LOG_LVL_TRACE, "~InstanceCount " << iter->first << "=" << iter->second << " : " << *this);
        int sec = iter->second;
        if (sec == 0 || (sec <= 100000 && sec % 1000 == 0) || (sec > 100000 && sec % 100000 == 0)) {
            LOGS(_log, LOG_LVL_DEBUG,
                 "~InstanceCount " << iter->first << "=" << iter->second << " : " << *this
                                   << " icD=" << (void*)(&_icData));
        }
        if (sec == 0) {
            _icData._instances.erase(iter);
        }
    } else {
        LOGS(_log, LOG_LVL_ERROR,
             "~InstanceCount " << _className << " was not found! : " << *this
                               << " icD=" << (void*)(&_icData));
    }
}

int InstanceCount::getCount() {
    std::lock_guard lg(_icData._mx);
    auto iter = _icData._instances.find(_className);
    if (iter == _icData._instances.end()) {
        return 0;
    }
    return iter->second;
}

std::ostream& operator<<(std::ostream& os, InstanceCount const& instanceCount) {
    std::lock_guard lg(instanceCount._icData._mx);
    for (auto const& entry : instanceCount._icData._instances) {
        if (entry.second != 0) {
            os << entry.first << "=" << entry.second << " ";
        }
    }
    return os;
}

}  // namespace lsst::qserv::util
