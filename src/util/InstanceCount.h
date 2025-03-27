// -*- LSST-C++ -*-

#ifndef LSST_QSERV_UTIL_INSTANCECOUNT_H
#define LSST_QSERV_UTIL_INSTANCECOUNT_H

// System headers
#include <atomic>
#include <map>
#include <mutex>
#include <string>

namespace lsst::qserv::util {

class InstanceCount;

class InstanceCountData {
    InstanceCountData();
    ~InstanceCountData();

    friend InstanceCount;
    friend std::ostream& operator<<(std::ostream& out, InstanceCount const& instanceCount);

private:
    std::map<std::string, int64_t> _instances;  //< Map of instances per class name.
    std::recursive_mutex _mx;                   //< Protects _instances.
    std::atomic<uint64_t> _instanceLogLimiter{0};
};

/// This a utility class to track the number of instances of any class where it is a member.
//
class InstanceCount {
public:
    InstanceCount(std::string const& className);
    InstanceCount(InstanceCount const& other);
    InstanceCount(InstanceCount&& origin);
    ~InstanceCount();

    InstanceCount& operator=(InstanceCount const& o) = default;

    int getCount();  //< Return the number of instances of _className.

    friend std::ostream& operator<<(std::ostream& out, InstanceCount const& instanceCount);

private:
    std::string _className;            ///< Name of instance being counted.
    static InstanceCountData _icData;  ///< Map of counts and other data.

    void _increment(std::string const& source);
};

}  // namespace lsst::qserv::util

#endif  // LSST_QSERV_UTIL_INSTANCECOUNT_H
