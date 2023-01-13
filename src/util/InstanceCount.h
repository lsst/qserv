// -*- LSST-C++ -*-

#ifndef LSST_QSERV_UTIL_INSTANCECOUNT_H
#define LSST_QSERV_UTIL_INSTANCECOUNT_H

// System headers
#include <map>
#include <memory>
#include <mutex>
#include <string>

namespace lsst::qserv::util {

/// This a utility class to track the number of instances of any class where it is a member.
/// Alternatively, it can be used to track functions or code blocks by giving the
/// InstanceCount a unique identifier in the block of code to track.
/// It can also be used to see how many threads are  waiting on a mutex.
/// InstanceCount is a flexible debugging tool, but it is very noisy in the log.
/// Once a problem is solved, the local instances of InstanceCount should be removed.
//
class InstanceCount {
public:
    using Ptr = std::shared_ptr<InstanceCount>;
    InstanceCount(std::string const& className);
    InstanceCount(InstanceCount const& other);
    InstanceCount(InstanceCount&& origin);
    ~InstanceCount();

    InstanceCount& operator=(InstanceCount const& o) = default;

    int getCount();  //< Return the number of instances of _className.

    friend std::ostream& operator<<(std::ostream& out, InstanceCount const& instanceCount);

private:
    std::string _className;                        //< Names of the of which this is a member.
    static std::map<std::string, int> _instances;  //< Map of instances per class name.
    static std::recursive_mutex _mx;               //< Protects _instances.

    void _increment(std::string const& source);
};

}  // namespace lsst::qserv::util

#endif  // LSST_QSERV_UTIL_INSTANCECOUNT_H
