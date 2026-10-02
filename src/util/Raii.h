// -*- LSST-C++ -*-

#ifndef LSST_QSERV_UTIL_RAII_H
#define LSST_QSERV_UTIL_RAII_H

// System headers
#include <atomic>
#include <memory>
#include <string>
#include <type_traits>

namespace lsst::qserv::util {

/** This class uses RAII to count the number of various things.
 * `create()` is used to make an instance of the counter, and
 * `createRaii()` is used to make an instance of the RAII object that
 * increases the counter when created and decreases the counter when
 * destroyed.
 */
template <typename T>
requires std::is_trivially_copyable_v<T>
class RaiiCounter : public std::enable_shared_from_this<RaiiCounter<T>> {
public:
    using Ptr = std::shared_ptr<RaiiCounter<T>>;


    RaiiCounter(RaiiCounter const&) = delete;
    virtual ~RaiiCounter() = default;

    /** Return a shared pointer to a new RaiiCounter object.
     * @param name - optional name for the counter.
     * @param val - optional initial value for the counter.
     */
    static Ptr create(std::string const& name, T val) { return Ptr(new RaiiCounter(name, val)); }
    static Ptr create() { return Ptr(new RaiiCounter()); }
    static Ptr create(std::string const& name) { return Ptr(new RaiiCounter(name)); }
    static Ptr create(T val) { return Ptr(new RaiiCounter(val)); }

    T getCount() const { return _count; }
    std::string const& getName() const { return _name; }

    class Raii {
    public:
        ~Raii() { --(_target->_count); }
        Raii() = delete;
        Raii(Raii const&) = delete;
        Raii& operator=(Raii const&) = delete;

        friend class RaiiCounter<T>;
    private:
        Raii(RaiiCounter::Ptr target) : _target(target) { ++(_target->_count); }
        RaiiCounter::Ptr _target;
    };

    using RaiiPtr = std::shared_ptr<RaiiCounter<T>::Raii>;

    /** Return a shared pointer to a new Raii object increase _counter when created
     * and decrease _counter when destroyed.
     */
    RaiiPtr createRaii() { return RaiiPtr(new Raii(getRaiiCounter())); }

    Ptr getRaiiCounter() { return this->shared_from_this(); }

    RaiiCounter() = default;
    explicit RaiiCounter(std::string const& name) : _name(name) {}
    explicit RaiiCounter(T initialCount) : _count(initialCount) {}
    explicit RaiiCounter(std::string const& name, T initialCount) : _name(name), _count(initialCount) {}

protected:
    std::string const _name { "none" };
    std::atomic<T> _count{0};
};

}  // namespace lsst::qserv::util

#endif  // LSST_QSERV_UTIL_RAII_H
