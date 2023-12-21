/*
 * LSST Data Management System
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
#ifndef LSST_QSERV_REPLICA_NAMEDMUTEXREGISTRY_H
#define LSST_QSERV_REPLICA_NAMEDMUTEXREGISTRY_H

// System headers
#include <map>
#include <memory>
#include <string>

// Qserv headers
#include "replica/util/Mutex.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class NamedMutexRegistry represents a collection of named instances
 * of class replica::Mutex. Each instance has a unique name. Instances are
 * created automatically and stored in the registry upon the very first
 * request mentioning a new name.
 * Unused mutex objects would be automatically garbage-collected at each
 * invocation of method NamedMutexRegistry::get().
 * It's used for synchronizing operations over databases, tables, chunks in
 * a context of a multi-threaded application.
 *
 * @note This class has thread-safe implementation to allow calling method
 *   NamedMutexRegistry::get().
 */
class NamedMutexRegistry {
public:
    NamedMutexRegistry() = default;
    NamedMutexRegistry(NamedMutexRegistry const&) = delete;
    NamedMutexRegistry& operator=(NamedMutexRegistry const&) = delete;
    ~NamedMutexRegistry() = default;

    /**
     * @param name The name of a mutex.
     * @return A shared pointer to the mutex.
     * @throw std::invalid_argument If the name is empty.
     */
    std::shared_ptr<replica::Mutex> get(std::string const& name);

    /// @return The current number of entries.
    size_t size() const;

private:
    std::map<std::string, std::shared_ptr<replica::Mutex>> _registry;
    mutable replica::Mutex _registryAccessMtx;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_NAMEDMUTEXREGISTRY_H
