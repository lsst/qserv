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
#ifndef LSST_QSERV_REPLICA_CHUNKMAP_H
#define LSST_QSERV_REPLICA_CHUNKMAP_H

// System headers
#include <cstdlib>
#include <string>
#include <map>
#include <memory>
#include <mutex>

// Third party headers
#include "nlohmann/json.hpp"

// Forward declarations
namespace lsst::qserv::replica {
class ServiceProvider;
}  // namespace lsst::qserv::replica

// This header declarations

namespace lsst::qserv::replica {

class ChunkMap : public std::enable_shared_from_this<ChunkMap> {
public:
    /**
     * [worker] -> [database] -> [baseTable] -> [chunk] -> size
     *
     * The map represents the information on the replica disposition across Qserv workers.
     * The information is obtained from the persistent state of the Replication system on each
     * run of the task. The maps gets updated only if the new map is different from the current one.
     */
    using Chunks =
            std::map<std::string,
                     std::map<std::string, std::map<std::string, std::map<std::uint32_t, std::uint64_t>>>>;

    /**
     * Create a new instance of the ChunkMap class.
     * @param serviceProvider the service provider instance.
     * @return a shared pointer to the newly created ChunkMap instance.
     */
    static std::shared_ptr<ChunkMap> create(std::shared_ptr<ServiceProvider> const& serviceProvider);

    ChunkMap() = delete;
    ChunkMap(ChunkMap const&) = delete;
    ChunkMap& operator=(ChunkMap const&) = delete;
    ~ChunkMap() = default;

    /**
     * Update the chunk map with the latest information if needed in the Czar database and in
     * the transient state of this class.
     * @return true if the map was updated, false otherwise.
     */
    bool update();

    /**
     * Get the current chunk map as a JSON object. The object will have the same schema as
     * the internal representation of the chunk map (see type Chunks defined above).
     * @return a JSON representation of the chunk map.
     */
    nlohmann::json toJson() const;

private:
    ChunkMap(std::shared_ptr<ServiceProvider> const& serviceProvider);

    /**
     * Update the transient state of the chunk map with the latest information.
     * @param lock A lock guard to ensure thread-safe access to the chunk map.
     * @return true if the transient state was updated, false otherwise.
     */
    bool _updateTransientState(std::lock_guard<std::mutex> const& lock);

    // Parameters of the object

    std::shared_ptr<ServiceProvider> const _serviceProvider;  ///< The service provider instance.

    // The internal representation of the current state of the chunk map.

    std::shared_ptr<Chunks> _chunks;  ///< The current state.
    mutable std::mutex _mtx;          ///< Mutex to protect access to the chunk map.
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_CHUNKMAP_H
