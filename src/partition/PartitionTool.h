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
#ifndef LSST_PARTITION_PARTITIONTOOL_H
#define LSST_PARTITION_PARTITIONTOOL_H

// System headers
#include <memory>

// Third party headers
#include "nlohmann/json.hpp"

// Forward declarations
namespace lsst::partition {
class ChunkIndex;
class ConfigStore;
}  // namespace lsst::partition

// This header declarations
namespace lsst::partition {

/**
 * Class PartitionTool is he partitioner for tables which have a single
 * partitioning position.
 */
class PartitionTool {
public:
    /// Forward declaration for the customized worker class. The class is defined in
    /// a scope of PartitionTool to avoid the name clash with similar classes of other tools.
    class Worker;

    /**
     * Construct and run the partition tool with the specified parameters and/or
     * command-line arguments.
     *
     * @note Both 'params' and command-line arguments are optional, but at least one of them
     *   must be provided. Upon sucessful completion, the generated configuration and chunk index
     *   are available via the 'config' and 'chunkIndex' public members.
     *
     * @param params (optional) a JSON object with parameters for the partition tool
     * @param argc (optional) the number of command-line arguments
     * @param argv (optional) the command-line arguments
     * @throws std::invalid_argument if neither 'params' nor command-line arguments are provided
     * @throws std::runtime_error for any errors encounter during the processing
     * @throws ExitOnHelp if help information was requested
     */
    explicit PartitionTool(nlohmann::json const& params = nlohmann::json::object(), int argc = 0,
                           char const* const* argv = nullptr);
    PartitionTool() = delete;
    PartitionTool(PartitionTool const&) = delete;
    PartitionTool& operator=(PartitionTool const&) = delete;
    ~PartitionTool() = default;

    std::shared_ptr<ConfigStore> config = nullptr;     ///< The configuration store
    std::shared_ptr<ChunkIndex> chunkIndex = nullptr;  ///< The generated chunk index
};

}  // namespace lsst::partition

#endif  // LSST_PARTITION_PARTITIONTOOL_H
