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
#ifndef LSST_PARTITION_PARTITIONMATCHESTOOL_H
#define LSST_PARTITION_PARTITIONMATCHESTOOL_H

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
 * Class PartitionMatchesTool is the partitioner for match tables.
 *
 * A match table M contains foreign keys into a pair of identically partitioned
 * positional tables U and V (containing e.g. objects and reference objects).
 * A match in M is assigned to a chunk C if either of the positions pointed
 * to is assigned to C. If no positions in a match are separated by more than the
 * partitioning overlap radius, then a 3-way equi-join between U, M and V can
 * be decomposed into the union of 3-way joins over the set of sub-chunks:
 *
 *     (
 *         SELECT ...
 *         FROM Uᵢ INNER JOIN Mᵨ ON (Uᵢ.pk = Mᵨ.fkᵤ)
 *                 INNER JOIN Vᵢ ON (Mᵨ.fkᵥ = Vᵢ.pk)
 *         WHERE ...
 *     ) UNION ALL (
 *         SELECT ...
 *         FROM Uᵢ INNER JOIN Mᵨ ON (Uᵢ.pk = Mᵨ.fkᵤ)
 *                 INNER JOIN OVᵢ ON (Mᵨ.fkᵥ = OVᵢ.pk)
 *         WHERE ...
 *     )
 *
 * Here, Uᵢ and Vᵢ contain the rows of U and V in sub-chunk i of chunk p,
 * Mᵨ contains the rows of M in chunk p, and OVᵢ is the subset of V \ Vᵢ
 * within the overlap radius of Vᵢ.
 */
class PartitionMatchesTool {
public:
    /// Forward declaration for the customized worker class. The class is defined in
    /// a scope of PartitionMatchesTool to avoid the name clash with similar classes of other tools.
    class Worker;

    /**
     * Construct and run the partition matches tool with the specified parameters and/or
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
    explicit PartitionMatchesTool(nlohmann::json const& params = nlohmann::json::object(), int argc = 0,
                                  char const* const* argv = nullptr);
    PartitionMatchesTool() = delete;
    PartitionMatchesTool(PartitionMatchesTool const&) = delete;
    PartitionMatchesTool& operator=(PartitionMatchesTool const&) = delete;
    ~PartitionMatchesTool() = default;

    std::shared_ptr<ConfigStore> config = nullptr;     ///< The configuration store
    std::shared_ptr<ChunkIndex> chunkIndex = nullptr;  ///< The generated chunk index
};

}  // namespace lsst::partition

#endif  // LSST_PARTITION_PARTITIONMATCHESTOOL_H
