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
#ifndef LSST_QSERV_REPLICA_SQLGETINDEXESJOB_H
#define LSST_QSERV_REPLICA_SQLGETINDEXESJOB_H

// System headers
#include <cstdint>
#include <functional>
#include <list>
#include <map>
#include <string>
#include <tuple>
#include <vector>

// Qserv headers
#include "replica/jobs/SqlJob.h"
#include "replica/util/Common.h"

// Third party headers
#include "nlohmann/json.hpp"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class SqlIndexes represents postprocessed results of the index retrieval jobs (class SqlGetIndexesJob).
 * The object could be produced only in case of the successful completion of the jobs.
 */
class SqlIndexes {
public:
    // Parameters defining a context of the job.
    std::string database;
    std::string table;
    bool overlap;

    /**
     * The class Column describes columns of an index.
     */
    class Column {
    public:
        std::string name;          ///< the column name
        unsigned int seq = 0;      ///< the column sequence number in the index, starting with 1
        unsigned int subPart = 0;  ///< the number of indexed characters if partially indexed, or 0 otherwise

        /// The enum Collation describes how the column is sorted in the index.
        enum class Collation : int {
            ASC = 0,    ///< for ascending order
            DESC,       ///< for descending order
            NOT_SORTED  ///< for undetermined order
        };
        Collation collation = Collation::NOT_SORTED;  ///< the collation of the columns

        static std::string collation2string(Collation collation);
    };

    /**
     * The class Index describes the index itself.
     */
    class Index {
    public:
        // The index definition
        std::string name;           ///< the key name
        bool unique = false;        ///< index uniqueness
        std::string type;           ///< index type
        std::string comment;        ///< the optional comment
        std::list<Column> columns;  ///< definitions for contributing columns

        /// Compare index definitions of the current and other index.
        /// @note The 'comment' attribute of the definition will be excluded from the comparison.
        /// @param other Other index to compare the current one with.
        /// @return 'true' if the definitions are the same.
        bool equalIndexDef(Index const& other) const;

        /// The enum Status describes the status of the index
        enum class Status : int {
            COMPLETE = 0,  ///< all replicas of chunks/tables have the same index
            INCOMPLETE,    ///< the index is missing in some replicas
            INCONSISTENT   ///< index definition is different at some replicas
        };
        Status status = Status::INCONSISTENT;  ///< the status of the index

        static std::string status2string(Status status);

        size_t numReplicasTotal = 0;  ///< the total number of replicas probed by the job
        size_t numReplicas = 0;       ///< the number of replicas where the index was found
    };
    std::list<Index> indexes;  ///< indexes defined on the table

    /// @return  JSON representation of the object
    nlohmann::json toJson() const;
};

inline bool operator==(SqlIndexes::Column const& lhs, SqlIndexes::Column const& rhs) {
    return (lhs.name == rhs.name) && (lhs.seq == rhs.seq) && (lhs.subPart == rhs.subPart) &&
           (lhs.collation == rhs.collation);
}

/**
 * Class SqlGetIndexesJob represents a tool which will broadcast batches of
 * the table index retrieval requests to workers.
 *
 * @note the meaning of the 'table' depends on a kind of a table. If this is
 * a regular table then tables with exact names will be searched at all workers.
 * For the partitioned tables the operation will include both the prototype
 * tables (tables at exactly the specified name existing at all workers) and
 * the corresponding chunk tables for all chunks associated with the corresponding
 * workers, as well as so called "dummy chunk" tables.
 */
class SqlGetIndexesJob : public SqlJob {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlGetIndexesJob> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param database The name of a database where the tables are residing.
     * @param table The name of the base table to be affected by the operation.
     * @param overlap The flag (applies to the partitioned tables only)
     *   indicating which kind of the partitioned tables to be affected by
     *   the operation. If the flag is set to 'true' then only the overlap tables
     *   will be involved into the operation. Otherwise, only the chunk tables will
     *   be affected.
     * @param allWorkers The flag which if set to 'true' will engage all known
     *   workers regardless of their status. If the flag is set to 'false' then
     *   only 'ENABLED' workers which are not in the 'READ-ONLY' state will be
     *   involved into the operation.
     * @param controller This is needed launching requests and accessing the Configuration.
     * @param parentJobId An identifier of a parent job.
     * @param onFinish A callback function to be called upon a completion of the job.
     * @param priority The priority level of the job.
     * @return A pointer to the created object.
     */
    static Ptr create(std::string const& database, std::string const& table, bool overlap, bool allWorkers,
                      Controller::Ptr const& controller, std::string const& parentJobId,
                      CallbackType const& onFinish, int priority);

    SqlGetIndexesJob() = delete;
    SqlGetIndexesJob(SqlGetIndexesJob const&) = delete;
    SqlGetIndexesJob& operator=(SqlGetIndexesJob const&) = delete;

    ~SqlGetIndexesJob() final = default;

    std::string const& database() const { return _database; }
    std::string const& table() const { return _table; }
    bool overlap() const { return _overlap; }
    std::list<std::pair<std::string, std::string>> extendedPersistentState() const final;

    /**
     * Return info on the indexes.
     * @note This method is meant to be used only for succesfully completed jobs.
     * @return definitions of indexes
     * @throw std::logic_error If the method is called wguile the job is still running,
     *  or if it's failed.
     */
    SqlIndexes indexes() const;

protected:
    void notify(replica::Lock const& lock) final;
    std::list<SqlRequest::Ptr> launchRequests(replica::Lock const& lock, std::string const& worker,
                                              size_t maxRequestsPerWorker) final;

private:
    SqlGetIndexesJob(std::string const& database, std::string const& table, bool overlap, bool allWorkers,
                     Controller::Ptr const& controller, std::string const& parentJobId,
                     CallbackType const& onFinish, int priority);

    // Input parameters
    std::string const _database;
    std::string const _table;
    bool const _overlap;
    CallbackType _onFinish;  /// @note is reset when the job finishes

    /// A collection of tables to be processed by workers. The collection
    /// is also needed to mark those for which request has been sent.
    /// The registry prevents duplicate requests because exactly one
    /// such request is permitted to be sent to each worker.
    std::map<std::string, std::vector<std::string>> _workers2tables;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SQLGETINDEXESJOB_H
