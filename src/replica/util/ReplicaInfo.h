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
#ifndef LSST_QSERV_REPLICA_REPLICAINFO_H
#define LSST_QSERV_REPLICA_REPLICAINFO_H

/**
 * This header declares class ReplicaInfo and other relevant classes which
 * are the transient representation of replicas within the Controller-side
 * Replication Framework.
 */

// System headers
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// Forward declarations
namespace lsst::qserv::replica {
class ProtocolReplicaInfo;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class ReplicaInfo represents a status of a replica received from the corresponding worker service.
 */
class ReplicaInfo {
public:
    class FileInfo {
    public:
        /// The short name
        std::string name;

        /// The current (or final) size (bytes)
        std::uint64_t size = 0;

        /// The (file content) modification timestamp in seconds (since the UNIX Epoch)
        std::time_t mtime = 0;

        /// The control/check sum of the file's content
        std::string cs;

        /// The time in milliseconds when the file creation began (where applies)
        std::uint64_t beginTransferTime = 0;

        /// The time in milliseconds when the file creation finished or when the last recording
        /// to the file was made (where applies)
        std::uint64_t endTransferTime = 0;

        /// The size of the input file (if applicable)
        std::uint64_t inSize = 0;

        FileInfo() = default;
        FileInfo(std::string const& name_, std::uint64_t size_, std::time_t mtime_, std::string const& cs_,
                 std::uint64_t beginTransferTime_, std::uint64_t endTransferTime_, std::uint64_t inSize_)
                : name(name_),
                  size(size_),
                  mtime(mtime_),
                  cs(cs_),
                  beginTransferTime(beginTransferTime_),
                  endTransferTime(endTransferTime_),
                  inSize(inSize_) {}
        explicit FileInfo(nlohmann::json const& info);
        FileInfo(FileInfo const&) = default;
        FileInfo& operator=(FileInfo const&) = default;

        bool operator==(FileInfo const& other) const {
            return name == other.name and size == other.size and cs == other.cs;
        }

        bool operator!=(FileInfo const& other) const { return not operator==(other); }

        /// @return the base name of a partitioned table (regardless if this is an overlap)
        std::string baseTable() const;

        /// @return 'true' if this file stores an overlap of a partition
        bool isOverlap() const;

        // @return 'true' if this is the data file
        bool isData() const { return name.substr(name.size() - 4, 4) == ".MYD"; }

        // @return 'true' if this is the index file
        bool isIndex() const { return name.substr(name.size() - 4, 4) == ".MYI"; }

        /// @return JSON representation of the object
        nlohmann::json toJson() const;

    private:
        /// @return table name with chunk (if any) and file extension removed
        std::string _removeChunkAndExt() const;

        static const std::size_t _extSize;      ///< The size of the MYISAM tables file extensions
        static const std::size_t _overlapSize;  ///< The size of the "FullOverlap" marker in table names
    };
    typedef std::vector<FileInfo> FileInfoCollection;

    enum Status { NOT_FOUND, CORRUPT, INCOMPLETE, COMPLETE };

    static std::string status2string(Status status);
    static Status string2status(std::string const& str);

    ReplicaInfo() = default;
    ReplicaInfo(Status status, std::string const& worker, std::string const& database, unsigned int chunk,
                std::uint64_t verifyTime, FileInfoCollection const& fileInfo);
    ReplicaInfo(Status status, std::string const& worker, std::string const& database, unsigned int chunk,
                std::uint64_t verifyTime);
    explicit ReplicaInfo(ProtocolReplicaInfo const* info);
    explicit ReplicaInfo(nlohmann::json const& info);
    ReplicaInfo(ReplicaInfo const& ri) = default;
    ReplicaInfo& operator=(ReplicaInfo const& ri) = default;

    ~ReplicaInfo() = default;

    void setFileInfo(FileInfoCollection const& fileInfo);
    void setFileInfo(FileInfoCollection&& fileInfo);

    Status status() const { return _status; }
    std::string const& worker() const { return _worker; }
    std::string const& database() const { return _database; }
    unsigned int chunk() const { return _chunk; }
    std::uint64_t verifyTime() const { return _verifyTime; }
    FileInfoCollection const& fileInfo() const { return _fileInfo; }

    /// file name is the key
    std::map<std::string, FileInfo> fileInfoMap() const;

    /**
     * @return The minimum start time of the file migration operations of any
     *  file associated with the replica.
     * @note The method is allowed to return 0 if the ReplicaInfo was not constructed
     *  in a context of creating a new replica.
     */
    std::uint64_t beginTransferTime() const;

    /**
     * @return The maximum end time of the file migration operations of any
     *  file associated with the replica.
     * @note The method is allowed to return 0 if the ReplicaInfo was not constructed
     *  in a context of creating a new replica.
     */
    std::uint64_t endTransferTime() const;

    std::unique_ptr<ProtocolReplicaInfo> info() const;

    void setInfo(ProtocolReplicaInfo* info) const;

    bool operator==(ReplicaInfo const& other) const {
        return _status == other._status and _worker == other._worker and _database == other._database and
               _chunk == other._chunk and _equalFileCollections(other);
    }

    bool operator!=(ReplicaInfo const& other) const { return not operator==(other); }

    /// @return JSON representation of the object
    nlohmann::json toJson() const;

private:
    bool _equalFileCollections(ReplicaInfo const& other) const;

    Status _status = Status::NOT_FOUND;
    std::string _worker;
    std::string _database;
    unsigned int _chunk;
    std::uint64_t _verifyTime = 0;
    FileInfoCollection _fileInfo;
};

std::ostream& operator<<(std::ostream& os, ReplicaInfo::FileInfo const& fi);

std::ostream& operator<<(std::ostream& os, ReplicaInfo const& ri);

typedef std::vector<ReplicaInfo> ReplicaInfoCollection;

std::ostream& operator<<(std::ostream& os, ReplicaInfoCollection const& ric);

/**
 * The type which groups ReplicaInfo by:
 *
 *   <chunk number>, <database>, <worker>
 */
typedef std::map<unsigned int,                   // chunk
                 std::map<std::string,           // database
                          std::map<std::string,  // worker
                                   ReplicaInfo>>>
        ChunkDatabaseWorkerReplicaInfo;

/**
 * Pretty-print the collection of replicas as a table
 *
 * @param caption The table caption to be printed before the table
 * @param prefix The prefix string to be printed at the beginning of each line
 * @param collection The collection to be printed
 * @param os The output stream where to direct the output to
 * @param pageSize The optional number of rows in the table (0 means no pagination)
 */
void printAsTable(std::string const& caption, std::string const& prefix,
                  ChunkDatabaseWorkerReplicaInfo const& collection, std::ostream& os,
                  std::size_t pageSize = 0);

/**
 * The type which groups ReplicaInfo by:
 *
 *   <chunk number>, <database>
 */
typedef std::map<unsigned int,          // chunk
                 std::map<std::string,  // database
                          ReplicaInfo>>
        ChunkDatabaseReplicaInfo;

/**
 * Pretty-print the collection of replicas as a table
 *
 * @param caption The table caption to be printed before the table
 * @param prefix The prefix string to be printed at the beginning of each line
 * @param collection The collection to be printed
 * @param os The output stream where to direct the output to
 * @param pageSize The optional number of rows in the table (0 means no pagination)
 */
void printAsTable(std::string const& caption, std::string const& prefix,
                  ChunkDatabaseReplicaInfo const& collection, std::ostream& os, std::size_t pageSize = 0);

/**
 * The type which groups ReplicaInfo by:
 *
 *   <database family>, <chunk number>, <database>, <worker>
 */
typedef std::map<std::string,                             // database family
                 std::map<unsigned int,                   // chunk
                          std::map<std::string,           // database
                                   std::map<std::string,  // worker
                                            ReplicaInfo>>>>
        FamilyChunkDatabaseWorkerInfo;

/**
 * Pretty-print the collection of replicas as a table
 *
 * @param caption The table caption to be printed before the table
 * @param prefix The prefix string to be printed at the beginning of each line
 * @param collection The collection to be printed
 * @param os The output stream where to direct the output to
 * @param pageSize The optional number of rows in the table (0 means no pagination)
 */
void printAsTable(std::string const& caption, std::string const& prefix,
                  FamilyChunkDatabaseWorkerInfo const& collection, std::ostream& os,
                  std::size_t pageSize = 0);

/**
 * Structure QservReplica represents replica entries used in communications
 * with Qserv workers management services.
 */
struct QservReplica {
    unsigned int chunk;
    std::string database;
    unsigned int useCount;

    QservReplica(unsigned int chunk_, std::string const& database_, unsigned int useCount_)
            : chunk(chunk_), database(database_), useCount(useCount_) {}
};

/// The type definition for a collection of Qserv replicas
typedef std::vector<QservReplica> QservReplicaCollection;

/**
 * One-directional comparison of the replica collections reported by Qserv workers
 *
 * The function will also report elements which aren't found in second collection.
 *
 * @note The output collection will be modified (reset to the empty state) even if
 *  the function will not find any differences.
 * @param one The input collection to be compared with the second one
 * @param two The input collection to be compared with the first one
 * @param inFirstOnly The output collection with elements of the first collection which are not
 *  found in the second collection
 * @return 'true' if different
 */
bool diff(QservReplicaCollection const& one, QservReplicaCollection const& two,
          QservReplicaCollection& inFirstOnly);

/**
 * Bi-directional comparison of the replica collections reported by Qserv workers
 *
 * The function will also elements keys which aren't found in opposite
 * collections.
 *
 * @note The output collections will be modified (reset to the empty state) even if
 *  the function won't find any differences.
 * @param one The input collection to be compared with the second one
 * @param two The input collection to be compared with the first one
 * @param inFirstOnly The output collection with elements of the first collection which are
 *  not found in the second collection
 * @param inSecondOnly The output collection with elements of the second collection which are
 *  not found in the first collection
 * @return 'true' if different
 */
bool diff2(QservReplicaCollection const& one, QservReplicaCollection const& two,
           QservReplicaCollection& inFirstOnly, QservReplicaCollection& inSecondOnly);

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_REPLICAINFO_H
