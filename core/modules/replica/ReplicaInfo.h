/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#include <ctime>
#include <map>
#include <ostream>
#include <string>
#include <vector>

// Forward declarations

namespace lsst {
namespace qserv {
namespace proto {

class ReplicationReplicaInfo;

}}} // namespace lsst::qserv::proto

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class ReplicaInfo represents a status of a replica received from
  * the corresponding worker service.
  *
  * Note that this class can only be constructed from an object of
  * the corresponding Protobuf type. And there is a complementary operation
  * for translating the state of this class's object into an object of
  * the same Protobuf type.
  */
class ReplicaInfo {

public:

    /// Structure FileInfo represents an information entry for a file
    struct FileInfo {

        /// The short name of the file
        std::string name;

        /// The current (or final) size of the file in bytes
        uint64_t size;

        /// The (file content) modification timestamp in seconds (since the UNIX Epoch)
        std::time_t mtime;

        /// The control/check sum of the file's content
        std::string cs;

        /// The time in milliseconds when the file creation began (where applies)
        uint64_t beginTransferTime;

        /// The time in milliseconds when the file creation finished or when
        /// the last recording to the file was made (where applies)
        uint64_t endTransferTime;

        /// The size of the input file
        uint64_t inSize;

        /**
         * Comparison operator
         *
         * @param other - object to be compared with
         * @return 'true' if the current object is semantically identical to the other one
         */
        bool operator==(FileInfo const& other) const {
            return
                name == other.name and
                size == other.size and
                cs   == other.cs;
        }

        /**
         * The complementary comparison operator
         * 
         * @param other - object to be compared with
         * @return 'true' if the current object is semantically different from the other one
         */
        bool operator!=(FileInfo const& other) const {
            return not operator==(other);
        }
    };
    typedef std::vector<FileInfo> FileInfoCollection;

    /// Type Status defines possible states of a replica
    enum Status {
        NOT_FOUND,
        CORRUPT,
        INCOMPLETE,
        COMPLETE
    };

    /// @return the string representation of the status
    static std::string status2string(Status status);

    /**
     * Construct with the default state NOT_FOUND
     */
    ReplicaInfo();

    /**
     * Construct with the specified state.
     *
     * @param status     - object status (see notes above)
     * @param worker     - the name of the worker where the replica is located
     * @param database   - the name of the database
     * @param chunk      - the chunk number
     * @param verifyTime - when the replica info was obtainer by a worker
     * @param fileInfo   - a collection of info on each file of the chunk
     */
    ReplicaInfo(Status status,
                std::string const& worker,
                std::string const& database,
                unsigned int chunk,
                uint64_t verifyTime,
                FileInfoCollection const& fileInfo);

    /**
     * Construct with the specified state (no files provided)
     *
     * @param status     - object status (see notes above)
     * @param worker     - the name of the worker where the replica is located
     * @param database   - the name of the database
     * @param chunk      - the chunk number
     * @param verifyTime - when the replica info was obtainer by a worker
     */
    ReplicaInfo(Status status,
                std::string const& worker,
                std::string const& database,
                unsigned int chunk,
                uint64_t verifyTime);

    /**
     * Construct from a Protobuf object
     *
     * @param info - Protobuf object
     */
    explicit ReplicaInfo(proto::ReplicationReplicaInfo const* info);

    ReplicaInfo(ReplicaInfo const& ri) = default;
    ReplicaInfo& operator=(ReplicaInfo const& ri) = default;

    ~ReplicaInfo() = default;

    /**
     * Explicitly set a collection of files.
     *
     * @param fileInfo - collection of files to set (or replace older one)
     */
    void setFileInfo(FileInfoCollection const& fileInfo);

    /**
     * Explicitly set a collection of files (the move semantics for the input collection)
     *
     * @param fileInfo - collection of files to set (or replace older one)
     */
    void setFileInfo(FileInfoCollection&& fileInfo);

    // Trivial get methods

    Status status() const { return _status; }

    std::string const& worker() const   { return _worker; }
    std::string const& database() const { return _database; }

    unsigned int chunk() const { return _chunk; }

    /// @return the last time when the replica status was checked
    uint64_t verifyTime() const { return _verifyTime; }

    /// @return a collection of files associated with the replica
    FileInfoCollection const& fileInfo() const { return _fileInfo; }

    /**
     * @return a collection of files associated with the replica as a map,
     * in which the file name is the key.
     */
    std::map<std::string, FileInfo> fileInfoMap() const;

    /**
     * @return the minimum start time of the file migration operations of any
     * file associated with the replica.
     *
     * NOTE: the method is allowed to return 0 if the ReplicaInfo was not
     * produced in a context of creating a new replica.
     */
    uint64_t beginTransferTime() const;

    /**
     * @return the maximum end time of the file migration operations of any
     * file associated with the replica.
     *
     * NOTE: the method is allowed to return 0 if the ReplicaInfo was not
     * produced in a context of creating a new replica.
     */
    uint64_t endTransferTime() const;

    /**
     * @return a Protobuf object
     *
     * OWNERSHIP TRANSFER NOTE: this method allocates a new object and
     * returns a pointer along with its ownership.
     */
    lsst::qserv::proto::ReplicationReplicaInfo* info() const;

    /**
     * Initialize a Protobuf object from the object's state
     *
     * @param info - Protobuf object
     */
    void setInfo(proto::ReplicationReplicaInfo* info) const;

    /**
     * Comparision operator
     *
     * @param other - object to be compared with
     * @return 'true' if the current object is semantically identical to the other one
     */
    bool operator==(ReplicaInfo const& other) const {
        return
            _status   == other._status and
            _worker   == other._worker and
            _database == other._database and
            _chunk    == other._chunk and
            equalFileCollections(other);
    }
    
    /**
     * The complementary comparison operator
     * 
     * @param other - object to be compared with
     * @return 'true' if the current object is semantically different from the other one
     */
    bool operator!=(ReplicaInfo const& other) const {
        return not operator==(other);
    }

private:

    /**
     * Compare this object's file collection with the other's
     *
     * @param other - object whose file collection needs to be compared with the current one's
     * @return 'true' of both collections are semantically equivalent
     */
    bool equalFileCollections(ReplicaInfo const& other) const;    

private:

    // Data members

    Status _status;

    std::string _worker;
    std::string _database;

    unsigned int _chunk;

    uint64_t _verifyTime;

    FileInfoCollection _fileInfo;
};

/// Overloaded streaming operator for type ReplicaInfo::FileInfo
std::ostream& operator<<(std::ostream& os, ReplicaInfo::FileInfo const& fi);

/// Overloaded streaming operator for type ReplicaInfo
std::ostream& operator<<(std::ostream& os, ReplicaInfo const& ri);

/// The collection type for transient representations
typedef std::vector<ReplicaInfo> ReplicaInfoCollection;

/// Overloaded streaming operator for type ReplicaInfoCollection
std::ostream& operator<<(std::ostream& os, ReplicaInfoCollection const& ric);


/**
 * The type which groups ReplicaInfo by:
 *
 *   <chunk number>, <database>, <worker>
 */
typedef std::map<unsigned int,          // chunk
         std::map<std::string,          // database
                  std::map<std::string, // worker
                           ReplicaInfo>>> ChunkDatabaseWorkerReplicaInfo;


/**
 * Pretty-print the collection of replicas as a table
 * 
 * @param caption
 *   The table caption to be printed before the table
 * 
 * @param prefix
 *   The prefix string to be printed at the beginning of each line
 * 
 * @param collection
 *   The collection to be printed
 * 
 * @param os
 *   The output stream where to direct the output to
 * 
 * @param pageSize
 *   The optional number of rows in the table (0 means no pagination)
 */
void printAsTable(std::string const& caption,
                  std::string const& prefix,
                  ChunkDatabaseWorkerReplicaInfo const& collection,
                  std::ostream& os,
                  size_t pageSize=0);


/**
 * The type which groups ReplicaInfo by:
 *
 *   <chunk number>, <database>
 */
typedef std::map<unsigned int,          // chunk
                 std::map<std::string,  // database
                          ReplicaInfo>> ChunkDatabaseReplicaInfo;


/**
 * Pretty-print the collection of replicas as a table
 * 
 * @param caption
 *   The table caption to be printed before the table
 * 
 * @param prefix
 *   The prefix string to be printed at the beginning of each line
 * 
 * @param collection
 *   The collection to be printed
 * 
 * @param os
 *   The output stream where to direct the output to
 * 
 * @param pageSize
 *   The optional number of rows in the table (0 means no pagination)
 */
void printAsTable(std::string const& caption,
                  std::string const& prefix,
                  ChunkDatabaseReplicaInfo const& collection,
                  std::ostream& os,
                  size_t pageSize=0);


/**
 * The type which groups ReplicaInfo by:
 *
 *   <database family>, <chunk number>, <database>, <worker>
 */
typedef std::map<std::string,                               // database family
                 std::map<unsigned int,                     // chunk
                          std::map<std::string,             // database
                                   std::map<std::string,    // worker
                                            ReplicaInfo>>>> FamilyChunkDatabaseWorkerInfo;


/**
 * Pretty-print the collection of replicas as a table
 * 
 * @param caption
 *   The table caption to be printed before the table
 * 
 * @param prefix
 *   The prefix string to be printed at the beginning of each line
 * 
 * @param collection
 *   The collection to be printed
 * 
 * @param os
 *   The output stream where to direct the output to
 * 
 * @param pageSize
 *   The optional number of rows in the table (0 means no pagination)
 */
void printAsTable(std::string const& caption,
                  std::string const& prefix,
                  FamilyChunkDatabaseWorkerInfo const& collection,
                  std::ostream& os,
                  size_t pageSize=0);


/**
 * Structure QservReplica represents replica entries used in communications
 * with Qserv workers management services.
 */
struct QservReplica {
    unsigned int chunk;
    std::string database;
    unsigned int useCount;
};

/// The type definition for a collection of Qserv replicas
typedef std::vector<QservReplica> QservReplicaCollection;

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_REPLICAINFO_H
