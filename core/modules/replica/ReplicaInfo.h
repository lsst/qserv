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
#ifndef LSST_QSERV_REPLICA_REPLICA_INFO_H
#define LSST_QSERV_REPLICA_REPLICA_INFO_H

/// ReplicaInfo.h declares:
///
/// struct ReplicaInfo
/// (see individual class documentation for more information)

// System headers
#include <ctime>
#include <map>
#include <ostream>
#include <string>
#include <vector>

// Qserv headers


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
  * the correspondig worker service.
  *
  * Note that this class can only be constructed from an object of
  * the corresponding protobuf type. And there is a complementary operation
  * for translating the state of this class's object into an object of
  * the same protobuf type.
  */
class ReplicaInfo {

public:

    /// An information entry for a file
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
    };
    typedef std::vector<FileInfo> FileInfoCollection;

    /// Possible statuses of a replica
    enum Status {
        NOT_FOUND,
        CORRUPT,
        INCOMPLETE,
        COMPLETE
    };
    
    /// Return the string representation of the status
    static std::string status2string (Status status);

    /**
     * Construct with the default state NOT_FOUND
     */
    ReplicaInfo ();

    /**
     * Construct with the specified state.
     *
     * @param status     - object status (see notes above)
     * @param worker     - the name of the worker wre the replica is located
     * @param database   - the name of the database
     * @param chunk      - the chunk number
     * @param verifyTime - when the replica info was obtainer by a worker
     * @param fileInfo   - a collection of info on each file of the chunk
     */
    ReplicaInfo (Status                    status,
                 std::string const&        worker,
                 std::string const&        database,
                 unsigned int              chunk,
                 uint64_t                  verifyTime,
                 FileInfoCollection const& fileInfo);

    /// Construct from a protobuf object
    explicit ReplicaInfo (proto::ReplicationReplicaInfo const* info);

    /// Copy constructor
    ReplicaInfo (ReplicaInfo const& ri) = default;

    /// Assignment operator
    ReplicaInfo& operator= (ReplicaInfo const& ri) = default;

    /// Destructor
    ~ReplicaInfo () = default;

    // Trivial accessors

    Status status () const { return _status; }

    std::string const& worker   () const { return _worker; }
    std::string const& database () const { return _database; }

    unsigned int chunk () const { return _chunk; }

    /**
     * Return the last time when the replica status was checked
     */
    uint64_t verifyTime () const { return _verifyTime; }


    /// Return a collection of files constituiting the replica
    FileInfoCollection const& fileInfo () const { return _fileInfo; }

    /**
     * Return a collection of files constituiting the replica as a map,
     * in which the file name is the key.
     */
    std::map<std::string,FileInfo> fileInfoMap () const;

    /**
     * Return the minimum start time of the file migration operations of any
     * file associated with the replica.
     *
     * NOTE: the method is allowed to return 0 if the ReplicaInfo was not
     * produced in a context of creating a new replica.
     */
    uint64_t beginTransferTime () const;

    /**
     * Return the maximum end time of the file migration operations of any
     * file associated with the replica.
     *
     * NOTE: the method is allowed to return 0 if the ReplicaInfo was not
     * produced in a context of creating a new replica.
     */
    uint64_t endTransferTime () const;

    /**
     * Return a protobuf object
     *
     * OWNERSHIP TRANSFER NOTE: this method allocates a new object and
     * returns a pointer along with its ownership.
     */
    lsst::qserv::proto::ReplicationReplicaInfo* info () const;

    /**
     * Initialize a protobuf object from the object's state
     */
    void setInfo (proto::ReplicationReplicaInfo* info) const;

private:

    // Data members
    
    Status _status;

    std::string _worker;
    std::string _database;

    unsigned int _chunk;
    
    uint64_t _verifyTime;

    FileInfoCollection _fileInfo;
};

/// Overloaded streaming operator for type ReplicaInfo
std::ostream& operator<< (std::ostream& os, ReplicaInfo const& ri);

/// The collection type for transient representations
typedef std::vector<ReplicaInfo> ReplicaInfoCollection;

/// Overloaded streaming operator for type ReplicaInfoCollection
std::ostream& operator<< (std::ostream& os, ReplicaInfoCollection const& ric);

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_REPLICA_INFO_H