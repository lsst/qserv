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

// Class header
#include "replica/ReplicaInfo.h"

// System headers
#include <algorithm>
#include <stdexcept>

// Qserv headers
#include "proto/replication.pb.h"


namespace proto = lsst::qserv::proto;

namespace {

/// State translation

void setInfoImpl ( lsst::qserv::replica::ReplicaInfo const& ri,
                  proto::ReplicationReplicaInfo*                 info) {

    switch (ri.status()) {
        case lsst::qserv::replica::ReplicaInfo::Status::NOT_FOUND:  info->set_status(proto::ReplicationReplicaInfo::NOT_FOUND);  break;
        case lsst::qserv::replica::ReplicaInfo::Status::CORRUPT:    info->set_status(proto::ReplicationReplicaInfo::CORRUPT);    break;
        case lsst::qserv::replica::ReplicaInfo::Status::INCOMPLETE: info->set_status(proto::ReplicationReplicaInfo::INCOMPLETE); break;
        case lsst::qserv::replica::ReplicaInfo::Status::COMPLETE:   info->set_status(proto::ReplicationReplicaInfo::COMPLETE);   break;
        default:
            throw std::logic_error (
                        "unhandled status " + lsst::qserv::replica::ReplicaInfo::status2string(ri.status()) +
                        " in ReplicaInfo::setInfoImpl()");
    }
    info->set_worker      (ri.worker    ());
    info->set_database    (ri.database  ());
    info->set_chunk       (ri.chunk     ());
    info->set_verify_time (ri.verifyTime());

    for (auto const& fi: ri.fileInfo()) {
        lsst::qserv::proto::ReplicationFileInfo* fileInfo = info->add_file_info_many();
        fileInfo->set_name                (fi.name);
        fileInfo->set_size                (fi.size);
        fileInfo->set_mtime               (fi.mtime);
        fileInfo->set_cs                  (fi.cs);
        fileInfo->set_begin_transfer_time (fi.beginTransferTime);
        fileInfo->set_end_transfer_time   (fi.endTransferTime);
        fileInfo->set_in_size             (fi.inSize);
    }  
}
}  // namespace

namespace lsst {
namespace qserv {
namespace replica {


std::string
ReplicaInfo::status2string (Status status) {
    switch (status) {
        case Status::NOT_FOUND:                  return "NOT_FOUND";
        case Status::CORRUPT:                    return "CORRUPT";
        case Status::INCOMPLETE:                 return "INCOMPLETE";
        case Status::COMPLETE:                   return "COMPLETE";
    }
    throw std::logic_error("unhandled status " + std::to_string(status) +
                           " in ReplicaInfo::status2string()");
}
ReplicaInfo::ReplicaInfo ()
    :   _status     (Status::NOT_FOUND),
        _worker     (""),
        _database   (""),
        _chunk      (0),
        _verifyTime (0),
        _fileInfo   () {
}

ReplicaInfo::ReplicaInfo (Status                                 status,
                          std::string const&                     worker,
                          std::string const&                     database,
                          unsigned int                           chunk,
                          uint64_t                               verifyTime,
                          ReplicaInfo::FileInfoCollection const& fileInfo)
    :   _status     (status),
        _worker     (worker),
        _database   (database),
        _chunk      (chunk),
        _verifyTime (verifyTime),
        _fileInfo   (fileInfo) {
}

ReplicaInfo::ReplicaInfo (proto::ReplicationReplicaInfo const* info) {

    switch (info->status()) {
        case proto::ReplicationReplicaInfo::NOT_FOUND:  this->_status = Status::NOT_FOUND;  break;
        case proto::ReplicationReplicaInfo::CORRUPT:    this->_status = Status::CORRUPT;    break;
        case proto::ReplicationReplicaInfo::INCOMPLETE: this->_status = Status::INCOMPLETE; break;
        case proto::ReplicationReplicaInfo::COMPLETE:   this->_status = Status::COMPLETE;   break;
        default:
            throw std::logic_error (
                        "unhandled status " + proto::ReplicationReplicaInfo_ReplicaStatus_Name(info->status()) +
                        " in ReplicaInfo::ReplicaInfo()");
    }
    _worker   = info->worker();
    _database = info->database();
    _chunk    = info->chunk();

    for (int idx = 0; idx < info->file_info_many_size(); ++idx) {
        proto::ReplicationFileInfo const& fileInfo = info->file_info_many(idx);
        _fileInfo.emplace_back (
            FileInfo ({
                fileInfo.name(),
                fileInfo.size(),
                fileInfo.mtime(),
                fileInfo.cs(),
                fileInfo.begin_transfer_time(),
                fileInfo.end_transfer_time(),
                fileInfo.in_size()
            })
        );
    }
    _verifyTime = info->verify_time();
}

uint64_t
ReplicaInfo::beginTransferTime () const {
    uint64_t t = 0;
    for (auto const& f: _fileInfo)
        t = t ? std::min(t, f.beginTransferTime) : f.beginTransferTime;
    return t;
}

uint64_t
ReplicaInfo::endTransferTime () const {
    uint64_t t = 0;
    for (auto const& f: _fileInfo) t = std::max(t, f.endTransferTime);
    return t;
}

proto::ReplicationReplicaInfo*
ReplicaInfo::info () const {
    proto::ReplicationReplicaInfo* ptr = new proto::ReplicationReplicaInfo;
    ::setInfoImpl(*this, ptr);
    return ptr;
}

void
ReplicaInfo::setInfo (lsst::qserv::proto::ReplicationReplicaInfo* info) const {
    ::setInfoImpl(*this, info);
}

std::map<std::string,ReplicaInfo::FileInfo>
ReplicaInfo::fileInfoMap () const {
    std::map<std::string,ReplicaInfo::FileInfo> result;
    for (auto const& f: _fileInfo)
        result[f.name] = f;
    return result;
}

    
std::ostream&
operator<< (std::ostream& os, ReplicaInfo::FileInfo const& fi) {
    static float const MB =  1024.0*1024.0;
    static float const millisec_per_sec = 1000.0;
    float const sizeMB  = fi.size / MB;
    float const seconds = (fi.endTransferTime - fi.beginTransferTime) / millisec_per_sec;
    float const completedPercent = fi.inSize ? 100.0 * fi.size / fi.inSize : 0.0;

    os  << "FileInfo"
        << " name: "   << fi.name
        << " size: "   << fi.size
        << " mtime: "  << fi.mtime
        << " inSize: " << fi.inSize
        << " cs: "     << fi.cs
        << " beginTransferTime: " << fi.beginTransferTime
        << " endTransferTime: "   << fi.endTransferTime
        << " completed [%]: "     << completedPercent
        << " xfer [MB/s]: "       << (fi.endTransferTime ? sizeMB / seconds : 0.0);

    return os;
}

std::ostream&
operator<< (std::ostream& os, ReplicaInfo const& ri) {
    os  << "ReplicaInfo"
        << " status: "     << ReplicaInfo::status2string(ri.status())
        << " worker: "     << ri.worker()
        << " database: "   << ri.database()
        << " chunk: "      << ri.chunk()
        << " verifyTime: " << ri.verifyTime()
        << " files: ";
    for (auto const& fi: ri.fileInfo())
        os << "\n   (" << fi << ")";
    return os;
}

std::ostream&
operator<< (std::ostream &os, ReplicaInfoCollection const& ric) {
    os << "ReplicaInfoCollection";
    for (auto const& ri: ric)
        os << "\n (" << ri << ")";
    return os;
}

}}} // namespace lsst::qserv::replica
