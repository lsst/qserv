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

// Class header
#include "replica/ReplicaInfo.h"

// System headers
#include <algorithm>
#include <stdexcept>

// Qserv headers
#include "replica/protocol.pb.h"
#include "util/TablePrinter.h"

using namespace std;
using namespace lsst::qserv::replica;
using json = nlohmann::json;

namespace {

/// State translation
void setInfoImpl(ReplicaInfo const& ri, ProtocolReplicaInfo* info) {
    switch (ri.status()) {
        case ReplicaInfo::Status::NOT_FOUND:
            info->set_status(ProtocolReplicaInfo::NOT_FOUND);
            break;
        case ReplicaInfo::Status::CORRUPT:
            info->set_status(ProtocolReplicaInfo::CORRUPT);
            break;
        case ReplicaInfo::Status::INCOMPLETE:
            info->set_status(ProtocolReplicaInfo::INCOMPLETE);
            break;
        case ReplicaInfo::Status::COMPLETE:
            info->set_status(ProtocolReplicaInfo::COMPLETE);
            break;
        default:
            throw logic_error("ReplicaInfo::" + string(__func__) + "  unhandled status " +
                              ReplicaInfo::status2string(ri.status()));
    }
    info->set_worker(ri.worker());
    info->set_database(ri.database());
    info->set_chunk(ri.chunk());
    info->set_verify_time(ri.verifyTime());

    for (auto&& fi : ri.fileInfo()) {
        ProtocolFileInfo* fileInfo = info->add_file_info_many();
        fileInfo->set_name(fi.name);
        fileInfo->set_size(fi.size);
        fileInfo->set_mtime(fi.mtime);
        fileInfo->set_cs(fi.cs);
        fileInfo->set_begin_transfer_time(fi.beginTransferTime);
        fileInfo->set_end_transfer_time(fi.endTransferTime);
        fileInfo->set_in_size(fi.inSize);
    }
}

}  // namespace

namespace lsst::qserv::replica {

const size_t ReplicaInfo::FileInfo::_extSize = string(".MYD").size();
const size_t ReplicaInfo::FileInfo::_overlapSize = string("FullOverlap").size();

string ReplicaInfo::FileInfo::baseTable() const {
    // IMPLEMENTATION NOTE: the algorithm implemented below is more
    // efficient than others based on the C++ Regular Expression library.
    // It also works for both regular and partitioned tables.

    string const noChunkNoExt = _removeChunkAndExt();
    if (noChunkNoExt.size() <= _overlapSize) {
        // no room for overlap, or the table name is "FullOverlap" which is also legit
        return noChunkNoExt;
    }
    if (noChunkNoExt.substr(noChunkNoExt.size() - _overlapSize) == "FullOverlap") {
        // the overlap marker removed
        return noChunkNoExt.substr(0, noChunkNoExt.size() - _overlapSize);
    }
    // NO overlap marker in the table name
    return noChunkNoExt;
}

bool ReplicaInfo::FileInfo::isOverlap() const {
    string const noChunkNoExt = _removeChunkAndExt();
    return noChunkNoExt.size() > _overlapSize and
           0 == noChunkNoExt.compare(noChunkNoExt.size() - _overlapSize, _overlapSize, "FullOverlap");
}

string ReplicaInfo::FileInfo::_removeChunkAndExt() const {
    auto const underscorePos = name.find_last_of('_');
    if (underscorePos == string::npos) {
        // not a chunk table, extension removed
        return name.substr(0, name.size() - _extSize);
    } else {
        // the chunk table, both chunk number and extension removed
        return name.substr(0, underscorePos);
    }
}

json ReplicaInfo::FileInfo::toJson() const {
    json infoJson;

    infoJson["name"] = name;
    infoJson["size"] = size;
    infoJson["mtime"] = mtime;
    infoJson["cs"] = cs;
    infoJson["begin_transfer_time"] = beginTransferTime;
    infoJson["end_transfer_time"] = endTransferTime;
    infoJson["in_size"] = inSize;

    return infoJson;
}

string ReplicaInfo::status2string(Status status) {
    switch (status) {
        case Status::NOT_FOUND:
            return "NOT_FOUND";
        case Status::CORRUPT:
            return "CORRUPT";
        case Status::INCOMPLETE:
            return "INCOMPLETE";
        case Status::COMPLETE:
            return "COMPLETE";
    }
    throw logic_error("ReplicaInfo::" + string(__func__) + "  unhandled status " + to_string(status));
}

ReplicaInfo::ReplicaInfo()
        : _status(Status::NOT_FOUND), _worker(""), _database(""), _chunk(0), _verifyTime(0), _fileInfo() {}

ReplicaInfo::ReplicaInfo(Status status, string const& worker, string const& database, unsigned int chunk,
                         uint64_t verifyTime, ReplicaInfo::FileInfoCollection const& fileInfo)
        : _status(status),
          _worker(worker),
          _database(database),
          _chunk(chunk),
          _verifyTime(verifyTime),
          _fileInfo(fileInfo) {}

ReplicaInfo::ReplicaInfo(Status status, string const& worker, string const& database, unsigned int chunk,
                         uint64_t verifyTime)
        : _status(status), _worker(worker), _database(database), _chunk(chunk), _verifyTime(verifyTime) {}

ReplicaInfo::ReplicaInfo(ProtocolReplicaInfo const* info) {
    switch (info->status()) {
        case ProtocolReplicaInfo::NOT_FOUND:
            this->_status = Status::NOT_FOUND;
            break;
        case ProtocolReplicaInfo::CORRUPT:
            this->_status = Status::CORRUPT;
            break;
        case ProtocolReplicaInfo::INCOMPLETE:
            this->_status = Status::INCOMPLETE;
            break;
        case ProtocolReplicaInfo::COMPLETE:
            this->_status = Status::COMPLETE;
            break;
        default:
            throw logic_error("ReplicaInfo::" + string(__func__) + "  unhandled status " +
                              ProtocolReplicaInfo_ReplicaStatus_Name(info->status()));
    }
    _worker = info->worker();
    _database = info->database();
    _chunk = info->chunk();

    for (int idx = 0; idx < info->file_info_many_size(); ++idx) {
        ProtocolFileInfo const& fileInfo = info->file_info_many(idx);
        _fileInfo.emplace_back(
                FileInfo({fileInfo.name(), fileInfo.size(), fileInfo.mtime(), fileInfo.cs(),
                          fileInfo.begin_transfer_time(), fileInfo.end_transfer_time(), fileInfo.in_size()}));
    }
    _verifyTime = info->verify_time();
}

void ReplicaInfo::setFileInfo(FileInfoCollection const& fileInfo) { _fileInfo = fileInfo; }

void ReplicaInfo::setFileInfo(FileInfoCollection&& fileInfo) { _fileInfo = fileInfo; }

uint64_t ReplicaInfo::beginTransferTime() const {
    uint64_t t = 0;
    for (auto&& f : _fileInfo) {
        t = t ? min(t, f.beginTransferTime) : f.beginTransferTime;
    }
    return t;
}

uint64_t ReplicaInfo::endTransferTime() const {
    uint64_t t = 0;
    for (auto&& f : _fileInfo) {
        t = max(t, f.endTransferTime);
    }
    return t;
}

unique_ptr<ProtocolReplicaInfo> ReplicaInfo::info() const {
    auto ptr = make_unique<ProtocolReplicaInfo>();
    ::setInfoImpl(*this, ptr.get());
    return ptr;
}

void ReplicaInfo::setInfo(ProtocolReplicaInfo* info) const { ::setInfoImpl(*this, info); }

map<string, ReplicaInfo::FileInfo> ReplicaInfo::fileInfoMap() const {
    map<string, ReplicaInfo::FileInfo> result;
    for (auto&& f : _fileInfo) {
        result[f.name] = f;
    }
    return result;
}

json ReplicaInfo::toJson() const {
    json infoJson;

    infoJson["status"] = status2string(_status);
    infoJson["worker"] = _worker;
    infoJson["database"] = _database;
    infoJson["chunk"] = _chunk;
    infoJson["verify_time"] = _verifyTime;

    json filesJson = json::array();
    for (auto&& file : _fileInfo) {
        filesJson.push_back(file.toJson());
    }
    infoJson["files"] = filesJson;

    return infoJson;
}

bool ReplicaInfo::_equalFileCollections(ReplicaInfo const& other) const {
    // Files of both collections needs to be map-sorted because objects may
    // have them stored in different order.

    map<string, ReplicaInfo::FileInfo> thisFileInfo = this->fileInfoMap();
    map<string, ReplicaInfo::FileInfo> otherFileInfo = other.fileInfoMap();

    if (thisFileInfo.size() != otherFileInfo.size()) return false;

    for (auto&& elem : thisFileInfo) {
        auto otherIter = otherFileInfo.find(elem.first);
        if (otherIter == otherFileInfo.end()) return false;
        if (otherIter->second != elem.second) return false;
    }
    return true;
}

ostream& operator<<(ostream& os, ReplicaInfo::FileInfo const& fi) {
    static float const MB = 1024.0 * 1024.0;
    static float const millisec_per_sec = 1000.0;
    float const sizeMB = fi.size / MB;
    float const seconds = (fi.endTransferTime - fi.beginTransferTime) / millisec_per_sec;
    float const completedPercent = fi.inSize ? 100.0 * fi.size / fi.inSize : 0.0;

    os << "FileInfo"
       << " name: " << fi.name << " size: " << fi.size << " mtime: " << fi.mtime << " inSize: " << fi.inSize
       << " cs: " << fi.cs << " beginTransferTime: " << fi.beginTransferTime
       << " endTransferTime: " << fi.endTransferTime << " completed [%]: " << completedPercent
       << " xfer [MB/s]: " << (fi.endTransferTime ? sizeMB / seconds : 0.0);

    return os;
}

ostream& operator<<(ostream& os, ReplicaInfo const& ri) {
    os << "ReplicaInfo"
       << " status: " << ReplicaInfo::status2string(ri.status()) << " worker: " << ri.worker()
       << " database: " << ri.database() << " chunk: " << ri.chunk() << " verifyTime: " << ri.verifyTime()
       << " files: ";
    for (auto&& fi : ri.fileInfo()) {
        os << "\n   (" << fi << ")";
    }
    return os;
}

ostream& operator<<(ostream& os, ReplicaInfoCollection const& ric) {
    os << "ReplicaInfoCollection";
    for (auto&& ri : ric) {
        os << "\n (" << ri << ")";
    }
    return os;
}

void printAsTable(string const& caption, string const& prefix,
                  ChunkDatabaseWorkerReplicaInfo const& collection, ostream& os, size_t pageSize) {
    vector<unsigned int> columnChunk;
    vector<string> columnDatabase;
    vector<size_t> columnNumReplicas;
    vector<string> columnWorkers;

    for (auto&& chunkEntry : collection) {
        unsigned int const& chunk = chunkEntry.first;

        for (auto&& databaseEntry : chunkEntry.second) {
            auto&& databaseName = databaseEntry.first;
            auto const numReplicas = databaseEntry.second.size();

            string workers;

            for (auto&& replicaEntry : databaseEntry.second) {
                auto&& workerName = replicaEntry.first;
                auto&& replicaInfo = replicaEntry.second;

                workers +=
                        workerName + (replicaInfo.status() != ReplicaInfo::Status::COMPLETE ? "(!) " : " ");
            }
            columnChunk.push_back(chunk);
            columnDatabase.push_back(databaseName);
            columnNumReplicas.push_back(numReplicas);
            columnWorkers.push_back(workers);
        }
    }
    util::ColumnTablePrinter table(caption, prefix, false);

    table.addColumn("chunk", columnChunk);
    table.addColumn("database", columnDatabase, util::ColumnTablePrinter::LEFT);
    table.addColumn("#replicas", columnNumReplicas);
    table.addColumn("workers", columnWorkers, util::ColumnTablePrinter::LEFT);

    table.print(os, false, false, pageSize, pageSize != 0);
}

void printAsTable(string const& caption, string const& prefix, ChunkDatabaseReplicaInfo const& collection,
                  ostream& os, size_t pageSize) {
    vector<unsigned int> columnChunk;
    vector<string> columnDatabase;
    vector<string> columnWarnings;

    for (auto&& chunkEntry : collection) {
        unsigned int const& chunk = chunkEntry.first;

        for (auto&& databaseEntry : chunkEntry.second) {
            auto&& databaseName = databaseEntry.first;
            auto&& replicaInfo = databaseEntry.second;

            columnChunk.push_back(chunk);
            columnDatabase.push_back(databaseName);
            columnWarnings.push_back(replicaInfo.status() != ReplicaInfo::Status::COMPLETE ? "INCOMPLETE "
                                                                                           : "");
        }
    }
    util::ColumnTablePrinter table(caption, prefix, false);

    table.addColumn("chunk", columnChunk);
    table.addColumn("database", columnDatabase, util::ColumnTablePrinter::LEFT);
    table.addColumn("warnings", columnWarnings, util::ColumnTablePrinter::LEFT);

    table.print(os, false, false, pageSize, pageSize != 0);
}

void printAsTable(string const& caption, string const& prefix,
                  FamilyChunkDatabaseWorkerInfo const& collection, ostream& os, size_t pageSize) {
    vector<string> columnFamily;
    vector<unsigned int> columnChunk;
    vector<string> columnDatabase;
    vector<size_t> columnNumReplicas;
    vector<string> columnWorkers;

    for (auto&& familyEntry : collection) {
        auto&& familyName = familyEntry.first;

        for (auto&& chunkEntry : familyEntry.second) {
            unsigned int const& chunk = chunkEntry.first;

            for (auto&& databaseEntry : chunkEntry.second) {
                auto&& databaseName = databaseEntry.first;
                auto const numReplicas = databaseEntry.second.size();

                string workers;

                for (auto&& replicaEntry : databaseEntry.second) {
                    auto&& workerName = replicaEntry.first;
                    auto&& replicaInfo = replicaEntry.second;

                    workers += workerName +
                               (replicaInfo.status() != ReplicaInfo::Status::COMPLETE ? "(!) " : " ");
                }
                columnFamily.push_back(familyName);
                columnChunk.push_back(chunk);
                columnDatabase.push_back(databaseName);
                columnNumReplicas.push_back(numReplicas);
                columnWorkers.push_back(workers);
            }
        }
    }
    util::ColumnTablePrinter table(caption, prefix, false);

    table.addColumn("database family", columnFamily, util::ColumnTablePrinter::LEFT);
    table.addColumn("chunk", columnChunk);
    table.addColumn("database", columnDatabase, util::ColumnTablePrinter::LEFT);
    table.addColumn("#replicas", columnNumReplicas);
    table.addColumn("workers", columnWorkers, util::ColumnTablePrinter::LEFT);

    table.print(os, false, false, pageSize, pageSize != 0);
}

bool diff(QservReplicaCollection const& one, QservReplicaCollection const& two,
          QservReplicaCollection& inFirstOnly) {
    inFirstOnly.clear();

    // Translate the second collection into a dictionary
    map<unsigned int, map<string, unsigned int>> replicas;

    for (auto&& replica : two) {
        replicas[replica.chunk][replica.database] = replica.useCount;
    }

    // Scan the first collection and identify all elements which aren't
    // present in the above created dictionary. Log those records into the
    // output collection.
    for (auto&& replica : one) {
        auto itr = replicas.find(replica.chunk);
        if (itr == replicas.end()) {
            inFirstOnly.push_back(replica);
            continue;
        }
        auto&& workers = itr->second;
        if (workers.find(replica.database) == workers.end()) {
            inFirstOnly.push_back(replica);
        }
    }
    return not((one.size() == two.size()) and inFirstOnly.empty());
}

bool diff2(QservReplicaCollection const& one, QservReplicaCollection const& two,
           QservReplicaCollection& inFirstOnly, QservReplicaCollection& inSecondOnly) {
    bool const notEqual1 = diff(one, two, inFirstOnly);
    bool const notEqual2 = diff(two, one, inSecondOnly);

    return notEqual1 or notEqual2;
}

}  // namespace lsst::qserv::replica
