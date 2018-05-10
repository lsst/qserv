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
#include "replica/Common.h"

// System headers
#include <stdexcept>

// Third party headers
#include "boost/uuid/uuid.hpp"
#include "boost/uuid/uuid_generators.hpp"
#include "boost/uuid/uuid_io.hpp"

// Qserv headers
#include "proto/replication.pb.h"

namespace lsst {
namespace qserv {
namespace replica {

std::string status2string(ExtendedCompletionStatus status) {
    switch (status) {
        case ExtendedCompletionStatus::EXT_STATUS_NONE:             return "EXT_STATUS_NONE";
        case ExtendedCompletionStatus::EXT_STATUS_INVALID_PARAM:    return "EXT_STATUS_INVALID_PARAM";
        case ExtendedCompletionStatus::EXT_STATUS_INVALID_ID:       return "EXT_STATUS_INVALID_ID";
        case ExtendedCompletionStatus::EXT_STATUS_DUPLICATE:        return "EXT_STATUS_DUPLICATE";
        case ExtendedCompletionStatus::EXT_STATUS_FOLDER_STAT:      return "EXT_STATUS_FOLDER_STAT";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_STAT:        return "EXT_STATUS_FILE_STAT";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_SIZE:        return "EXT_STATUS_FILE_SIZE";
        case ExtendedCompletionStatus::EXT_STATUS_FOLDER_READ:      return "EXT_STATUS_FOLDER_READ";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_READ:        return "EXT_STATUS_FILE_READ";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_ROPEN:       return "EXT_STATUS_FILE_ROPEN";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_CREATE:      return "EXT_STATUS_FILE_CREATE";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_OPEN:        return "EXT_STATUS_FILE_OPEN";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_RESIZE:      return "EXT_STATUS_FILE_RESIZE";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_WRITE:       return "EXT_STATUS_FILE_WRITE";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_COPY:        return "EXT_STATUS_FILE_COPY";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_DELETE:      return "EXT_STATUS_FILE_DELETE";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_RENAME:      return "EXT_STATUS_FILE_RENAME";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_EXISTS:      return "EXT_STATUS_FILE_EXISTS";
        case ExtendedCompletionStatus::EXT_STATUS_SPACE_REQ:        return "EXT_STATUS_SPACE_REQ";
        case ExtendedCompletionStatus::EXT_STATUS_NO_FOLDER:        return "EXT_STATUS_NO_FOLDER";
        case ExtendedCompletionStatus::EXT_STATUS_NO_FILE:          return "EXT_STATUS_NO_FILE";
        case ExtendedCompletionStatus::EXT_STATUS_NO_ACCESS:        return "EXT_STATUS_NO_ACCESS";
        case ExtendedCompletionStatus::EXT_STATUS_NO_SPACE:         return "EXT_STATUS_NO_SPACE";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_MTIME:       return "EXT_STATUS_FILE_MTIME";
    }
    throw std::logic_error(
                    "Common::status2string(ExtendedCompletionStatus) - unhandled status: " +
                    std::to_string(status));
}

ExtendedCompletionStatus translate(proto::ReplicationStatusExt status) {
    switch (status) {
        case proto::ReplicationStatusExt::NONE:             return ExtendedCompletionStatus::EXT_STATUS_NONE;
        case proto::ReplicationStatusExt::INVALID_PARAM:    return ExtendedCompletionStatus::EXT_STATUS_INVALID_PARAM;
        case proto::ReplicationStatusExt::INVALID_ID:       return ExtendedCompletionStatus::EXT_STATUS_INVALID_ID;
        case proto::ReplicationStatusExt::DUPLICATE:        return ExtendedCompletionStatus::EXT_STATUS_DUPLICATE;
        case proto::ReplicationStatusExt::FOLDER_STAT:      return ExtendedCompletionStatus::EXT_STATUS_FOLDER_STAT;
        case proto::ReplicationStatusExt::FILE_STAT:        return ExtendedCompletionStatus::EXT_STATUS_FILE_STAT;
        case proto::ReplicationStatusExt::FILE_SIZE:        return ExtendedCompletionStatus::EXT_STATUS_FILE_SIZE;
        case proto::ReplicationStatusExt::FOLDER_READ:      return ExtendedCompletionStatus::EXT_STATUS_FOLDER_READ;
        case proto::ReplicationStatusExt::FILE_READ:        return ExtendedCompletionStatus::EXT_STATUS_FILE_READ;
        case proto::ReplicationStatusExt::FILE_ROPEN:       return ExtendedCompletionStatus::EXT_STATUS_FILE_ROPEN;
        case proto::ReplicationStatusExt::FILE_CREATE:      return ExtendedCompletionStatus::EXT_STATUS_FILE_CREATE;
        case proto::ReplicationStatusExt::FILE_OPEN:        return ExtendedCompletionStatus::EXT_STATUS_FILE_OPEN;
        case proto::ReplicationStatusExt::FILE_RESIZE:      return ExtendedCompletionStatus::EXT_STATUS_FILE_RESIZE;
        case proto::ReplicationStatusExt::FILE_WRITE:       return ExtendedCompletionStatus::EXT_STATUS_FILE_WRITE;
        case proto::ReplicationStatusExt::FILE_COPY:        return ExtendedCompletionStatus::EXT_STATUS_FILE_COPY;
        case proto::ReplicationStatusExt::FILE_DELETE:      return ExtendedCompletionStatus::EXT_STATUS_FILE_DELETE;
        case proto::ReplicationStatusExt::FILE_RENAME:      return ExtendedCompletionStatus::EXT_STATUS_FILE_RENAME;
        case proto::ReplicationStatusExt::FILE_EXISTS:      return ExtendedCompletionStatus::EXT_STATUS_FILE_EXISTS;
        case proto::ReplicationStatusExt::SPACE_REQ:        return ExtendedCompletionStatus::EXT_STATUS_SPACE_REQ;
        case proto::ReplicationStatusExt::NO_FOLDER:        return ExtendedCompletionStatus::EXT_STATUS_NO_FOLDER;
        case proto::ReplicationStatusExt::NO_FILE:          return ExtendedCompletionStatus::EXT_STATUS_NO_FILE;
        case proto::ReplicationStatusExt::NO_ACCESS:        return ExtendedCompletionStatus::EXT_STATUS_NO_ACCESS;
        case proto::ReplicationStatusExt::NO_SPACE:         return ExtendedCompletionStatus::EXT_STATUS_NO_SPACE;
        case proto::ReplicationStatusExt::FILE_MTIME:       return ExtendedCompletionStatus::EXT_STATUS_FILE_MTIME;
    }
    throw std::logic_error(
                    "Common::translate(proto::ReplicationStatusExt) - unhandled status: " +
                    std::to_string(status));
}

proto::ReplicationStatusExt translate(ExtendedCompletionStatus status) {
    switch (status) {
        case ExtendedCompletionStatus::EXT_STATUS_NONE:             return proto::ReplicationStatusExt::NONE;
        case ExtendedCompletionStatus::EXT_STATUS_INVALID_PARAM:    return proto::ReplicationStatusExt::INVALID_PARAM;
        case ExtendedCompletionStatus::EXT_STATUS_INVALID_ID:       return proto::ReplicationStatusExt::INVALID_ID;
        case ExtendedCompletionStatus::EXT_STATUS_DUPLICATE:        return proto::ReplicationStatusExt::DUPLICATE;
        case ExtendedCompletionStatus::EXT_STATUS_FOLDER_STAT:      return proto::ReplicationStatusExt::FOLDER_STAT;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_STAT:        return proto::ReplicationStatusExt::FILE_STAT;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_SIZE:        return proto::ReplicationStatusExt::FILE_SIZE;
        case ExtendedCompletionStatus::EXT_STATUS_FOLDER_READ:      return proto::ReplicationStatusExt::FOLDER_READ;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_READ:        return proto::ReplicationStatusExt::FILE_READ;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_ROPEN:       return proto::ReplicationStatusExt::FILE_ROPEN;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_CREATE:      return proto::ReplicationStatusExt::FILE_CREATE;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_OPEN:        return proto::ReplicationStatusExt::FILE_OPEN;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_RESIZE:      return proto::ReplicationStatusExt::FILE_RESIZE;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_WRITE:       return proto::ReplicationStatusExt::FILE_WRITE;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_COPY:        return proto::ReplicationStatusExt::FILE_COPY;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_DELETE:      return proto::ReplicationStatusExt::FILE_DELETE;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_RENAME:      return proto::ReplicationStatusExt::FILE_RENAME;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_EXISTS:      return proto::ReplicationStatusExt::FILE_EXISTS;
        case ExtendedCompletionStatus::EXT_STATUS_SPACE_REQ:        return proto::ReplicationStatusExt::SPACE_REQ;
        case ExtendedCompletionStatus::EXT_STATUS_NO_FOLDER:        return proto::ReplicationStatusExt::NO_FOLDER;
        case ExtendedCompletionStatus::EXT_STATUS_NO_FILE:          return proto::ReplicationStatusExt::NO_FILE;
        case ExtendedCompletionStatus::EXT_STATUS_NO_ACCESS:        return proto::ReplicationStatusExt::NO_ACCESS;
        case ExtendedCompletionStatus::EXT_STATUS_NO_SPACE:         return proto::ReplicationStatusExt::NO_SPACE;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_MTIME:       return proto::ReplicationStatusExt::FILE_MTIME;
    }
    throw std::logic_error(
                    "Common::translate(ExtendedCompletionStatus) - unhandled status: " +
                    std::to_string(status));
}

////////////////////////////////////////////
//                Generators              //
////////////////////////////////////////////

// This macro to appear witin each block which requires thread safety
#define LOCK(MUTEX) std::lock_guard<util::Mutex> lock(MUTEX)

util::Mutex Generators::_mtx;

std::string Generators::uniqueId() {
    LOCK(_mtx);
    boost::uuids::uuid id = boost::uuids::random_generator()();
    return boost::uuids::to_string(id);
}

////////////////////////////////////////////
//        Parameters of requests          //
////////////////////////////////////////////

ReplicationRequestParams::ReplicationRequestParams()
    :   priority(0),
        database(),
        chunk(0),
        sourceWorker() {
}

ReplicationRequestParams::ReplicationRequestParams(proto::ReplicationRequestReplicate const& message)
    :   priority(message.priority()),
        database(message.database()),
        chunk(message.chunk()),
        sourceWorker(message.worker()) {
}

DeleteRequestParams::DeleteRequestParams()
    :   priority(0),
        database(),
        chunk(0) {
}
DeleteRequestParams::DeleteRequestParams(proto::ReplicationRequestDelete const& message)
    :   priority(message.priority()),
        database(message.database()),
        chunk(message.chunk()) {
}

FindRequestParams::FindRequestParams()
    :   priority(0),
        database(),
        chunk(0) {
}

FindRequestParams::FindRequestParams(proto::ReplicationRequestFind const& message)
    :   priority(message.priority()),
        database(message.database()),
        chunk(message.chunk()) {
}

FindAllRequestParams::FindAllRequestParams()
    :   priority(0),
        database() {
}
FindAllRequestParams::FindAllRequestParams(proto::ReplicationRequestFindAll const& message)
    :   priority(message.priority()),
        database(message.database()) {
}

}}} // namespace lsst::qserv::replica
