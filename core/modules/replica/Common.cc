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
#include "replica/Common.h"

// System headers
#include <stdexcept>

// Third party headers
#include "boost/uuid/uuid.hpp"
#include "boost/uuid/uuid_generators.hpp"
#include "boost/uuid/uuid_io.hpp"

using namespace std;

namespace lsst {
namespace qserv {
namespace replica {

string status2string(ExtendedCompletionStatus status) {
    switch (status) {
        case ExtendedCompletionStatus::EXT_STATUS_NONE:          return "EXT_STATUS_NONE";
        case ExtendedCompletionStatus::EXT_STATUS_INVALID_PARAM: return "EXT_STATUS_INVALID_PARAM";
        case ExtendedCompletionStatus::EXT_STATUS_INVALID_ID:    return "EXT_STATUS_INVALID_ID";
        case ExtendedCompletionStatus::EXT_STATUS_DUPLICATE:     return "EXT_STATUS_DUPLICATE";
        case ExtendedCompletionStatus::EXT_STATUS_FOLDER_STAT:   return "EXT_STATUS_FOLDER_STAT";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_STAT:     return "EXT_STATUS_FILE_STAT";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_SIZE:     return "EXT_STATUS_FILE_SIZE";
        case ExtendedCompletionStatus::EXT_STATUS_FOLDER_READ:   return "EXT_STATUS_FOLDER_READ";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_READ:     return "EXT_STATUS_FILE_READ";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_ROPEN:    return "EXT_STATUS_FILE_ROPEN";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_CREATE:   return "EXT_STATUS_FILE_CREATE";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_OPEN:     return "EXT_STATUS_FILE_OPEN";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_RESIZE:   return "EXT_STATUS_FILE_RESIZE";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_WRITE:    return "EXT_STATUS_FILE_WRITE";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_COPY:     return "EXT_STATUS_FILE_COPY";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_DELETE:   return "EXT_STATUS_FILE_DELETE";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_RENAME:   return "EXT_STATUS_FILE_RENAME";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_EXISTS:   return "EXT_STATUS_FILE_EXISTS";
        case ExtendedCompletionStatus::EXT_STATUS_SPACE_REQ:     return "EXT_STATUS_SPACE_REQ";
        case ExtendedCompletionStatus::EXT_STATUS_NO_FOLDER:     return "EXT_STATUS_NO_FOLDER";
        case ExtendedCompletionStatus::EXT_STATUS_NO_FILE:       return "EXT_STATUS_NO_FILE";
        case ExtendedCompletionStatus::EXT_STATUS_NO_ACCESS:     return "EXT_STATUS_NO_ACCESS";
        case ExtendedCompletionStatus::EXT_STATUS_NO_SPACE:      return "EXT_STATUS_NO_SPACE";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_MTIME:    return "EXT_STATUS_FILE_MTIME";
        case ExtendedCompletionStatus::EXT_STATUS_MYSQL_ERROR:   return "EXT_STATUS_MYSQL_ERROR";
        case ExtendedCompletionStatus::EXT_STATUS_LARGE_RESULT:  return "EXT_STATUS_LARGE_RESULT";
    }
    throw logic_error(
            "Common::" + string(__func__) + "(ExtendedCompletionStatus) - unhandled status: " +
            to_string(status));
}


ExtendedCompletionStatus translate(ProtocolStatusExt status) {
    switch (status) {
        case ProtocolStatusExt::NONE:          return ExtendedCompletionStatus::EXT_STATUS_NONE;
        case ProtocolStatusExt::INVALID_PARAM: return ExtendedCompletionStatus::EXT_STATUS_INVALID_PARAM;
        case ProtocolStatusExt::INVALID_ID:    return ExtendedCompletionStatus::EXT_STATUS_INVALID_ID;
        case ProtocolStatusExt::DUPLICATE:     return ExtendedCompletionStatus::EXT_STATUS_DUPLICATE;
        case ProtocolStatusExt::FOLDER_STAT:   return ExtendedCompletionStatus::EXT_STATUS_FOLDER_STAT;
        case ProtocolStatusExt::FILE_STAT:     return ExtendedCompletionStatus::EXT_STATUS_FILE_STAT;
        case ProtocolStatusExt::FILE_SIZE:     return ExtendedCompletionStatus::EXT_STATUS_FILE_SIZE;
        case ProtocolStatusExt::FOLDER_READ:   return ExtendedCompletionStatus::EXT_STATUS_FOLDER_READ;
        case ProtocolStatusExt::FILE_READ:     return ExtendedCompletionStatus::EXT_STATUS_FILE_READ;
        case ProtocolStatusExt::FILE_ROPEN:    return ExtendedCompletionStatus::EXT_STATUS_FILE_ROPEN;
        case ProtocolStatusExt::FILE_CREATE:   return ExtendedCompletionStatus::EXT_STATUS_FILE_CREATE;
        case ProtocolStatusExt::FILE_OPEN:     return ExtendedCompletionStatus::EXT_STATUS_FILE_OPEN;
        case ProtocolStatusExt::FILE_RESIZE:   return ExtendedCompletionStatus::EXT_STATUS_FILE_RESIZE;
        case ProtocolStatusExt::FILE_WRITE:    return ExtendedCompletionStatus::EXT_STATUS_FILE_WRITE;
        case ProtocolStatusExt::FILE_COPY:     return ExtendedCompletionStatus::EXT_STATUS_FILE_COPY;
        case ProtocolStatusExt::FILE_DELETE:   return ExtendedCompletionStatus::EXT_STATUS_FILE_DELETE;
        case ProtocolStatusExt::FILE_RENAME:   return ExtendedCompletionStatus::EXT_STATUS_FILE_RENAME;
        case ProtocolStatusExt::FILE_EXISTS:   return ExtendedCompletionStatus::EXT_STATUS_FILE_EXISTS;
        case ProtocolStatusExt::SPACE_REQ:     return ExtendedCompletionStatus::EXT_STATUS_SPACE_REQ;
        case ProtocolStatusExt::NO_FOLDER:     return ExtendedCompletionStatus::EXT_STATUS_NO_FOLDER;
        case ProtocolStatusExt::NO_FILE:       return ExtendedCompletionStatus::EXT_STATUS_NO_FILE;
        case ProtocolStatusExt::NO_ACCESS:     return ExtendedCompletionStatus::EXT_STATUS_NO_ACCESS;
        case ProtocolStatusExt::NO_SPACE:      return ExtendedCompletionStatus::EXT_STATUS_NO_SPACE;
        case ProtocolStatusExt::FILE_MTIME:    return ExtendedCompletionStatus::EXT_STATUS_FILE_MTIME;
        case ProtocolStatusExt::MYSQL_ERROR:   return ExtendedCompletionStatus::EXT_STATUS_MYSQL_ERROR;
        case ProtocolStatusExt::LARGE_RESULT:  return ExtendedCompletionStatus::EXT_STATUS_LARGE_RESULT;
    }
    throw logic_error(
                "Common::" + string(__func__) + "(ProtocolStatusExt) - unhandled status: " +
                to_string(status));
}


ProtocolStatusExt translate(ExtendedCompletionStatus status) {
    switch (status) {
        case ExtendedCompletionStatus::EXT_STATUS_NONE:          return ProtocolStatusExt::NONE;
        case ExtendedCompletionStatus::EXT_STATUS_INVALID_PARAM: return ProtocolStatusExt::INVALID_PARAM;
        case ExtendedCompletionStatus::EXT_STATUS_INVALID_ID:    return ProtocolStatusExt::INVALID_ID;
        case ExtendedCompletionStatus::EXT_STATUS_DUPLICATE:     return ProtocolStatusExt::DUPLICATE;
        case ExtendedCompletionStatus::EXT_STATUS_FOLDER_STAT:   return ProtocolStatusExt::FOLDER_STAT;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_STAT:     return ProtocolStatusExt::FILE_STAT;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_SIZE:     return ProtocolStatusExt::FILE_SIZE;
        case ExtendedCompletionStatus::EXT_STATUS_FOLDER_READ:   return ProtocolStatusExt::FOLDER_READ;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_READ:     return ProtocolStatusExt::FILE_READ;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_ROPEN:    return ProtocolStatusExt::FILE_ROPEN;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_CREATE:   return ProtocolStatusExt::FILE_CREATE;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_OPEN:     return ProtocolStatusExt::FILE_OPEN;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_RESIZE:   return ProtocolStatusExt::FILE_RESIZE;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_WRITE:    return ProtocolStatusExt::FILE_WRITE;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_COPY:     return ProtocolStatusExt::FILE_COPY;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_DELETE:   return ProtocolStatusExt::FILE_DELETE;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_RENAME:   return ProtocolStatusExt::FILE_RENAME;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_EXISTS:   return ProtocolStatusExt::FILE_EXISTS;
        case ExtendedCompletionStatus::EXT_STATUS_SPACE_REQ:     return ProtocolStatusExt::SPACE_REQ;
        case ExtendedCompletionStatus::EXT_STATUS_NO_FOLDER:     return ProtocolStatusExt::NO_FOLDER;
        case ExtendedCompletionStatus::EXT_STATUS_NO_FILE:       return ProtocolStatusExt::NO_FILE;
        case ExtendedCompletionStatus::EXT_STATUS_NO_ACCESS:     return ProtocolStatusExt::NO_ACCESS;
        case ExtendedCompletionStatus::EXT_STATUS_NO_SPACE:      return ProtocolStatusExt::NO_SPACE;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_MTIME:    return ProtocolStatusExt::FILE_MTIME;
        case ExtendedCompletionStatus::EXT_STATUS_MYSQL_ERROR:   return ProtocolStatusExt::MYSQL_ERROR;
        case ExtendedCompletionStatus::EXT_STATUS_LARGE_RESULT:  return ProtocolStatusExt::LARGE_RESULT;
    }
    throw logic_error(
                "Common::" + string(__func__) + "(ExtendedCompletionStatus) - unhandled status: " +
                to_string(status));
}


////////////////////////////////////////////
//                Generators              //
////////////////////////////////////////////

util::Mutex Generators::_mtx;

string Generators::uniqueId() {
    util::Lock lock(_mtx, "Generators::" + string(__func__));
    boost::uuids::uuid id = boost::uuids::random_generator()();
    return boost::uuids::to_string(id);
}


////////////////////////////////////////////
//        Parameters of requests          //
////////////////////////////////////////////

ReplicationRequestParams::ReplicationRequestParams(ProtocolRequestReplicate const& request)
    :   priority(request.priority()),
        database(request.database()),
        chunk(request.chunk()),
        sourceWorker(request.worker()) {
}


DeleteRequestParams::DeleteRequestParams(ProtocolRequestDelete const& request)
    :   priority(request.priority()),
        database(request.database()),
        chunk(request.chunk()) {
}


FindRequestParams::FindRequestParams(ProtocolRequestFind const& request)
    :   priority(request.priority()),
        database(request.database()),
        chunk(request.chunk()) {
}


FindAllRequestParams::FindAllRequestParams(ProtocolRequestFindAll const& request)
    :   priority(request.priority()),
        database(request.database()) {
}


EchoRequestParams::EchoRequestParams(ProtocolRequestEcho const& request)
    :   priority(request.priority()),
        data(request.data()),
        delay(request.delay()) {
}


SqlRequestParams::SqlRequestParams(ProtocolRequestSql const& request)
    :   priority(request.priority()),
        maxRows(request.max_rows()) {

    auto const requestType = request.type();
    switch (requestType) {
        case ProtocolRequestSql::QUERY:                     type = QUERY; break;
        case ProtocolRequestSql::CREATE_DATABASE:           type = CREATE_DATABASE; break;
        case ProtocolRequestSql::DROP_DATABASE:             type = DROP_DATABASE; break;
        case ProtocolRequestSql::ENABLE_DATABASE:           type = ENABLE_DATABASE; break;
        case ProtocolRequestSql::DISABLE_DATABASE:          type = DISABLE_DATABASE; break;
        case ProtocolRequestSql::CREATE_TABLE:              type = CREATE_TABLE; break;
        case ProtocolRequestSql::DROP_TABLE:                type = DROP_TABLE; break;
        case ProtocolRequestSql::REMOVE_TABLE_PARTITIONING: type = REMOVE_TABLE_PARTITIONING; break;
        default:
            throw runtime_error(
                    "SqlRequestParams::" + string(__func__) +
                    "  unsupported request type: " + ProtocolRequestSql_Type_Name(requestType));
    }
    if (request.has_query())    query    = request.query();
    if (request.has_user())     user     = request.user();
    if (request.has_password()) password = request.password();
    if (request.has_database()) database = request.database();
    if (request.has_table())    table    = request.table();
    if (request.has_engine())   engine   = request.engine();

    for (int index = 0; index < request.columns_size(); ++index) {
        auto const& column = request.columns(index);
        columns.emplace_back(column.name(), column.type());
    }
}

}}} // namespace lsst::qserv::replica
