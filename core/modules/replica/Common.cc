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
#include "nlohmann/json.hpp"

using namespace std;
using namespace nlohmann;

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
        case ExtendedCompletionStatus::EXT_STATUS_FOLDER_CREATE: return "EXT_STATUS_FOLDER_CREATE";
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
        case ExtendedCompletionStatus::EXT_STATUS_NO_SUCH_TABLE: return "EXT_STATUS_NO_SUCH_TABLE";
        case ExtendedCompletionStatus::EXT_STATUS_NOT_PARTITIONED_TABLE: return "EXT_STATUS_NOT_PARTITIONED_TABLE";
        case ExtendedCompletionStatus::EXT_STATUS_NO_SUCH_PARTITION:     return "EXT_STATUS_NO_SUCH_PARTITION";
        case ExtendedCompletionStatus::EXT_STATUS_MULTIPLE:              return "EXT_STATUS_MULTIPLE";
        case ExtendedCompletionStatus::EXT_STATUS_OTHER_EXCEPTION:       return "EXT_STATUS_OTHER_EXCEPTION";
        case ExtendedCompletionStatus::EXT_STATUS_FOREIGN_INSTANCE:      return "EXT_STATUS_FOREIGN_INSTANCE";
        case ExtendedCompletionStatus::EXT_STATUS_DUPLICATE_KEY: return "EXT_STATUS_DUPLICATE_KEY";
        case ExtendedCompletionStatus::EXT_STATUS_CANT_DROP_KEY: return "EXT_STATUS_CANT_DROP_KEY";
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
        case ProtocolStatusExt::FOLDER_CREATE: return ExtendedCompletionStatus::EXT_STATUS_FOLDER_CREATE;
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
        case ProtocolStatusExt::NO_SUCH_TABLE: return ExtendedCompletionStatus::EXT_STATUS_NO_SUCH_TABLE;
        case ProtocolStatusExt::NOT_PARTITIONED_TABLE: return ExtendedCompletionStatus::EXT_STATUS_NOT_PARTITIONED_TABLE;
        case ProtocolStatusExt::NO_SUCH_PARTITION:     return ExtendedCompletionStatus::EXT_STATUS_NO_SUCH_PARTITION;
        case ProtocolStatusExt::MULTIPLE:              return ExtendedCompletionStatus::EXT_STATUS_MULTIPLE;
        case ProtocolStatusExt::OTHER_EXCEPTION:       return ExtendedCompletionStatus::EXT_STATUS_OTHER_EXCEPTION;
        case ProtocolStatusExt::FOREIGN_INSTANCE:      return ExtendedCompletionStatus::EXT_STATUS_FOREIGN_INSTANCE;
        case ProtocolStatusExt::DUPLICATE_KEY: return ExtendedCompletionStatus::EXT_STATUS_DUPLICATE_KEY;
        case ProtocolStatusExt::CANT_DROP_KEY: return ExtendedCompletionStatus::EXT_STATUS_CANT_DROP_KEY;
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
        case ExtendedCompletionStatus::EXT_STATUS_FOLDER_CREATE: return ProtocolStatusExt::FOLDER_CREATE;
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
        case ExtendedCompletionStatus::EXT_STATUS_NO_SUCH_TABLE: return ProtocolStatusExt::NO_SUCH_TABLE;
        case ExtendedCompletionStatus::EXT_STATUS_NOT_PARTITIONED_TABLE: return ProtocolStatusExt::NOT_PARTITIONED_TABLE;
        case ExtendedCompletionStatus::EXT_STATUS_NO_SUCH_PARTITION:     return ProtocolStatusExt::NO_SUCH_PARTITION;
        case ExtendedCompletionStatus::EXT_STATUS_MULTIPLE:              return ProtocolStatusExt::MULTIPLE;
        case ExtendedCompletionStatus::EXT_STATUS_OTHER_EXCEPTION:       return ProtocolStatusExt::OTHER_EXCEPTION;
        case ExtendedCompletionStatus::EXT_STATUS_FOREIGN_INSTANCE:      return ProtocolStatusExt::FOREIGN_INSTANCE;
        case ExtendedCompletionStatus::EXT_STATUS_DUPLICATE_KEY: return ProtocolStatusExt::DUPLICATE_KEY;
        case ExtendedCompletionStatus::EXT_STATUS_CANT_DROP_KEY: return ProtocolStatusExt::CANT_DROP_KEY;
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
    :   database(request.database()),
        chunk(request.chunk()),
        sourceWorker(request.worker()) {
}


DeleteRequestParams::DeleteRequestParams(ProtocolRequestDelete const& request)
    :   database(request.database()),
        chunk(request.chunk()) {
}


FindRequestParams::FindRequestParams(ProtocolRequestFind const& request)
    :   database(request.database()),
        chunk(request.chunk()) {
}


FindAllRequestParams::FindAllRequestParams(ProtocolRequestFindAll const& request)
    :   database(request.database()) {
}


EchoRequestParams::EchoRequestParams(ProtocolRequestEcho const& request)
    :   data(request.data()),
        delay(request.delay()) {
}


SqlRequestParams::SqlRequestParams(ProtocolRequestSql const& request)
    :   maxRows(request.max_rows()) {

    auto const requestType = request.type();
    switch (requestType) {
        case ProtocolRequestSql::QUERY:                     type = QUERY; break;
        case ProtocolRequestSql::CREATE_DATABASE:           type = CREATE_DATABASE; break;
        case ProtocolRequestSql::DROP_DATABASE:             type = DROP_DATABASE; break;
        case ProtocolRequestSql::ENABLE_DATABASE:           type = ENABLE_DATABASE; break;
        case ProtocolRequestSql::DISABLE_DATABASE:          type = DISABLE_DATABASE; break;
        case ProtocolRequestSql::GRANT_ACCESS:              type = GRANT_ACCESS; break;
        case ProtocolRequestSql::CREATE_TABLE:              type = CREATE_TABLE; break;
        case ProtocolRequestSql::DROP_TABLE:                type = DROP_TABLE; break;
        case ProtocolRequestSql::REMOVE_TABLE_PARTITIONING: type = REMOVE_TABLE_PARTITIONING; break;
        case ProtocolRequestSql::DROP_TABLE_PARTITION:      type = DROP_TABLE_PARTITION; break;
        case ProtocolRequestSql::GET_TABLE_INDEX:           type = GET_TABLE_INDEX; break;
        case ProtocolRequestSql::CREATE_TABLE_INDEX:        type = CREATE_TABLE_INDEX; break;
        case ProtocolRequestSql::DROP_TABLE_INDEX:          type = DROP_TABLE_INDEX; break;
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
    if (request.has_partition_by_column()) partitionByColumn = request.partition_by_column();
    if (request.has_transaction_id())      transactionId     = request.transaction_id();

    for (int index = 0; index < request.columns_size(); ++index) {
        auto const& column = request.columns(index);
        columns.emplace_back(column.name(), column.type());
    }
    for (int index = 0; index < request.tables_size(); ++index) {
        tables.push_back(request.tables(index));
    }
    if (request.has_batch_mode()) batchMode = request.batch_mode();
    if (request.has_index_spec()) indexSpec = IndexSpec(request.index_spec());
    if (request.has_index_name()) indexName = request.index_name();
    if (request.has_index_comment()) indexComment = request.index_comment();

    for (int i = 0; i < request.index_columns_size(); ++i) {
        auto const& column = request.index_columns(i);
        indexColumns.emplace_back(column.name(), column.length(), column.ascending());
    }

}


string SqlRequestParams::type2str() const {
    switch (type) {
        case QUERY:                     return "QUERY";
        case CREATE_DATABASE:           return "CREATE_DATABASE";
        case DROP_DATABASE:             return "DROP_DATABASE";
        case ENABLE_DATABASE:           return "ENABLE_DATABASE";
        case DISABLE_DATABASE:          return "DISABLE_DATABASE";
        case GRANT_ACCESS:              return "GRANT_ACCESS";
        case CREATE_TABLE:              return "CREATE_TABLE";
        case DROP_TABLE:                return "DROP_TABLE";
        case REMOVE_TABLE_PARTITIONING: return "REMOVE_TABLE_PARTITIONING";
        case DROP_TABLE_PARTITION:      return "DROP_TABLE_PARTITION";
        case GET_TABLE_INDEX:           return "GET_TABLE_INDEX";
        case CREATE_TABLE_INDEX:        return "CREATE_TABLE_INDEX";
        case DROP_TABLE_INDEX:          return "DROP_TABLE_INDEX";
    }
    throw runtime_error(
            "SqlRequestParams::" + string(__func__) + "  unsupported request type");
}


SqlRequestParams::IndexSpec::IndexSpec(ProtocolRequestSql::IndexSpec spec) {
    switch (spec) {
        case ProtocolRequestSql::DEFAULT:  { _spec = IndexSpec::DEFAULT;  break; }
        case ProtocolRequestSql::UNIQUE:   { _spec = IndexSpec::UNIQUE;   break; }
        case ProtocolRequestSql::FULLTEXT: { _spec = IndexSpec::FULLTEXT; break; }
        case ProtocolRequestSql::SPATIAL:  { _spec = IndexSpec::SPATIAL;  break; }
        default:
            throw invalid_argument(
                    "SqlRequestParams::IndexSpec::" + string(__func__) +
                    "  unsupported protocol index specification: '"
                    + ProtocolRequestSql_IndexSpec_Name(spec) + "'");
    }
}


SqlRequestParams::IndexSpec::IndexSpec(string const& str) {
    if      (str == "DEFAULT")  { _spec = IndexSpec::DEFAULT;  }
    else if (str == "UNIQUE")   { _spec = IndexSpec::UNIQUE;   }
    else if (str == "FULLTEXT") { _spec = IndexSpec::FULLTEXT; }
    else if (str == "SPATIAL")  { _spec = IndexSpec::SPATIAL;  }
    else {
        throw invalid_argument(
                "SqlRequestParams::IndexSpec::" + string(__func__)
                + "  unsupported index specification: '" + str + "'");
    }
}


string SqlRequestParams::IndexSpec::str() const {
    switch (_spec) {
        case IndexSpec::DEFAULT:  return "DEFAULT";
        case IndexSpec::UNIQUE:   return "UNIQUE";
        case IndexSpec::FULLTEXT: return "FULLTEXT";
        case IndexSpec::SPATIAL:  return "SPATIAL";
    }
    throw runtime_error(
            "SqlRequestParams::IndexSpec::" + string(__func__)
            + "  unsupported index specification");
}


ProtocolRequestSql::IndexSpec SqlRequestParams::IndexSpec::protocol() const {
    switch (_spec) {
        case IndexSpec::DEFAULT:  return ProtocolRequestSql::DEFAULT;
        case IndexSpec::UNIQUE:   return ProtocolRequestSql::UNIQUE;
        case IndexSpec::FULLTEXT: return ProtocolRequestSql::FULLTEXT;
        case IndexSpec::SPATIAL:  return ProtocolRequestSql::SPATIAL;
    }
    throw runtime_error(
            "SqlRequestParams::IndexSpec::" + string(__func__) +
            "  unsupported index specification: '" + str() + "'");
}


ostream& operator<<(ostream& os, SqlRequestParams const& params) {

    // Make the output to look like a serialized JSON object to allow parsing
    // log files using standard tools.

    json objParams;
    objParams["type"] = params.type2str();
    objParams["maxRows"] = params.maxRows;
    objParams["query"] = params.query;
    objParams["user"] = params.user;
    objParams["password"] = "******";
    objParams["database"] = params.database;
    objParams["table"] = params.table;
    objParams["engine"] = params.engine;
    objParams["partitionByColumn"] = params.partitionByColumn;
    objParams["transactionId"] = params.transactionId;
    objParams["batchMode"] = params.batchMode ? 1 : 0;

    json objParamsColumns = json::array();
    for (auto&& coldef: params.columns) {
        json objColDef;
        objColDef["name"] = coldef.name;
        objColDef["type"] = coldef.type;
        objParamsColumns.push_back(objColDef);
    }
    objParams["columns"] = objParamsColumns;

    json objParamsTables = json::array();
    for (auto&& table: params.tables) {
        objParamsTables.push_back(table);
    }
    objParams["tables"] = objParamsTables;
    objParams["index_spec"] = params.indexSpec.str();
    objParams["index_name"] = params.indexName;
    objParams["index_comment"] = params.indexComment;

    json objParamsIndexColumns = json::array();
    for (auto&& column: params.indexColumns) {
        json objColumn;
        objColumn["name"] = column.name;
        objColumn["length"] = column.length;
        objColumn["ascending"] = column.ascending ? 1 : 0;
        objParamsIndexColumns.push_back(objColumn);
    }
    objParams["index_columns"] = objParamsIndexColumns;

    json obj;
    obj["SqlRequestParams"] = objParams;

    os << obj.dump();
    return os;
}


IndexRequestParams::IndexRequestParams(ProtocolRequestIndex const& request)
    :   database(request.database()),
        chunk(request.chunk()),
        hasTransactions(request.has_transactions()),
        transactionId(request.transaction_id()) {
}

}}} // namespace lsst::qserv::replica
