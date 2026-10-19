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
#include "replica/proto/Protocol.h"

// System headers
#include <stdexcept>

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica::protocol {

SqlRequestType toSqlRequestType(string const& str) {
    using enum SqlRequestType;
    if (str == "QUERY") {
        return QUERY;
    } else if (str == "CREATE_DATABASE") {
        return CREATE_DATABASE;
    } else if (str == "DROP_DATABASE") {
        return DROP_DATABASE;
    } else if (str == "ENABLE_DATABASE") {
        return ENABLE_DATABASE;
    } else if (str == "DISABLE_DATABASE") {
        return DISABLE_DATABASE;
    } else if (str == "GRANT_ACCESS") {
        return GRANT_ACCESS;
    } else if (str == "CREATE_TABLE") {
        return CREATE_TABLE;
    } else if (str == "DROP_TABLE") {
        return DROP_TABLE;
    } else if (str == "REMOVE_TABLE_PARTITIONING") {
        return REMOVE_TABLE_PARTITIONING;
    } else if (str == "DROP_TABLE_PARTITION") {
        return DROP_TABLE_PARTITION;
    } else if (str == "GET_TABLE_INDEX") {
        return GET_TABLE_INDEX;
    } else if (str == "CREATE_TABLE_INDEX") {
        return CREATE_TABLE_INDEX;
    } else if (str == "DROP_TABLE_INDEX") {
        return DROP_TABLE_INDEX;
    } else if (str == "ALTER_TABLE") {
        return ALTER_TABLE;
    } else if (str == "TABLE_ROW_STATS") {
        return TABLE_ROW_STATS;
    }
    throw invalid_argument("Unknown SQL request type: '" + str + "'");
}

SqlRequestType toSqlRequestType(unsigned int value) {
    if (value < static_cast<unsigned int>(SqlRequestType::_FIRST) ||
        value > static_cast<unsigned int>(SqlRequestType::_LAST)) {
        throw invalid_argument("Unknown SQL request type value: " + to_string(value));
    }
    return static_cast<SqlRequestType>(value);
}

string toString(SqlRequestType status) {
    using enum SqlRequestType;
    switch (status) {
        case QUERY:
            return "QUERY";
        case CREATE_DATABASE:
            return "CREATE_DATABASE";
        case DROP_DATABASE:
            return "DROP_DATABASE";
        case ENABLE_DATABASE:
            return "ENABLE_DATABASE";
        case DISABLE_DATABASE:
            return "DISABLE_DATABASE";
        case GRANT_ACCESS:
            return "GRANT_ACCESS";
        case CREATE_TABLE:
            return "CREATE_TABLE";
        case DROP_TABLE:
            return "DROP_TABLE";
        case REMOVE_TABLE_PARTITIONING:
            return "REMOVE_TABLE_PARTITIONING";
        case DROP_TABLE_PARTITION:
            return "DROP_TABLE_PARTITION";
        case GET_TABLE_INDEX:
            return "GET_TABLE_INDEX";
        case CREATE_TABLE_INDEX:
            return "CREATE_TABLE_INDEX";
        case DROP_TABLE_INDEX:
            return "DROP_TABLE_INDEX";
        case ALTER_TABLE:
            return "ALTER_TABLE";
        case TABLE_ROW_STATS:
            return "TABLE_ROW_STATS";
        default:
            throw logic_error("Unhandled SQL request type: " + to_string(static_cast<int>(status)));
    }
}

unsigned int toUnsignedInt(SqlRequestType status) { return static_cast<unsigned int>(status); }

SqlIndexSpec toSqlIndexSpec(std::string const& str) {
    using enum SqlIndexSpec;
    if (str == "DEFAULT") {
        return DEFAULT;
    } else if (str == "UNIQUE") {
        return UNIQUE;
    } else if (str == "FULLTEXT") {
        return FULLTEXT;
    } else if (str == "SPATIAL") {
        return SPATIAL;
    }
    throw invalid_argument("Unknown SQL index specification: '" + str + "'");
}

SqlIndexSpec toSqlIndexSpec(unsigned int value) {
    if (value < static_cast<unsigned int>(SqlIndexSpec::_FIRST) ||
        value > static_cast<unsigned int>(SqlIndexSpec::_LAST)) {
        throw invalid_argument("Unknown SQL index specification value: " + to_string(value));
    }
    return static_cast<SqlIndexSpec>(value);
}

std::string toString(SqlIndexSpec spec) {
    using enum SqlIndexSpec;
    switch (spec) {
        case DEFAULT:
            return "DEFAULT";
        case UNIQUE:
            return "UNIQUE";
        case FULLTEXT:
            return "FULLTEXT";
        case SPATIAL:
            return "SPATIAL";
        default:
            throw logic_error("Unhandled SQL index specification: " + to_string(static_cast<int>(spec)));
    }
}

unsigned int toUnsignedInt(SqlIndexSpec spec) { return static_cast<unsigned int>(spec); }

Status toStatus(std::string const& statusStr) {
    using enum Status;
    if (statusStr == "CREATED") return CREATED;
    if (statusStr == "SUCCESS") return SUCCESS;
    if (statusStr == "QUEUED") return QUEUED;
    if (statusStr == "IN_PROGRESS") return IN_PROGRESS;
    if (statusStr == "IS_CANCELLING") return IS_CANCELLING;
    if (statusStr == "BAD") return BAD;
    if (statusStr == "FAILED") return FAILED;
    if (statusStr == "CANCELLED") return CANCELLED;
    throw invalid_argument("Unknown status string: " + statusStr);
}

Status toStatus(unsigned int value) {
    if (value < static_cast<unsigned int>(Status::_FIRST) ||
        value > static_cast<unsigned int>(Status::_LAST)) {
        throw invalid_argument("Unknown status value: " + to_string(value));
    }
    return static_cast<Status>(value);
}

string toString(Status status) {
    using enum Status;
    switch (status) {
        case CREATED:
            return "CREATED";
        case SUCCESS:
            return "SUCCESS";
        case QUEUED:
            return "QUEUED";
        case IN_PROGRESS:
            return "IN_PROGRESS";
        case IS_CANCELLING:
            return "IS_CANCELLING";
        case BAD:
            return "BAD";
        case FAILED:
            return "FAILED";
        case CANCELLED:
            return "CANCELLED";
        default:
            throw logic_error("Unhandled status: " + to_string(static_cast<int>(status)));
    }
}

unsigned int toUnsignedInt(Status status) { return static_cast<unsigned int>(status); }

StatusExt toStatusExt(string const& str) {
    using enum StatusExt;
    if (str == "NONE") return NONE;
    if (str == "INVALID_PARAM") return INVALID_PARAM;
    if (str == "INVALID_ID") return INVALID_ID;
    if (str == "FOLDER_STAT") return FOLDER_STAT;
    if (str == "FOLDER_CREATE") return FOLDER_CREATE;
    if (str == "FILE_STAT") return FILE_STAT;
    if (str == "FILE_SIZE") return FILE_SIZE;
    if (str == "FOLDER_READ") return FOLDER_READ;
    if (str == "FILE_READ") return FILE_READ;
    if (str == "FILE_ROPEN") return FILE_ROPEN;
    if (str == "FILE_CREATE") return FILE_CREATE;
    if (str == "FILE_OPEN") return FILE_OPEN;
    if (str == "FILE_RESIZE") return FILE_RESIZE;
    if (str == "FILE_WRITE") return FILE_WRITE;
    if (str == "FILE_COPY") return FILE_COPY;
    if (str == "FILE_DELETE") return FILE_DELETE;
    if (str == "FILE_RENAME") return FILE_RENAME;
    if (str == "FILE_EXISTS") return FILE_EXISTS;
    if (str == "SPACE_REQ") return SPACE_REQ;
    if (str == "NO_FOLDER") return NO_FOLDER;
    if (str == "NO_FILE") return NO_FILE;
    if (str == "NO_ACCESS") return NO_ACCESS;
    if (str == "NO_SPACE") return NO_SPACE;
    if (str == "FILE_MTIME") return FILE_MTIME;
    if (str == "MYSQL_ERROR") return MYSQL_ERROR;
    if (str == "LARGE_RESULT") return LARGE_RESULT;
    if (str == "NO_SUCH_TABLE") return NO_SUCH_TABLE;
    if (str == "NOT_PARTITIONED_TABLE") return NOT_PARTITIONED_TABLE;
    if (str == "NO_SUCH_PARTITION") return NO_SUCH_PARTITION;
    if (str == "MULTIPLE") return MULTIPLE;
    if (str == "OTHER_EXCEPTION") return OTHER_EXCEPTION;
    if (str == "FOREIGN_INSTANCE") return FOREIGN_INSTANCE;
    if (str == "DUPLICATE_KEY") return DUPLICATE_KEY;
    if (str == "CANT_DROP_KEY") return CANT_DROP_KEY;
    if (str == "CONFIG_NO_SUCH_DB") return CONFIG_NO_SUCH_DB;
    if (str == "CONFIG_NO_SUCH_TABLE") return CONFIG_NO_SUCH_TABLE;
    throw invalid_argument("Unknown extended status string: " + str);
}

StatusExt toStatusExt(unsigned int value) {
    if (value < static_cast<unsigned int>(StatusExt::_FIRST) ||
        value > static_cast<unsigned int>(StatusExt::_LAST)) {
        throw invalid_argument("Unknown extended status value: " + to_string(value));
    }
    return static_cast<StatusExt>(value);
}

string toString(StatusExt extendedStatus) {
    using enum StatusExt;
    switch (extendedStatus) {
        case NONE:
            return "NONE";
        case INVALID_PARAM:
            return "INVALID_PARAM";
        case INVALID_ID:
            return "INVALID_ID";
        case FOLDER_STAT:
            return "FOLDER_STAT";
        case FOLDER_CREATE:
            return "FOLDER_CREATE";
        case FILE_STAT:
            return "FILE_STAT";
        case FILE_SIZE:
            return "FILE_SIZE";
        case FOLDER_READ:
            return "FOLDER_READ";
        case FILE_READ:
            return "FILE_READ";
        case FILE_ROPEN:
            return "FILE_ROPEN";
        case FILE_CREATE:
            return "FILE_CREATE";
        case FILE_OPEN:
            return "FILE_OPEN";
        case FILE_RESIZE:
            return "FILE_RESIZE";
        case FILE_WRITE:
            return "FILE_WRITE";
        case FILE_COPY:
            return "FILE_COPY";
        case FILE_DELETE:
            return "FILE_DELETE";
        case FILE_RENAME:
            return "FILE_RENAME";
        case FILE_EXISTS:
            return "FILE_EXISTS";
        case SPACE_REQ:
            return "SPACE_REQ";
        case NO_FOLDER:
            return "NO_FOLDER";
        case NO_FILE:
            return "NO_FILE";
        case NO_ACCESS:
            return "NO_ACCESS";
        case NO_SPACE:
            return "NO_SPACE";
        case FILE_MTIME:
            return "FILE_MTIME";
        case MYSQL_ERROR:
            return "MYSQL_ERROR";
        case LARGE_RESULT:
            return "LARGE_RESULT";
        case NO_SUCH_TABLE:
            return "NO_SUCH_TABLE";
        case NOT_PARTITIONED_TABLE:
            return "NOT_PARTITIONED_TABLE";
        case NO_SUCH_PARTITION:
            return "NO_SUCH_PARTITION";
        case MULTIPLE:
            return "MULTIPLE";
        case OTHER_EXCEPTION:
            return "OTHER_EXCEPTION";
        case FOREIGN_INSTANCE:
            return "FOREIGN_INSTANCE";
        case DUPLICATE_KEY:
            return "DUPLICATE_KEY";
        case CANT_DROP_KEY:
            return "CANT_DROP_KEY";
        case CONFIG_NO_SUCH_DB:
            return "CONFIG_NO_SUCH_DB";
        case CONFIG_NO_SUCH_TABLE:
            return "CONFIG_NO_SUCH_TABLE";
        default:
            throw invalid_argument("Unknown extended status enum value: " +
                                   to_string(static_cast<unsigned int>(extendedStatus)));
    }
}

unsigned int toUnsignedInt(StatusExt extendedStatus) { return static_cast<unsigned int>(extendedStatus); }

std::string toString(Status status, StatusExt extendedStatus) {
    return toString(status) + "::" + toString(extendedStatus);
}

ServiceState toServiceState(string const& stateStr) {
    using enum ServiceState;
    if (stateStr == "SUSPEND_IN_PROGRESS") return SUSPEND_IN_PROGRESS;
    if (stateStr == "SUSPENDED") return SUSPENDED;
    if (stateStr == "RUNNING") return RUNNING;
    throw invalid_argument("Unknown service state string: " + stateStr);
}

ServiceState toServiceState(unsigned int value) {
    if (value < static_cast<unsigned int>(ServiceState::_FIRST) ||
        value > static_cast<unsigned int>(ServiceState::_LAST)) {
        throw invalid_argument("Unknown service state value: " + to_string(value));
    }
    return static_cast<ServiceState>(value);
}

string toString(ServiceState state) {
    using enum ServiceState;
    switch (state) {
        case SUSPEND_IN_PROGRESS:
            return "SUSPEND_IN_PROGRESS";
        case SUSPENDED:
            return "SUSPENDED";
        case RUNNING:
            return "RUNNING";
        default:
            throw logic_error("Unhandled service state: " + to_string(static_cast<int>(state)));
    }
}

unsigned int toUnsignedInt(ServiceState state) { return static_cast<unsigned int>(state); }

#define _THROW_INVALID_PARAM(name, msg)                                                                    \
    throw invalid_argument("protocol::RequestParams::" + string(__func__) + " parameter '" + name + "' " + \
                           string(msg))

RequestParams::RequestParams(json const& req) : _req(req) {}

bool RequestParams::has(string const& name) const { return _req.find(name) != _req.end(); }

string RequestParams::requiredString(string const& name) const {
    json const& param = _required(name);
    if (param.is_string()) return param.get<string>();
    _THROW_INVALID_PARAM(name, "does not have a string value");
}

string RequestParams::optionalString(string const& name, string const& defaultValue) const {
    return has(name) ? requiredString(name) : defaultValue;
}

bool RequestParams::requiredBool(string const& name) const {
    json const& param = _required(name);
    if (param.is_boolean())
        return param.get<bool>();
    else if (param.is_number_integer())
        return param.get<int64_t>() != 0;
    else if (param.is_number_unsigned())
        return param.get<uint64_t>() != 0;
    _THROW_INVALID_PARAM(name, "does not have a boolean value");
}

bool RequestParams::optionalBool(string const& name, bool defaultValue) const {
    return has(name) ? requiredBool(name) : defaultValue;
}

uint16_t RequestParams::requiredUInt16(string const& name) const {
    json const& param = _required(name);
    if (param.is_number_unsigned()) {
        uint64_t const val = param.get<uint64_t>();
        if (val <= std::numeric_limits<uint16_t>::max()) return static_cast<uint16_t>(val);
    }
    _THROW_INVALID_PARAM(name, "does not have an unsigned uint16_t value");
}

uint16_t RequestParams::optionalUInt16(string const& name, uint16_t defaultValue) const {
    return has(name) ? requiredUInt16(name) : defaultValue;
}

uint32_t RequestParams::requiredUInt32(string const& name) const {
    json const& param = _required(name);
    if (param.is_number_unsigned()) {
        uint64_t const val = param.get<uint64_t>();
        if (val <= std::numeric_limits<uint32_t>::max()) return static_cast<uint32_t>(val);
    }
    _THROW_INVALID_PARAM(name, "does not have an unsigned uint32_t value");
}

uint32_t RequestParams::optionalUInt32(string const& name, uint32_t defaultValue) const {
    return has(name) ? requiredUInt32(name) : defaultValue;
}

int32_t RequestParams::requiredInt32(string const& name) const {
    json const& param = _required(name);
    if (param.is_number_integer()) {
        int64_t const val = param.get<int64_t>();
        if (val >= std::numeric_limits<int32_t>::min() && val <= std::numeric_limits<int32_t>::max())
            return static_cast<int32_t>(val);
    }
    _THROW_INVALID_PARAM(name, "does not have an int32_t value");
}

int32_t RequestParams::optionalInt32(string const& name, int32_t defaultValue) const {
    return has(name) ? requiredInt32(name) : defaultValue;
}

uint64_t RequestParams::requiredUInt64(string const& name) const {
    json const& param = _required(name);
    if (param.is_number_unsigned()) return param.get<uint64_t>();
    _THROW_INVALID_PARAM(name, "does not have an unsigned 64-bit integer value");
}

uint64_t RequestParams::optionalUInt64(string const& name, uint64_t defaultValue) const {
    return has(name) ? requiredUInt64(name) : defaultValue;
}

double RequestParams::requiredDouble(string const& name) const {
    json const& param = _required(name);
    if (param.is_number_float()) return param.get<double>();
    _THROW_INVALID_PARAM(name, "does not have a double value");
}

double RequestParams::optionalDouble(string const& name, double defaultValue) const {
    return has(name) ? requiredDouble(name) : defaultValue;
}

vector<string> RequestParams::requiredStringVec(string const& name) const {
    json const& param = _requiredVec(name);
    vector<string> result;
    for (auto const& item : param) {
        if (!item.is_string()) {
            _THROW_INVALID_PARAM(name, "does not have an array of string values");
        }
        result.push_back(item.get<string>());
    }
    return result;
}

vector<string> RequestParams::optionalStringVec(string const& name,
                                                vector<string> const& defaultValue) const {
    return has(name) ? requiredStringVec(name) : defaultValue;
}

vector<uint64_t> RequestParams::requiredUInt64Vec(string const& name) const {
    json const& param = _requiredVec(name);
    vector<uint64_t> result;
    for (auto const& item : param) {
        if (!item.is_number_unsigned()) {
            _THROW_INVALID_PARAM(name, "does not have an array of unsigned 64-bit integer values");
        }
        result.push_back(item.get<uint64_t>());
    }
    return result;
}

vector<uint64_t> RequestParams::optionalUInt64Vec(string const& name,
                                                  vector<uint64_t> const& defaultValue) const {
    return has(name) ? requiredUInt64Vec(name) : defaultValue;
}

json const& RequestParams::requiredVec(string const& name) const { return _requiredVec(name); }

json const& RequestParams::requiredObj(string const& name) const { return _requiredObj(name); }

json const& RequestParams::_required(string const& name) const {
    auto const itr = _req.find(name);
    if (itr == _req.end()) {
        _THROW_INVALID_PARAM(name, "is not found in the request");
    }
    return *itr;
}

json const& RequestParams::_requiredVec(string const& name) const {
    json const& param = _required(name);
    if (param.is_array()) return param;
    _THROW_INVALID_PARAM(name, "does not have the JSON array value");
}

json const& RequestParams::_requiredObj(string const& name) const {
    json const& param = _required(name);
    if (param.is_object()) return param;
    _THROW_INVALID_PARAM(name, "does not have the JSON object value");
}

}  // namespace lsst::qserv::replica::protocol
