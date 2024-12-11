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

SqlRequestType parseSqlRequestType(string const& str) {
    if (str == "QUERY") {
        return SqlRequestType::QUERY;
    } else if (str == "CREATE_DATABASE") {
        return SqlRequestType::CREATE_DATABASE;
    } else if (str == "DROP_DATABASE") {
        return SqlRequestType::DROP_DATABASE;
    } else if (str == "ENABLE_DATABASE") {
        return SqlRequestType::ENABLE_DATABASE;
    } else if (str == "DISABLE_DATABASE") {
        return SqlRequestType::DISABLE_DATABASE;
    } else if (str == "GRANT_ACCESS") {
        return SqlRequestType::GRANT_ACCESS;
    } else if (str == "CREATE_TABLE") {
        return SqlRequestType::CREATE_TABLE;
    } else if (str == "DROP_TABLE") {
        return SqlRequestType::DROP_TABLE;
    } else if (str == "REMOVE_TABLE_PARTITIONING") {
        return SqlRequestType::REMOVE_TABLE_PARTITIONING;
    } else if (str == "DROP_TABLE_PARTITION") {
        return SqlRequestType::DROP_TABLE_PARTITION;
    } else if (str == "GET_TABLE_INDEX") {
        return SqlRequestType::GET_TABLE_INDEX;
    } else if (str == "CREATE_TABLE_INDEX") {
        return SqlRequestType::CREATE_TABLE_INDEX;
    } else if (str == "DROP_TABLE_INDEX") {
        return SqlRequestType::DROP_TABLE_INDEX;
    } else if (str == "ALTER_TABLE") {
        return SqlRequestType::ALTER_TABLE;
    } else if (str == "TABLE_ROW_STATS") {
        return SqlRequestType::TABLE_ROW_STATS;
    }
    throw invalid_argument("Unknown SQL request type: '" + str + "'");
}

string toString(SqlRequestType status) {
    switch (status) {
        case SqlRequestType::QUERY:
            return "QUERY";
        case SqlRequestType::CREATE_DATABASE:
            return "CREATE_DATABASE";
        case SqlRequestType::DROP_DATABASE:
            return "DROP_DATABASE";
        case SqlRequestType::ENABLE_DATABASE:
            return "ENABLE_DATABASE";
        case SqlRequestType::DISABLE_DATABASE:
            return "DISABLE_DATABASE";
        case SqlRequestType::GRANT_ACCESS:
            return "GRANT_ACCESS";
        case SqlRequestType::CREATE_TABLE:
            return "CREATE_TABLE";
        case SqlRequestType::DROP_TABLE:
            return "DROP_TABLE";
        case SqlRequestType::REMOVE_TABLE_PARTITIONING:
            return "REMOVE_TABLE_PARTITIONING";
        case SqlRequestType::DROP_TABLE_PARTITION:
            return "DROP_TABLE_PARTITION";
        case SqlRequestType::GET_TABLE_INDEX:
            return "GET_TABLE_INDEX";
        case SqlRequestType::CREATE_TABLE_INDEX:
            return "CREATE_TABLE_INDEX";
        case SqlRequestType::DROP_TABLE_INDEX:
            return "DROP_TABLE_INDEX";
        case SqlRequestType::ALTER_TABLE:
            return "ALTER_TABLE";
        case SqlRequestType::TABLE_ROW_STATS:
            return "TABLE_ROW_STATS";
        default:
            throw logic_error("Unhandled SQL request type: " + to_string(static_cast<int>(status)));
    }
}

string toString(Status status) {
    switch (status) {
        case Status::CREATED:
            return "CREATED";
        case Status::SUCCESS:
            return "SUCCESS";
        case Status::QUEUED:
            return "QUEUED";
        case Status::IN_PROGRESS:
            return "IN_PROGRESS";
        case Status::IS_CANCELLING:
            return "IS_CANCELLING";
        case Status::BAD:
            return "BAD";
        case Status::FAILED:
            return "FAILED";
        case Status::CANCELLED:
            return "CANCELLED";
        default:
            throw logic_error("Unhandled status: " + to_string(static_cast<int>(status)));
    }
}

string toString(StatusExt extendedStatus) {
    switch (extendedStatus) {
        case StatusExt::NONE:
            return "NONE";
        case StatusExt::INVALID_PARAM:
            return "INVALID_PARAM";
        case StatusExt::INVALID_ID:
            return "INVALID_ID";
        case StatusExt::FOLDER_STAT:
            return "FOLDER_STAT";
        case StatusExt::FOLDER_CREATE:
            return "FOLDER_CREATE";
        case StatusExt::FILE_STAT:
            return "FILE_STAT";
        case StatusExt::FILE_SIZE:
            return "FILE_SIZE";
        case StatusExt::FOLDER_READ:
            return "FOLDER_READ";
        case StatusExt::FILE_READ:
            return "FILE_READ";
        case StatusExt::FILE_ROPEN:
            return "FILE_ROPEN";
        case StatusExt::FILE_CREATE:
            return "FILE_CREATE";
        case StatusExt::FILE_OPEN:
            return "FILE_OPEN";
        case StatusExt::FILE_RESIZE:
            return "FILE_RESIZE";
        case StatusExt::FILE_WRITE:
            return "FILE_WRITE";
        case StatusExt::FILE_COPY:
            return "FILE_COPY";
        case StatusExt::FILE_DELETE:
            return "FILE_DELETE";
        case StatusExt::FILE_RENAME:
            return "FILE_RENAME";
        case StatusExt::FILE_EXISTS:
            return "FILE_EXISTS";
        case StatusExt::SPACE_REQ:
            return "SPACE_REQ";
        case StatusExt::NO_FOLDER:
            return "NO_FOLDER";
        case StatusExt::NO_FILE:
            return "NO_FILE";
        case StatusExt::NO_ACCESS:
            return "NO_ACCESS";
        case StatusExt::NO_SPACE:
            return "NO_SPACE";
        case StatusExt::FILE_MTIME:
            return "FILE_MTIME";
        case StatusExt::MYSQL_ERROR:
            return "MYSQL_ERROR";
        case StatusExt::LARGE_RESULT:
            return "LARGE_RESULT";
        case StatusExt::NO_SUCH_TABLE:
            return "NO_SUCH_TABLE";
        case StatusExt::NOT_PARTITIONED_TABLE:
            return "NOT_PARTITIONED_TABLE";
        case StatusExt::NO_SUCH_PARTITION:
            return "NO_SUCH_PARTITION";
        case StatusExt::MULTIPLE:
            return "MULTIPLE";
        case StatusExt::OTHER_EXCEPTION:
            return "OTHER_EXCEPTION";
        case StatusExt::FOREIGN_INSTANCE:
            return "FOREIGN_INSTANCE";
        case StatusExt::DUPLICATE_KEY:
            return "DUPLICATE_KEY";
        case StatusExt::CANT_DROP_KEY:
            return "CANT_DROP_KEY";
        case StatusExt::CONFIG_NO_SUCH_DB:
            return "CONFIG_NO_SUCH_DB";
        case StatusExt::CONFIG_NO_SUCH_TABLE:
            return "CONFIG_NO_SUCH_TABLE";
        default:
            throw logic_error("Unhandled extended status: " + to_string(static_cast<int>(extendedStatus)));
    }
}

string toString(Status status, StatusExt extendedStatus) {
    return toString(status) + "::" + toString(extendedStatus);
}

string toString(ServiceState state) {
    switch (state) {
        case ServiceState::SUSPEND_IN_PROGRESS:
            return "SUSPEND_IN_PROGRESS";
        case ServiceState::SUSPENDED:
            return "SUSPENDED";
        case ServiceState::RUNNING:
            return "RUNNING";
        default:
            throw logic_error("Unhandled service state: " + to_string(static_cast<int>(state)));
    }
}

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
