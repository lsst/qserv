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
#include "replica/SqlResultSet.h"

// System headers
#include <algorithm>
#include <stdexcept>

// Third party headers
#include <mysql/mysql.h>

using namespace std;
using json = nlohmann::json;

namespace {

/**
 * Add a field to a JSON object
 */
template <typename T>
void addField(json& fieldsJson,
              char const* name,
              T const& value,
              char const* description) {
    fieldsJson[name]["value"] = value;
    fieldsJson[name]["description"] = description;
}
}  // namespace

namespace lsst {
namespace qserv {
namespace replica {

string SqlResultSet::ResultSet::Field::type2string() const {
    
    switch (type) {
        case MYSQL_TYPE_DECIMAL:     return "MYSQL_TYPE_DECIMAL";
        case MYSQL_TYPE_TINY:        return "MYSQL_TYPE_TINY";
        case MYSQL_TYPE_SHORT:       return "MYSQL_TYPE_SHORT";
        case MYSQL_TYPE_LONG:        return "MYSQL_TYPE_LONG";
        case MYSQL_TYPE_FLOAT:       return "MYSQL_TYPE_FLOAT";
        case MYSQL_TYPE_DOUBLE:      return "MYSQL_TYPE_DOUBLE";
        case MYSQL_TYPE_NULL:        return "MYSQL_TYPE_NULL";
        case MYSQL_TYPE_TIMESTAMP:   return "MYSQL_TYPE_TIMESTAMP";
        case MYSQL_TYPE_LONGLONG:    return "MYSQL_TYPE_LONGLONG";
        case MYSQL_TYPE_INT24:       return "MYSQL_TYPE_INT24";
        case MYSQL_TYPE_DATE:        return "MYSQL_TYPE_DATE";
        case MYSQL_TYPE_TIME:        return "MYSQL_TYPE_TIME";
        case MYSQL_TYPE_DATETIME:    return "MYSQL_TYPE_DATETIME";
        case MYSQL_TYPE_YEAR:        return "MYSQL_TYPE_YEAR";
        case MYSQL_TYPE_NEWDATE:     return "MYSQL_TYPE_NEWDATE";
        case MYSQL_TYPE_VARCHAR:     return "MYSQL_TYPE_VARCHAR";
        case MYSQL_TYPE_BIT:         return "MYSQL_TYPE_BIT";
        case MYSQL_TYPE_TIMESTAMP2:  return "MYSQL_TYPE_TIMESTAMP2";
        case MYSQL_TYPE_DATETIME2:   return "MYSQL_TYPE_DATETIME2";
        case MYSQL_TYPE_TIME2:       return "MYSQL_TYPE_TIME2";
        case MYSQL_TYPE_JSON:        return "MYSQL_TYPE_JSON";
        case MYSQL_TYPE_NEWDECIMAL:  return "MYSQL_TYPE_NEWDECIMAL";
        case MYSQL_TYPE_ENUM:        return "MYSQL_TYPE_ENUM";
        case MYSQL_TYPE_SET:         return "MYSQL_TYPE_SET";
        case MYSQL_TYPE_TINY_BLOB:   return "MYSQL_TYPE_TINY_BLOB";
        case MYSQL_TYPE_MEDIUM_BLOB: return "MYSQL_TYPE_MEDIUM_BLOB";
        case MYSQL_TYPE_LONG_BLOB:   return "MYSQL_TYPE_LONG_BLOB";
        case MYSQL_TYPE_BLOB:        return "MYSQL_TYPE_BLOB";
        case MYSQL_TYPE_VAR_STRING:  return "MYSQL_TYPE_VAR_STRING";
        case MYSQL_TYPE_STRING:      return "MYSQL_TYPE_STRING";
        case MYSQL_TYPE_GEOMETRY:    return "MYSQL_TYPE_GEOMETRY";
        default:
            return "MYSQL_TYPE_UNKNOWN";
    }
}


SqlResultSet::ResultSet::Field::Field(ProtocolResponseSqlField const& field)
    :   name(     field.name()),
        orgName(  field.org_name()),
        table(    field.table()),
        orgTable( field.org_table()),
        db(       field.db()),
        catalog(  field.catalog()),
        def(      field.def()),
        length(   field.length()),
        maxLength(field.max_length()),
        flags(    field.flags()),
        decimals( field.decimals()),
        type(     field.type()) {
}


SqlResultSet::ResultSet::Row::Row(ProtocolResponseSqlRow const& row) {
    for (int i = 0; i < row.cells_size(); ++i) {
        cells.push_back(row.cells(i));
        nulls.push_back(row.nulls(i) ? 1 : 0);
    }
}


SqlResultSet::ResultSet::ResultSet(ProtocolResponseSqlResultSet const& result) {
    extendedStatus = result.status_ext();
    error = result.error();
    charSetName = result.char_set_name();
    hasResult = result.has_result();
    for (int i = 0; i < result.fields_size(); ++i) {
        fields.emplace_back(result.fields(i));
    }
    for (int i = 0; i < result.rows_size(); ++i) {
        rows.emplace_back(result.rows(i));
    }
}


json SqlResultSet::ResultSet::toJson() const {

    json resultJson = json::object();

    resultJson["extended_status"] = status2string(extendedStatus);
    resultJson["error"] = error;
    resultJson["char_set_name"] = charSetName;
    resultJson["has_result"] = hasResult;

    for (size_t columnIdx = 0; columnIdx < fields.size(); ++columnIdx) {
        auto const& field = fields[columnIdx];
        auto&& fieldsJson = resultJson["fields"][columnIdx];
        addField(fieldsJson, "name",      field.name,          "The name of the column");
        addField(fieldsJson, "orgName",   field.orgName,       "The original name of the column");
        addField(fieldsJson, "table",     field.table,         "The name of the table");
        addField(fieldsJson, "orgTable",  field.orgTable,      "The original name of the table");
        addField(fieldsJson, "db",        field.db,            "The name of the database (schema)");
        addField(fieldsJson, "catalog",   field.catalog,       "The catalog name (always 'def')");
        addField(fieldsJson, "def",       field.def,           "default value");
        addField(fieldsJson, "length",    field.length,        "The length (width) of the column definition");
        addField(fieldsJson, "maxLength", field.maxLength,     "The maximum length of the column value");
        addField(fieldsJson, "flags",     field.flags,         "Flags");
        addField(fieldsJson, "decimals",  field.decimals,      "Number of decimals");
        addField(fieldsJson, "type",      field.type,          "Field type (see MySQL headers for enum enum_field_types)");
        addField(fieldsJson, "typeName",  field.type2string(), "Field type name (see MySQL headers for enum enum_field_types)");
    }
    for (auto&& row : rows) {
        json rowJson;
        rowJson["cells"] = row.cells;
        rowJson["nulls"] = row.nulls;
        resultJson["rows"].push_back(rowJson);
    }
    return resultJson;
}


util::ColumnTablePrinter SqlResultSet::ResultSet::toColumnTable(
        string const& caption,
        string const& indent,
        bool verticalSeparator) const {

    if (not hasResult) {
        throw logic_error(
                "SqlResultSet::ResultSet::" + string(__func__) + "  no result set for the query");
    }

    // Package input data into columns

    size_t const numRows = rows.size();
    size_t const numColumns = fields.size();

    vector<shared_ptr<vector<string>>> tableColumns;
    tableColumns.reserve(numColumns);

    for (size_t columnIdx = 0; columnIdx < numColumns; ++columnIdx) {
        auto tableColumnPtr = make_shared<vector<string>>();
        tableColumnPtr->reserve(numRows);
        tableColumns.push_back(tableColumnPtr);
    }
    for (auto&& row : rows) {                
        for (size_t columnIdx = 0; columnIdx < numColumns; ++columnIdx) {
            tableColumns[columnIdx]->push_back(
                    row.nulls.at(columnIdx) ? "NULL" : row.cells.at(columnIdx));
        }
    }

    // Build the table

    util::ColumnTablePrinter table(caption, indent, verticalSeparator);
    for (size_t columnIdx = 0; columnIdx < numColumns; ++columnIdx) {
        table.addColumn(fields[columnIdx].name,
                        *(tableColumns[columnIdx]),
                        util::ColumnTablePrinter::LEFT);
    }
    return table;
}


void SqlResultSet::set(ProtocolResponseSql const& message) {
    queryResultSet.clear();
    for (int i = 0; i < message.result_sets_size(); ++i) {
        auto&& resultSetMessage = message.result_sets(i);
        queryResultSet.emplace(resultSetMessage.scope(), resultSetMessage);
    }
}    


json SqlResultSet::toJson() const {

    json resultJson = json::array();

    for (auto&& itr: queryResultSet) {
        auto&& scope = itr.first;
        auto&& resultSet = itr.second;
        json resultSetJson = json::object();
        resultSetJson["scope"] = scope;
        resultSetJson["result_set"] = resultSet.toJson();
        resultJson.push_back(resultSetJson);
    }
    return resultJson;
}


bool SqlResultSet::hasErrors() const {
    return any_of(queryResultSet.cbegin(), queryResultSet.cend(), [](auto const& elem) {
        return elem.second.extendedStatus != ProtocolStatusExt::NONE;
    });
}

bool SqlResultSet::allErrorsOf(ProtocolStatusExt status) const {
    return not any_of(queryResultSet.cbegin(), queryResultSet.cend(), [status](auto const& elem) {
        return elem.second.extendedStatus != status;
    });
}


string SqlResultSet::firstError() const {
    auto const itr = find_if(queryResultSet.cbegin(), queryResultSet.cend(), [](auto const& elem) {
        return elem.second.extendedStatus != ProtocolStatusExt::NONE;
    });
    return itr == queryResultSet.cend()
        ? string()
        : itr->first + ":" + status2string(itr->second.extendedStatus) + ":" + itr->second.error;
}


vector<string> SqlResultSet::allErrors() const {
    vector<string> errors;
    for (auto&& itr: queryResultSet) {
        auto&& scope = itr.first;
        auto&& result = itr.second;
        if (result.extendedStatus != ProtocolStatusExt::NONE) {
            errors.push_back(scope + ":" + status2string(result.extendedStatus) + ":" + result.error);
        }
    }
    return errors;
}


ostream& operator<<(ostream& os, SqlResultSet const& info) {
    os << "SqlResultSet:{qservResultSet:[";
    bool first = false;
    for (auto&& itr: info.queryResultSet) {
        auto&& scope = itr.first;
        auto&& result = itr.second;
        if (first) {
            first = false;
            os << ",";
        }
        os << "{scope:'" << scope << "',"
           << "extendedStatus:" << status2string(result.extendedStatus) << ","
           << "error:'" << result.error << "',"
           << "charSetName:'" << result.charSetName << "',"
           << "hasResult:" << bool2str(result.hasResult) << ","
           << "fields.size:" << result.fields.size() << ","
           << "rows.size:" << result.rows.size() << "}";
    }
    os << "],performanceSec:" << info.performanceSec << "}}";
    return os;
}

}}} // namespace lsst::qserv::replica
