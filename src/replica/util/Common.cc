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
#include "replica/util/Common.h"

// System headers
#include <stdexcept>

// Third party headers
#include "boost/uuid/uuid.hpp"
#include "boost/uuid/uuid_generators.hpp"
#include "boost/uuid/uuid_io.hpp"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

string overlapSelector2str(ChunkOverlapSelector selector) {
    switch (selector) {
        case ChunkOverlapSelector::CHUNK:
            return "CHUNK";
        case ChunkOverlapSelector::OVERLAP:
            return "OVERLAP";
        case ChunkOverlapSelector::CHUNK_AND_OVERLAP:
            return "CHUNK_AND_OVERLAP";
    }
    throw invalid_argument(
            "lsst::qserv::replica::" + string(__func__) + " unhandled selector: " +
            to_string(static_cast<typename std::underlying_type<ChunkOverlapSelector>::type>(selector)));
}

ostream& operator<<(ostream& os, ChunkOverlapSelector selector) {
    os << overlapSelector2str(selector);
    return os;
}

ChunkOverlapSelector str2overlapSelector(string const& str) {
    if (str == "CHUNK")
        return ChunkOverlapSelector::CHUNK;
    else if (str == "OVERLAP")
        return ChunkOverlapSelector::OVERLAP;
    else if (str == "CHUNK_AND_OVERLAP")
        return ChunkOverlapSelector::CHUNK_AND_OVERLAP;
    throw invalid_argument("lsst::qserv::replica::" + string(__func__) + " the input string '" + str +
                           "' doesn't match any selector.");
}

////////////////////////////////////////////
//                Generators              //
////////////////////////////////////////////

replica::Mutex Generators::_mtx;

string Generators::uniqueId() {
    replica::Lock lock(_mtx, "Generators::" + string(__func__));
    boost::uuids::uuid id = boost::uuids::random_generator()();
    return boost::uuids::to_string(id);
}

///////////////////////////////////////////
//                SqlColDef              //
///////////////////////////////////////////

list<SqlColDef> parseSqlColumns(json const& columnsJsonArray) {
    if (!columnsJsonArray.is_array()) {
        throw invalid_argument("lsst::qserv::replica::" + string(__func__) +
                               "  columnsJsonArray is not an array");
    }
    list<SqlColDef> columns;
    for (auto const& column : columnsJsonArray) {
        columns.emplace_back(column.at("name"), column.at("type"));
    }
    return columns;
}

///////////////////////////////////////////
//                SqlIndexDef            //
///////////////////////////////////////////

SqlIndexDef::SqlIndexDef(json const& indexSpecJson) {
    if (!indexSpecJson.is_object()) {
        throw invalid_argument("lsst::qserv::replica::" + string(__func__) +
                               "  indexSpecJson is not an object");
    }
    spec = indexSpecJson.value("spec", "DEFAULT");
    name = indexSpecJson.at("name");
    comment = indexSpecJson.value("comment", "");
    auto const keysJsonArray = indexSpecJson.at("keys");
    if (!keysJsonArray.is_array()) {
        throw invalid_argument("lsst::qserv::replica::" + string(__func__) + "  keys is not an array");
    }
    for (auto const& key : keysJsonArray) {
        keys.emplace_back(key.at("name"), key.at("length"), key.at("ascending"));
    }
}

///////////////////////////////////////////
//                SqlIndexSpec           //
///////////////////////////////////////////

SqlIndexSpec::SqlIndexSpec(ProtocolRequestSql::IndexSpec spec) {
    switch (spec) {
        case ProtocolRequestSql::DEFAULT: {
            _spec = SqlIndexSpec::DEFAULT;
            break;
        }
        case ProtocolRequestSql::UNIQUE: {
            _spec = SqlIndexSpec::UNIQUE;
            break;
        }
        case ProtocolRequestSql::FULLTEXT: {
            _spec = SqlIndexSpec::FULLTEXT;
            break;
        }
        case ProtocolRequestSql::SPATIAL: {
            _spec = SqlIndexSpec::SPATIAL;
            break;
        }
        default:
            throw invalid_argument("IndexSpec::" + string(__func__) +
                                   "  unsupported protocol index specification: '" +
                                   ProtocolRequestSql_IndexSpec_Name(spec) + "'");
    }
}

SqlIndexSpec::SqlIndexSpec(string const& str) {
    if (str == "DEFAULT") {
        _spec = SqlIndexSpec::DEFAULT;
    } else if (str == "UNIQUE") {
        _spec = SqlIndexSpec::UNIQUE;
    } else if (str == "FULLTEXT") {
        _spec = SqlIndexSpec::FULLTEXT;
    } else if (str == "SPATIAL") {
        _spec = SqlIndexSpec::SPATIAL;
    } else {
        throw invalid_argument("SqlIndexSpec::" + string(__func__) + "  unsupported index specification: '" +
                               str + "'");
    }
}

string SqlIndexSpec::str() const {
    switch (_spec) {
        case SqlIndexSpec::DEFAULT:
            return "DEFAULT";
        case SqlIndexSpec::UNIQUE:
            return "UNIQUE";
        case SqlIndexSpec::FULLTEXT:
            return "FULLTEXT";
        case SqlIndexSpec::SPATIAL:
            return "SPATIAL";
    }
    throw runtime_error("SqlIndexSpec::" + string(__func__) + "  unsupported index specification");
}

ProtocolRequestSql::IndexSpec SqlIndexSpec::protocol() const {
    switch (_spec) {
        case SqlIndexSpec::DEFAULT:
            return ProtocolRequestSql::DEFAULT;
        case SqlIndexSpec::UNIQUE:
            return ProtocolRequestSql::UNIQUE;
        case SqlIndexSpec::FULLTEXT:
            return ProtocolRequestSql::FULLTEXT;
        case SqlIndexSpec::SPATIAL:
            return ProtocolRequestSql::SPATIAL;
    }
    throw runtime_error("SqlIndexSpec::" + string(__func__) + "  unsupported index specification: '" + str() +
                        "'");
}

string tableNameBuilder(string const& databaseName, string const& tableName, string const& suffix) {
    size_t const tableNameLimit = 64;
    string const name = databaseName + "__" + tableName + suffix;
    if (name.size() > tableNameLimit) {
        throw invalid_argument("replica::" + string(__func__) + " MySQL table name limit of " +
                               to_string(tableNameLimit) + " characters has been exceeded for table '" +
                               name + "'.");
    }
    return name;
}

}  // namespace lsst::qserv::replica
