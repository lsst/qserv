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
#include "replica/ConfigurationIFace.h"

// Qserv headers
#include "util/IterableFormatter.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using json = nlohmann::json;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.ConfigurationIFace");

} // namespace

namespace lsst {
namespace qserv {
namespace replica {

json WorkerInfo::toJson() const {

    json infoJson;

    infoJson["name"]         = name;
    infoJson["is_enabled"]   = isEnabled  ? 1 : 0;
    infoJson["is_read_only"] = isReadOnly ? 1 : 0;
    infoJson["svc_host"]     = svcHost;
    infoJson["svc_port"]     = svcPort;
    infoJson["fs_host"]      = fsHost;
    infoJson["fs_port"]      = fsPort;
    infoJson["data_dir"]     = dataDir;
    infoJson["db_host"]      = dbHost;
    infoJson["db_port"]      = dbPort;
    infoJson["db_user"]      = dbUser;
    infoJson["loader_host"]  = loaderHost;
    infoJson["loader_port"]  = loaderPort;
    infoJson["loader_tmp_dir"] = loaderTmpDir;
    infoJson["exporter_host"]    = exporterHost;
    infoJson["exporter_port"]    = exporterPort;
    infoJson["exporter_tmp_dir"] = exporterTmpDir;

    return infoJson;
}


string DatabaseInfo::schema4css(string const& table) const {
    string schema;
    for (auto const& coldef: columns.at(table)) {
        schema += string(schema.empty() ? "(" : ", ") + "`" + coldef.name + "` " + coldef.type;
    }
    schema += ")";
    return schema;
}


json DatabaseInfo::toJson() const {

    json infoJson;

    infoJson["name"] = name;
    infoJson["family"] = family;
    infoJson["is_published"] = isPublished ? 1 : 0;

    for (auto&& name: partitionedTables) {
        infoJson["tables"].push_back({
            {"name",           name},
            {"is_partitioned", 1},
            {"latitude_key",   latitudeColName.at(name)},
            {"longitude_key",  longitudeColName.at(name)}
        });
    }
    for (auto&& name: regularTables) {
        infoJson["tables"].push_back({
            {"name",           name},
            {"is_partitioned", 0},
            {"latitude_key",   ""},
            {"longitude_key",  ""}
        });
    }
    for (auto&& columnsEntry: columns) {
        string const& table = columnsEntry.first;
        auto const& coldefs = columnsEntry.second;
        json coldefsJson;
        for (auto&& coldef: coldefs) {
            json coldefJson;
            coldefJson["name"] = coldef.name;
            coldefJson["type"] = coldef.type;
            coldefsJson.push_back(coldefJson);
        }
        infoJson["columns"][table] = coldefsJson;
    }
    infoJson["director_table"] = directorTable;
    infoJson["director_table_key"] = directorTableKey;
    infoJson["chunk_id_key"] = chunkIdColName;
    infoJson["sub_chunk_id_key"] = subChunkIdColName;

    return infoJson;
}


json DatabaseFamilyInfo::toJson() const {

    json infoJson;

    infoJson["name"]                  = name;
    infoJson["min_replication_level"] = replicationLevel;
    infoJson["num_stripes"]           = numStripes;
    infoJson["num_sub_stripes"]       = numSubStripes;
    infoJson["overlap"]               = overlap;

    return infoJson;
}


ostream& operator <<(ostream& os, WorkerInfo const& info) {
    os  << "WorkerInfo ("
        << "name:'"      <<      info.name       << "',"
        << "isEnabled:"  << (int)info.isEnabled  << ","
        << "isReadOnly:" << (int)info.isReadOnly << ","
        << "svcHost:'"   <<      info.svcHost    << "',"
        << "svcPort:"    <<      info.svcPort    << ","
        << "fsHost:'"    <<      info.fsHost     << "',"
        << "fsPort:"     <<      info.fsPort     << ","
        << "dataDir:'"   <<      info.dataDir    << "',"
        << "dbHost:'"    <<      info.dbHost     << "',"
        << "dbPort:"     <<      info.dbPort     << ","
        << "dbUser:'"    <<      info.dbUser     << "',"
        << "loaderHost:'"   <<   info.loaderHost   << "',"
        << "loaderPort:"    <<   info.loaderPort   << ","
        << "loaderTmpDir:'" <<   info.loaderTmpDir << "',"
        << "exporterHost:'"   <<   info.exporterHost   << "',"
        << "exporterPort:"    <<   info.exporterPort   << ","
        << "exporterTmpDir:'" <<   info.exporterTmpDir << "')";
    return os;
}


ostream& operator <<(ostream& os, DatabaseInfo const& info) {
    os  << "DatabaseInfo ("
        << "name:'" << info.name << "',"
        << "family:'" << info.family << "',"
        << "isPublished:" << (int)info.isPublished << ","
        << "partitionedTables:[";
    for (auto const& table: info.partitionedTables) {
        os  << "(name:'" << table << "','"
            << "latitudeColName:'"  << info.latitudeColName.at(table) << "','"
            << "longitudeColName:'" << info.longitudeColName.at(table) << "'),";
    }
    os  << "],"
        << "regularTables:" << util::printable(info.regularTables)  << ","
        << "directorTable:" << info.directorTable << ","
        << "directorTableKey:" << info.directorTableKey << ","
        << "chunkIdColName:" << info.chunkIdColName << ","
        << "subChunkIdColName:" << info.subChunkIdColName << ")";
    return os;
}


ostream& operator <<(ostream& os, DatabaseFamilyInfo const& info) {
    os  << "DatabaseFamilyInfo ("
        << "name:'" << info.name << "',"
        << "replicationLevel:'" << info.replicationLevel << "',"
        << "numStripes:" << info.numStripes << ","
        << "numSubStripes:" << info.numSubStripes << ","
        << "overlap:" << info.overlap << ")";
    return os;
}


string ConfigurationIFace::context(string const& func) const {
    string const str = "CONFIG   " + func;
    return str;
}

}}} // namespace lsst::qserv::replica
