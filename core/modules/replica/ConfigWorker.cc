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
#include "replica/ConfigWorker.h"

// System headers
#include <iostream>
#include <stdexcept>

using namespace std;
using json = nlohmann::json;

// Template functions for filling worker attributes from JSON.

namespace {
template <typename T>
void parse(T& dest, json const& obj, string const& attr) {
    dest = obj.at(attr).get<T>();
}

template <>
void parse<bool>(bool& dest, json const& obj, string const& attr) {
    dest = obj.at(attr).get<int>() != 0;
}

template <typename T>
void parse(T& dest, json const& obj, string const& attr, json const& defaults) {
    dest = (obj.count(attr) != 0 ? obj : defaults).at(attr).get<T>();
}

template <typename T>
void parse(T& dest, json const& obj, string const& attr, T const& defaultValue) {
    dest = obj.count(attr) != 0 ? obj.at(attr).get<T>() : defaultValue;
}

// Template functions for filling worker attributes from the corresponding attriutes
// of another worker descriptor.

void parsePort(uint16_t& dest, uint16_t src, json const& defaultValueObj) {
    dest = src != 0 ? src : defaultValueObj.get<uint16_t>();
}

void parseHost(string& dest, string const& src, string const& defaultValue) {
    dest = !src.empty() ? src : defaultValue;
}

void parseStr(string& dest, string const& src, json const& defaultValueObj) {
    dest = !src.empty() ? src : defaultValueObj.get<string>();
}

}

namespace lsst {
namespace qserv {
namespace replica {

WorkerInfo::WorkerInfo(json const& obj, json const& defaults) {
    string const context = "WorkerInfo::WorkerInfo(json,defaults): ";
    if (obj.empty()) return;
    if (!obj.is_object()) {
        throw invalid_argument(context + "a JSON object is required.");
    }
    // Amend missing attributes from a collection of the defaults if needed.
    try {
        parse<string>(name, obj, "name");
        parse<bool>(isEnabled, obj, "is_enabled");
        parse<bool>(isReadOnly, obj, "is_read_only");

        parse<string>(svcHost, obj, "svc_host");
        parse<uint16_t>(svcPort, obj, "svc_port", defaults);

        parse<string>(fsHost, obj, "fs_host", svcHost);
        parse<uint16_t>(fsPort, obj, "fs_port", defaults);
        parse<string>(dataDir, obj, "data_dir", defaults);

        parse<string>(dbHost, obj, "db_host", svcHost);
        parse<uint16_t>(dbPort, obj, "db_port", defaults);
        parse<string>(dbUser, obj, "db_user", defaults);

        parse<string>(loaderHost, obj, "loader_host", svcHost);
        parse<uint16_t>(loaderPort, obj, "loader_port", defaults);
        parse<string>(loaderTmpDir, obj, "loader_tmp_dir", defaults);

        parse<string>(exporterHost, obj, "exporter_host", svcHost);
        parse<uint16_t>(exporterPort, obj, "exporter_port", defaults);
        parse<string>(exporterTmpDir, obj, "exporter_tmp_dir", defaults);

        parse<string>(httpLoaderHost, obj, "http_loader_host", svcHost);
        parse<uint16_t>(httpLoaderPort, obj, "http_loader_port", defaults);
        parse<string>(httpLoaderTmpDir, obj, "http_loader_tmp_dir", defaults);

    } catch (exception const& ex) {
        throw invalid_argument(context + "the JSON object is not valid, ex: " + string(ex.what()));
    }
}


WorkerInfo::WorkerInfo(WorkerInfo const& info, json const& defaults) {
    string const context = "WorkerInfo::WorkerInfo(info,defaults): ";
    if (info.name.empty()) {
        throw invalid_argument(context + "the input name of a worker.");
    }
    if (info.svcHost.empty()) {
        throw invalid_argument(context + "the input name of a host for the Replication service is empty.");
    }
    if (!defaults.is_object()) {
        throw invalid_argument(context + "a JSON object with worker defaults is required.");
    }
    // Amend missing attributes from a collection of the defaults if needed.
    try {
        name       = info.name;
        isEnabled  = info.isEnabled;
        isReadOnly = info.isReadOnly;

        svcHost = info.svcHost;
        parsePort(svcPort, info.svcPort, defaults.at("svc_port"));

        parseHost(fsHost, info.fsHost, info.svcHost);
        parsePort(fsPort, info.fsPort, defaults.at("fs_port"));
        parseStr(dataDir, info.dataDir, defaults.at("data_dir"));

        parseHost(dbHost, info.dbHost, info.svcHost);
        parsePort(dbPort, info.dbPort, defaults.at("db_port"));
        parseStr(dbUser, info.dbUser, defaults.at("db_user"));

        parseHost(loaderHost, info.loaderHost, info.svcHost);
        parsePort(loaderPort, info.loaderPort, defaults.at("loader_port"));
        parseStr(loaderTmpDir, info.loaderTmpDir, defaults.at("loader_tmp_dir"));

        parseHost(exporterHost, info.exporterHost, info.svcHost);
        parsePort(exporterPort, info.exporterPort, defaults.at("exporter_port"));
        parseStr(exporterTmpDir, info.exporterTmpDir, defaults.at("exporter_tmp_dir"));

        parseHost(httpLoaderHost, info.httpLoaderHost, info.svcHost);
        parsePort(httpLoaderPort, info.httpLoaderPort, defaults.at("http_loader_port"));
        parseStr(httpLoaderTmpDir, info.httpLoaderTmpDir, defaults.at("http_loader_tmp_dir"));

    } catch (exception const& ex) {
        throw invalid_argument(context + "the JSON object is not valid, ex: " + string(ex.what()));
    }
}


json WorkerInfo::toJson() const {
    json infoJson;
    infoJson["name"] = name;
    infoJson["is_enabled"] = isEnabled  ? 1 : 0;
    infoJson["is_read_only"] = isReadOnly ? 1 : 0;
    infoJson["svc_host"] = svcHost;
    infoJson["svc_port"] = svcPort;
    infoJson["fs_host"] = fsHost;
    infoJson["fs_port"] = fsPort;
    infoJson["data_dir"] = dataDir;
    infoJson["db_host"] = dbHost;
    infoJson["db_port"] = dbPort;
    infoJson["db_user"] = dbUser;
    infoJson["loader_host"] = loaderHost;
    infoJson["loader_port"] = loaderPort;
    infoJson["loader_tmp_dir"] = loaderTmpDir;
    infoJson["exporter_host"] = exporterHost;
    infoJson["exporter_port"] = exporterPort;
    infoJson["exporter_tmp_dir"] = exporterTmpDir;
    infoJson["http_loader_host"] = httpLoaderHost;
    infoJson["http_loader_port"] = httpLoaderPort;
    infoJson["http_loader_tmp_dir"] = httpLoaderTmpDir;
    return infoJson;
}


ostream& operator <<(ostream& os, WorkerInfo const& info) {
    os  << "WorkerInfo: " << info.toJson().dump();
    return os;
}

}}} // namespace lsst::qserv::replica
