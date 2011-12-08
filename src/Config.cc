/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
 
#include "lsst/qserv/worker/Config.h"

#include <sstream>
#include <boost/thread/once.hpp>

namespace qWorker = lsst::qserv::worker;

namespace { 
    // Settings declaration ////////////////////////////////////////////////
    static const int settingsCount = 6;
    // key, env var name, default, description
    static const char* settings[settingsCount][4] = {
        {"xrdQueryPath", "QSW_XRDQUERYPATH", "/query2", 
         "xrootd path for query,e.g. /query2"},
        {"mysqlSocket", "QSW_DBSOCK", "/var/lib/mysql/mysql.sock",
         "MySQL socket file path for db connections"},
        {"mysqlDump", "QSW_MYSQLDUMP", "/usr/bin/mysqldump", 
        "path to mysqldump program binary"},
        {"scratchPath", "QSW_SCRATCHPATH", "/tmp/qserv",
         "path to store (temporary) dump files, e.g., /tmp/qserv"},
        {"scratchDb", "QSW_SCRATCHDB", "qservScratch", 
         "MySQL db for creating temporary result tables."},
        {"numThreads", "QSW_NUMTHREADS", "4", 
         "Number of in-flight query threads allowed."}
    };

    // Singleton Config object support /////////////////////////////////////
    qWorker::Config& getConfigHelper() {
        static qWorker::Config c;
        return c;
    }
    void callOnceHelper() { 
        getConfigHelper();
    }
    boost::once_flag configHelperFlag = BOOST_ONCE_INIT;

    // Validator code /////////////////////////////////////////////////////
    bool isExecutable(std::string const& execFile) {
        return 0 == ::access(execFile.c_str(), X_OK);
    }

    bool validateMysql(qWorker::Config const& c) {
        // Can't do dump w/o an executable.
        // Shell exec will crash a boost test case badly if this fails.
        return isExecutable(c.getString("mysqlDump"));
        // In the future, could try connecting to mysql instance here.
    }
}

////////////////////////////////////////////////////////////////////////
qWorker::Config::Config() {
    _load();
    _validate();
}

int qWorker::Config::getInt(std::string const& key, int defVal) const {
    int ret = defVal;
    StringMap::const_iterator i = _map.find(key);
    if(i == _map.end()) {
        return defVal;
    }
    // coerce the string to int.
    std::stringstream s(i->second);
    s >> ret;
    return ret;
}

std::string const& qWorker::Config::getString(std::string const& key) const {
    static const std::string n;
    StringMap::const_iterator i = _map.find(key);
    if(i == _map.end()) {
        return n;
    }
    return i->second;
}

char const* qWorker::Config::_getEnvDefault(char const* varName, 
                                            char const* defVal) {
    char const* s = ::getenv(varName);
    if(s != (char const*)0) { 
        return s; 
    } else { 
        return defVal; 
    }
}

void qWorker::Config::_load() {
    // assume we're thread-protected.
    for(int i = 0; i < settingsCount; ++i) {
        _map[settings[i][0]] = _getEnvDefault(settings[i][1], settings[i][2]);
    }
}

void qWorker::Config::_validate() {
    // assume we're thread-protected
    bool valid = true;
    std::string error;
    bool r;

    r = validateMysql(*this);
    valid &= r;
    if(!r) {
        error += "Bad mysqldump path.";
    }
    _isValid = valid;
    _error = error;
}

////////////////////////////////////////////////////////////////////////
qWorker::Config& qWorker::getConfig() {
    boost::call_once(callOnceHelper, configHelperFlag);
    return getConfigHelper();
}
