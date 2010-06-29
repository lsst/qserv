#include "lsst/qserv/worker/Config.h"

#include <boost/thread/once.hpp>
namespace qWorker = lsst::qserv::worker;

namespace { 
    static const int settingsCount = 5;
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
         "MySQL db for creating temporary result tables."}
    };
    qWorker::Config& getConfigHelper() {
        static qWorker::Config c;
        return c;
    }
    void callOnceHelper() { 
        getConfigHelper();
    }
    boost::once_flag configHelperFlag = BOOST_ONCE_INIT;
}

////////////////////////////////////////////////////////////////////////
qWorker::Config::Config() {
    _load();
}

std::string const& qWorker::Config::getString(std::string const& key) {
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
    for(int i = 0; i < settingsCount; ++i) {
        _map[settings[i][0]] = _getEnvDefault(settings[i][1], settings[i][2]);
    }
}
////////////////////////////////////////////////////////////////////////
qWorker::Config& qWorker::getConfig() {
    boost::call_once(callOnceHelper, configHelperFlag);
    return getConfigHelper();
}
