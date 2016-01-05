// Class header
#include "qmeta/QMeta.h"

// System headers

// Third-party headers
#include "boost/lexical_cast.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qmeta/Exceptions.h"
#include "qmeta/QMetaMysql.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.qmeta.QMeta");

// get optional value from map, deturn default if key does not exist
std::string _mapGet(std::map<std::string, std::string> const& config,
                    std::string const& key,
                    std::string const& def=std::string()) {
    auto iter = config.find(key);
    if (iter == config.end()) {
        return def;
    }
    return iter->second;
}

}

namespace lsst {
namespace qserv {
namespace qmeta {

std::shared_ptr<QMeta>
QMeta::createFromConfig(std::map<std::string, std::string> const& config) {
    LOGS(_log, LOG_LVL_DEBUG, "Create QMeta instance from config map");

    auto iter = config.find("technology");
    if (iter == config.end()) {
        LOGS(_log, LOG_LVL_DEBUG, "\"technology\" does not exist in configuration map");
        throw ConfigError(ERR_LOC, "\"technology\" does not exist in configuration map");
    } else if (iter->second == "mysql") {
        // extract all optional values from map
        mysql::MySqlConfig mysqlConfig;
        mysqlConfig.hostname = _mapGet(config, "hostname");
        mysqlConfig.username = _mapGet(config, "username");
        mysqlConfig.password = _mapGet(config, "password");
        mysqlConfig.dbName = _mapGet(config, "database");
        mysqlConfig.socket = _mapGet(config, "socket");
        auto portStr = _mapGet(config, "port", "");
        if (portStr.empty()) portStr = "0";
        try {
            // tried to use std::stoi() here but it returns OK for strings like "0xFSCK"
            mysqlConfig.port = boost::lexical_cast<unsigned>(portStr);
        } catch (boost::bad_lexical_cast const& exc) {
            LOGS(_log, LOG_LVL_DEBUG, "failed to convert \"port\" to number: " << portStr);
            throw ConfigError(ERR_LOC, "failed to convert \"port\" to number " + portStr);
        }

        LOGS(_log, LOG_LVL_DEBUG, "Create QMeta instance with mysql store");
        return std::make_shared<QMetaMysql>(mysqlConfig);
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "Unexpected value of \"technology\" key: " << iter->second);
        throw ConfigError(ERR_LOC, "Unexpected value of \"technology\" key: " + iter->second);
    }
}

}}} // namespace lsst::qserv::qmeta
