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
#include "util/ConfigStore.h"
#include "util/ConfigStoreError.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.qmeta.QMeta");

}

namespace lsst {
namespace qserv {
namespace qmeta {

std::shared_ptr<QMeta>
QMeta::createFromConfig(std::map<std::string, std::string> const& config) {
    LOGS(_log, LOG_LVL_DEBUG, "Create QMeta instance from config map");

    util::ConfigStore configStore(config);
    std::string technology;

    try {
        technology = configStore.getRequired("technology");
    } catch (util::KeyNotFoundError const& e) {
        LOGS(_log, LOG_LVL_DEBUG, "\"technology\" does not exist in configuration map");
        throw ConfigError(ERR_LOC, "\"technology\" does not exist in configuration map");
    }
    if (technology == "mysql") {
        try {
            // extract all optional values from map
            mysql::MySqlConfig mysqlConfig(configStore.get("username"),
               configStore.get("password"),
               configStore.get("hostname"),
               configStore.getInt("port"),
               configStore.get("socket"),
               configStore.get("database"));

                LOGS(_log, LOG_LVL_DEBUG, "Create QMeta instance with mysql store");
                return std::make_shared<QMetaMysql>(mysqlConfig);
        } catch (util::ConfigStoreError const& exc) {
            LOGS(_log, LOG_LVL_DEBUG, "Exception launched while creating MySQL configuration: " << exc.what());
            throw ConfigError(ERR_LOC, exc.what());
        }
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "Unexpected value of \"technology\" key: " << technology);
        throw ConfigError(ERR_LOC, "Unexpected value of \"technology\" key: " + technology);
    }
}


}}} // namespace lsst::qserv::qmeta
