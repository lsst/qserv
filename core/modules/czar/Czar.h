/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */
#ifndef LSST_QSERV_CZAR_CZAR_H
#define LSST_QSERV_CZAR_CZAR_H

// System headers
#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <vector>

// Third-party headers

// Qserv headers
#include "ccontrol/UserQueryFactory.h"
#include "global/stringTypes.h"
#include "mysql/MySqlConfig.h"

namespace lsst {
namespace qserv {
namespace czar {

/// @addtogroup czar

/**
 *  @ingroup czar
 *
 *  @brief Class representing czar "entry points".
 *
 */

class Czar  {
public:

    /**
     * Make new instance.
     *
     * @param configPath:   Path to the configuration file.
     * @param czarName:     Name if this instance, must be unique. If empty name
     *                      is given then random name will be constructed.
     */
    Czar(std::string const& configPath, std::string const& czarName);

    Czar(Czar const&) = delete;
    Czar& operator=(Czar const&) = delete;

    /**
     * Submit query for execution.
     *
     * Returns list of strings:
     *  [0] error message, empty if all is fine
     *  [1] result table name
     *  [2] message table name
     *  [3] order by clause (optional)
     *
     * @param query: Query text.
     * @param hints: Optional query hints, default database name should be
     *               provided as "db" key.
     */
    std::vector<std::string> submitQuery(std::string const& query,
                                         std::map<std::string, std::string> const& hints);

    /**
     * Process a kill query command (experimental).
     *
     * @param query: (client)proxy-provided "KILL QUERY ..." string
     * @param clientId : client name from proxy
     */
    void killQueryUgly(std::string const& query, std::string const& clientId);

protected:

private:

    std::string _czarName;    ///< Unique czar name
    StringMap _config;        ///< Czar configuration (section.key -> value)
    std::atomic<unsigned> _idCounter;   ///< Query identifier for next query
    mysql::MySqlConfig _resultConfig;  ///< Configuration for result database
    std::unique_ptr<ccontrol::UserQueryFactory> _uqFactory;
};

}}} // namespace lsst::qserv::czar

#endif // LSST_QSERV_CZAR_CZAR_H
