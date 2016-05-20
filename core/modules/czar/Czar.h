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
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

// Third-party headers

// Qserv headers
#include "ccontrol/UserQuery.h"
#include "ccontrol/UserQueryFactory.h"
#include "czar/CzarConfig.h"
#include "czar/SubmitResult.h"
#include "global/stringTypes.h"
#include "mysql/MySqlConfig.h"
#include "util/ConfigStore.h"

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

class Czar {
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
     * @param query: Query text.
     * @param hints: Optional query hints, default database name should be
     *               provided as "db" key.
     * @return Structure with info about submitted query.
     */
    SubmitResult submitQuery(std::string const& query,
                             std::map<std::string, std::string> const& hints);

    /**
     * Process a kill query command (experimental).
     *
     * @param query: (client)proxy-provided "KILL QUERY ..." string
     * @param clientId : client name from proxy
     */
    std::string killQuery(std::string const& query, std::string const& clientId);

protected:

private:

    // combines client name (ID) and its thread ID into one unique ID
    typedef std::pair<std::string, int> ClientThreadId;
    typedef std::map<ClientThreadId, std::weak_ptr<ccontrol::UserQuery>> ClientToQuery;

    std::string const _czarName;        ///< Unique czar name
    CzarConfig const _czarConfig;

    std::atomic<uint64_t> _idCounter;   ///< Query/task identifier for next query
    std::unique_ptr<ccontrol::UserQueryFactory> _uqFactory;
    ClientToQuery _clientToQuery;       ///< maps client ID to query
    std::mutex _mutex;                  ///< protects both _uqFactory and _clientToQuery
};

}}} // namespace lsst::qserv::czar

#endif // LSST_QSERV_CZAR_CZAR_H
