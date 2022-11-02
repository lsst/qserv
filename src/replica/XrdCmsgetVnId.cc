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

// System headers
#include <iostream>
#include <string>
#include <vector>

// Qserv headers
#include "replica/Common.h"
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseMySQLUtils.h"

// XrootD headers
#include "XrdCms/XrdCmsVnId.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdVersion.hh"

using namespace std;
using namespace lsst::qserv::replica;
using namespace lsst::qserv::replica::database::mysql;

/**
 * @brief Read a value of the VNID from the Qserv worker database that's
 *   configured via a MySQL connection string passed among the input
 *   parameters of the function.
 *
 * The list of input parameters has the following syntax:
 * @code
 *   <work-db-conn-url> <max-reconnects> <conn-timeout-sec>
 * @code
 *
 * Where:
 *   work-db-conn-url:  the database connector string for the worker's MySQL service
 *   max-reconnects:    the maximum number of reconnects to he service
 *   conn-timeout-sec:  the timeout for connecting to the service and executing the query
 */
extern "C" string XrdCmsgetVnId(XrdCmsgetVnIdArgs) {
    string const context = string(__func__) + ": ";
    string vnId;
    try {
        vector<string> args = lsst::qserv::replica::strsplit(parms);
        if (args.size() != 3) {
            eDest.Say(context.data(), "illegal number of parameters for the plugin. ",
                      "Exactly 3 parameters are required: <work-db-conn-url> <max-reconnects> "
                      "<conn-timeout-sec>.");
        } else {
            string const qservWorkerDbUrl = args[0];
            Configuration::setQservWorkerDbUrl(qservWorkerDbUrl);
            // Parameter 'maxReconnects' limits the total number of retries to execute the query in case
            // if the query fails during execution. If the parameter's value is set to 0 then the default
            // value of the parameter will be pulled by the query processor from the Replication
            // system's Configuration.
            unsigned int maxReconnects = lsst::qserv::replica::stoui(args[1]);
            // Parameter 'timeoutSec' is used both while connecting to the database server and for executing
            // the query. If the MySQl service won't respond to the connection attempts beyond a period of
            // time specified by the parameter then the operation will fail. Similarly, if the query execution
            // will take longer than it's specified in the parameter then the query will fail. If the
            // parameter's value is set to 0 then the default value of the parameter will be pulled by the
            // query processor from the Replication system's Configuration.
            unsigned int timeoutSec = lsst::qserv::replica::stoui(args[2]);
            // This parameter allows the database connector to make reconnects if the MySQL service
            // won't be responding (or not be up) at the initial connection attempt.
            bool const allowReconnects = true;
            // Using the RAII-style connection handler to automatically close the connection and
            // release resources in case of exceptions.
            ConnectionHandler const handler(Connection::open2(
                    Configuration::qservWorkerDbParams("qservw_worker"), allowReconnects, timeoutSec));
            QueryGenerator const g(handler.conn);
            handler.conn->executeInOwnTransaction(
                    [&context, &vnId, &eDest, &g](decltype(handler.conn) conn) {
                        string const query = g.select("id") + g.from("Id");
                        if (!selectSingleValue(conn, query, vnId)) {
                            eDest.Say(context.data(),
                                      "worker identity is not set in the Qserv worker database.");
                        }
                    },
                    maxReconnects, timeoutSec);
        }
    } catch (exception const& ex) {
        eDest.Say(context.data(),
                  "failed to pull worker identity from the Qserv worker database, ex:", ex.what());
    }
    eDest.Say(context.data(), "vnid: ", vnId.data());
    return vnId;
}

XrdVERSIONINFO(XrdCmsgetVnId, vnid_mysql_0);
