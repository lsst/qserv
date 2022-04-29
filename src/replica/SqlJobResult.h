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
#ifndef LSST_QSERV_REPLICA_SQLJOBRESULT_H
#define LSST_QSERV_REPLICA_SQLJOBRESULT_H

// System headers
#include <functional>
#include <list>
#include <map>
#include <string>

// Qserv headers
#include "replica/SqlResultSet.h"
#include "util/TablePrinter.h"

// Third party headers
#include "nlohmann/json.hpp"

// This header declarations
namespace lsst { namespace qserv { namespace replica {

/**
 * Class SqlJobResult represents a combined result received from worker
 * services upon a completion of the relevant job types.
 */
class SqlJobResult {
public:
    /// Result sets for the requests for each worker. And result sets
    /// are stored as a list since subclass-defined processing may be
    /// assigned to multiple requests issued to each worker.
    std::map<std::string, std::list<SqlResultSet>> resultSets;

    /// Merge a result set of another job into into the current one
    void merge(SqlJobResult const& other);

    // The callback type and its convenience types for the worker and scope
    // names within each batch that are used for exploring results sets of
    // the requests,

    typedef std::string Worker;
    typedef std::string Scope;
    typedef std::function<void(Worker, Scope, SqlResultSet::ResultSet)> OnResultVisitCallback;

    /**
     * Iterate over the result sets.
     * @param onResultVisitCallback the callback function to be called on each
     * result set visited during the iteration.
     */
    void iterate(OnResultVisitCallback const& onResultVisitCallback) const;

    /// @return  JSON representation of the object
    nlohmann::json toJson() const;

    /**
     * Package results into a table which will have the following columns:
     * @code
     *   worker | [scopeName] | status | error
     *  --------+-------------+--------+-------
     * @code
     *
     * First three parameters ('caption', 'indent', and 'verticalSeparator') of
     * the method are the same as for the constructor of the table printer class.
     * @see class util::ColumnTablePrinter
     *
     * @param reportAll If a value of the parameter is 'true' then include all
     * result sets into the report regardless of the completion status reported
     * at the result set. Otherwise only those result sets which are reported as
     * failed to be processed by the job will be reported.
     * @param scopeName the meaning of a scope in which results sets were obtained.
     * These could be "table", "database", etc. It's printed in the header of
     * the second column of the table.
     */
    util::ColumnTablePrinter toColumnTable(std::string const& caption = std::string(),
                                           std::string const& indent = std::string(),
                                           bool verticalSeparator = true, bool reportAll = true,
                                           std::string const& scopeName = "") const;

    /**
     * Package results into a summary table which will have the following columns:
     * @code
     *   worker | #succeeded | #failed | performance [sec]
     *  --------+------------+---------+-------------------
     * @code
     *
     * First three parameters ('caption', 'indent', and 'verticalSeparator') of
     * the method are the same as for the constructor of the table printer class.
     * @see class util::ColumnTablePrinter
     */
    util::ColumnTablePrinter summaryToColumnTable(std::string const& caption = std::string(),
                                                  std::string const& indent = std::string(),
                                                  bool verticalSeparator = true) const;
};

}}}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SQLJOBRESULT_H
