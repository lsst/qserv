// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2009-2015 AURA/LSST.
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

 /**
  * @file
  *
  * @brief Test functions and structures used in QueryAnalysis tests
  *
  * @author Fabrice Jammes, IN2P3/SLAC
  */

// System headers
#include <memory>
#include <string>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "parser/SelectParser.h"
#include "qproc/QuerySession.h"
#include "query/Constraint.h"
#include "util/IterableFormatter.h"

using lsst::qserv::parser::SelectParser;
using lsst::qserv::qproc::ChunkQuerySpec;
using lsst::qserv::qproc::ChunkSpec;
using lsst::qserv::qproc::ChunkSpec;
using lsst::qserv::qproc::QuerySession;
using lsst::qserv::query::Constraint;
using lsst::qserv::query::ConstraintVec;
using lsst::qserv::query::ConstraintVector;
using lsst::qserv::util::formatable;

namespace lsst {
namespace qserv {
namespace tests {

struct QueryAnaHelper {

    static SelectParser::Ptr getParser(const std::string& stmt);

    /**
    * @brief Prepare the query session used to process SQL queries
    * issued from MySQL client.
    *
    * @param t:             Test environment required by the object
    * @param stmt:          sql query to process
    * @param expectedErr:   expected error message
    *
    */
    std::shared_ptr<QuerySession> buildQuerySession(QuerySession::Test qsTest,
                                                    std::string const & stmt);

    std::string buildFirstParallelQuery(bool withSubChunks = true);

    /** @brief control consistency of Qserv internal queries
     *
     *  These queries are generated during query analysis
     *
     * If a user SQL query requires
     *
     */
    std::vector<std::string> getInternalQueries(QuerySession::Test& t,
                                                std::string const & stmt);

    std::shared_ptr<QuerySession> querySession;
};



}}} // namespace lsst::qserv::test
