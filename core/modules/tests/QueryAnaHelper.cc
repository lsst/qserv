/*
 * LSST Data Management System
 * Copyright 2009-2016 AURA/LSST.
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

// Class header
#include "QueryAnaHelper.h"

// System headers
//#include <memory>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qproc/ChunkSpec.h"
#include "query/QueryTemplate.h"
#include "query/SelectStmt.h"

using lsst::qserv::parser::SelectParser;
using lsst::qserv::qproc::ChunkQuerySpec;
using lsst::qserv::qproc::ChunkSpec;
using lsst::qserv::qproc::ChunkSpec;
using lsst::qserv::qproc::QuerySession;
using lsst::qserv::query::Constraint;
using lsst::qserv::query::ConstraintVec;
using lsst::qserv::query::ConstraintVector;
using lsst::qserv::query::SelectStmt;
using lsst::qserv::util::printable;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.tests.QueryAnaHelper");
}

namespace lsst {
namespace qserv {
namespace tests {

SelectParser::Ptr QueryAnaHelper::getParser(const std::string& stmt) {
    SelectParser::Ptr p = SelectParser::newInstance(stmt);
    p->setup();
    return p;
}

std::shared_ptr<QuerySession> QueryAnaHelper::buildQuerySession(QuerySession::Test qsTest,
                                                                const std::string& stmt) {
    querySession = std::make_shared<QuerySession>(qsTest);
    querySession->analyzeQuery(stmt);

    if (LOG_CHECK_LVL(_log, LOG_LVL_DEBUG)) {
        ConstraintVec cv(querySession->getConstraints());
        std::shared_ptr<ConstraintVector> cvRaw = cv.getVector();
        if (cvRaw) {
            LOGS(_log, LOG_LVL_DEBUG, util::printable(*cvRaw));
        }
    }
    return querySession;
}

/* &&&
std::string QueryAnaHelper::buildFirstParallelQuery(bool withSubChunks) {
    querySession->addChunk(ChunkSpec::makeFake(100, withSubChunks));
    QuerySession::Iter i = querySession->cQueryBegin();
    if (i == querySession->cQueryEnd()) {
        throw new std::string("Empty query session");
    }

    ChunkQuerySpec& first = *i;
    std::string const & firstParallelQuery = first.queries[0];
    LOGS(_log, LOG_LVL_TRACE, "First parallel query: " << firstParallelQuery);
    return firstParallelQuery;
}
*/

std::string QueryAnaHelper::buildFirstParallelQuery(bool withSubChunks) {
    querySession->addChunk(ChunkSpec::makeFake(100, withSubChunks));
    auto i = querySession->cQueryBegin();
    if (i == querySession->cQueryEnd()) {
        throw new std::string("Empty query session");
    }

    auto& chunkSpec = *i;
    auto first = querySession->buildChunkQuerySpec(chunkSpec);
    std::string const & firstParallelQuery = first.queries[0];
    LOGS(_log, LOG_LVL_TRACE, "First parallel query: " << firstParallelQuery);
    return firstParallelQuery;
}

std::vector<std::string> QueryAnaHelper::getInternalQueries(
        QuerySession::Test& t, const std::string& stmt) {
    std::vector<std::string> queries;
    buildQuerySession(t, stmt);

    std::string sql = buildFirstParallelQuery();

    queries.push_back(sql);

    if (querySession->needsMerge()) {
        sql = querySession->getMergeStmt()->getQueryTemplate().sqlFragment();
    }
    else {
        sql = "";
    }
    queries.push_back(sql);

    sql = querySession->getProxyOrderBy();
    queries.push_back(sql);

    return queries;
}

}}} // namespace lsst::qserv::tests
