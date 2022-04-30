// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2019 LSST Corporation.
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

#ifndef LSST_QSERV_PARSER_PARSERUNNER_H
#define LSST_QSERV_PARSER_PARSERUNNER_H

// System headers
#include <memory>
#include <sstream>

// Qserv headers
#include "util/common.h"

// Forward declarations
namespace lsst::qserv::ccontrol {
class ParseListener;
class UserQuery;
class UserQueryResources;
}  // namespace lsst::qserv::ccontrol

namespace lsst::qserv::query {
class SelectStmt;
}

namespace lsst::qserv::ccontrol {

/// ParseRunner drives the antlr4-based SQL parser.
class ParseRunner {
public:
    /**
     * @brief Construct a new Parse Runner object. Runs the parse.
     *
     * @param statement The sql statement to parse
     *
     * @throws parser::ParseException if the query can not be parsed.
     */
    ParseRunner(std::string const& statement);

    /**
     * @brief Construct a new Parse Runner object
     *
     * @param statement The sql statement to parse
     * @param queryResources Resources that may be used to construct a UserQuery.
     *
     * @throws parser::ParseException if the query can not be parsed.
     */
    ParseRunner(std::string const& statement, std::shared_ptr<UserQueryResources> const& queryResources);

    typedef std::shared_ptr<ParseRunner> Ptr;

    /**
     * @brief Convenience function to get a SelectStatement.
     *
     * This calls the ParseRunner constructor, so it may throw if the constructor throws.
     *
     * @param statement The sql statement to parse.
     * @return std::shared_ptr<query::SelectStmt> the SelectStmt generated using the given statement.
     */
    static std::shared_ptr<query::SelectStmt> makeSelectStmt(std::string const& statement);

    /**
     * @brief Get the original select statement.
     */
    std::string const& getStatement() const { return _statement; }

    /**
     * @brief Get the SelectStmt object if one was created.
     *
     * @return std::shared_ptr<query::SelectStmt> a pointer to the generated SelectStmt, or nullptr.
     */
    std::shared_ptr<query::SelectStmt> getSelectStmt();

    /**
     * @brief Get the User Query object if one was created.
     *
     * @return std::shared_ptr<UserQuery> a pointer to the generated UserQuery or nullptr.
     */
    std::shared_ptr<UserQuery> getUserQuery();

private:
    /**
     * @brief Execute the parse.
     */
    void run();

    std::string const _statement;
    std::shared_ptr<UserQueryResources> _queryResources;
    std::shared_ptr<ParseListener> _listener;
};

}  // namespace lsst::qserv::ccontrol

#endif  // LSST_QSERV_PARSER_PARSERUNNER_H
