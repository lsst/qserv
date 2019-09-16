// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2015 LSST Corporation.
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
#ifndef LSST_QSERV_PARSER_SELECTPARSER_H
#define LSST_QSERV_PARSER_SELECTPARSER_H
/**
  * @file
  *
  * @brief SelectParser operates the ANTLR generated parsers on raw SELECT
  * statements.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <memory>
#include <sstream>

// Qserv headers
#include "ccontrol/ListenerDebugHelper.h"
#include "util/common.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace ccontrol {
    class QSMySqlListener;
    class UserQuery;
    class UserQueryResources;
}
namespace parser {
    class Antlr4Parser; // Internally-defined in SelectParser.cc
}
namespace query {
    class SelectStmt;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace parser {


class Antlr4Parser : public ccontrol::ListenerDebugHelper, public std::enable_shared_from_this<Antlr4Parser> {
public:
    static std::shared_ptr<Antlr4Parser> create(std::string const & q, std::shared_ptr<ccontrol::UserQueryResources> queryResources);

    void run();

    std::shared_ptr<query::SelectStmt> getStatement();

    std::shared_ptr<ccontrol::UserQuery> getUserQuery();

    std::string getStringTree() const override;

    std::string getTokens() const override;

    std::string getStatementString() const override;

private:
    Antlr4Parser(std::string const& q, std::shared_ptr<ccontrol::UserQueryResources> const& queryResources);

    std::string _statement;
    std::shared_ptr<ccontrol::UserQueryResources> _queryResources;
    std::shared_ptr<ccontrol::QSMySqlListener> _listener;
};


/// class SelectParser - drives the ANTLR-generated SQL parser for a
/// SELECT statement. Attaches some simple handlers that populate a
/// corresponding data structure, which can then be processed and
/// evaluated to determine query generation and dispatch.
///
/// SelectParser is the spiritual successor to the original SqlParseRunner, but
/// is much simpler because it is only responsible for creating a parsed query
/// representation, without any furhter analysis or annotation.
class SelectParser {
public:
    SelectParser(std::string const& statement);

    typedef std::shared_ptr<SelectParser> Ptr;

    /// Convenience function to get a SelectStatement.
    /// This function calls SelectParser::setup; so it may throw any exception thrown by that function.
    static std::shared_ptr<query::SelectStmt> makeSelectStmt(std::string const& statement);

    // @return Original select statement
    std::string const& getStatement() const { return _statement; }

    std::shared_ptr<query::SelectStmt> getSelectStmt();

private:

    std::string const _statement;
    std::shared_ptr<query::SelectStmt> _selectStmt;
    std::shared_ptr<Antlr4Parser> _aParser;
};


}}} // namespace lsst::qserv::parser

#endif // LSST_QSERV_PARSER_SELECTPARSER_H
