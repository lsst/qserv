// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2013 LSST Corporation.
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
  * @file SelectParser.h
  *
  * @brief SelectParser operates the ANTLR generated parsers on raw SELECT
  * statements.
  *
  * @author Daniel L. Wang, SLAC
  */

// C++ standard
#include <list>
#include <sstream>

#include <boost/shared_ptr.hpp>
// package
#include "util/common.h"

namespace lsst {
namespace qserv {

namespace query {
    // Forward
    class SelectStmt;
}

namespace parser {

// Forward
class AntlrParser; // Internally-defined in SelectParser.cc

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
    typedef boost::shared_ptr<SelectParser> Ptr;

    static Ptr newInstance(std::string const& statement);

    /// Setup the parser and parse into a SelectStmt
    void setup();

    // @return Original select statement
    std::string const& getStatement() const { return _statement; }

    boost::shared_ptr<query::SelectStmt> getSelectStmt() { return _selectStmt; }

private:
    SelectParser(std::string const& statement);

    std::string const _statement;
    boost::shared_ptr<query::SelectStmt> _selectStmt;
    boost::shared_ptr<AntlrParser> _aParser;
};

}}} // namespace lsst::qserv::parser

#endif // LSST_QSERV_PARSER_SELECTPARSER_H
