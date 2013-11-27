// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
#ifndef LSST_QSERV_PARSER_PARSEEXCEPTION_H
#define LSST_QSERV_PARSER_PARSEEXCEPTION_H
/**
  * @file
  *
  * @author Daniel L. Wang, SLAC
  */
#include <map>
#include <antlr/AST.hpp>
#include <stdexcept>

namespace lsst {
namespace qserv {
namespace parser {

/// ParseException is a trivial exception for Qserv parse problems.
/// ParseExceptions automatically retrieves basic information from the ANTLR
/// parse node to be bundled with the exception for greater context.
class ParseException : public std::runtime_error {
public:
    ParseException(char const* msg, antlr::RefAST subTree);
    ParseException(std::string const& msg, antlr::RefAST subTree);
protected:
    explicit ParseException(std::string const& msg)
        : std::runtime_error(msg) {}
};

}}} // namespace lsst::qserv::parser

#endif // LSST_QSERV_PARSER_PARSEEXCEPTION_H
