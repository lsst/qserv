/*
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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

#ifndef LSST_QSERV_SQL_PARSER_BASE
#define LSST_QSERV_SQL_PARSER_BASE
#include "antlr/CommonAST.hpp"

// parserBase.h Abstract types to be used in the SQL grammar.
// * Not placed in include/
// * Placed with DmlSQL2.g and SqlSQL2.g, which generate .cpp and .hpp
class VoidFourRefFunc {
public:
    virtual ~VoidFourRefFunc() {}
    virtual void operator()(antlr::RefAST a, antlr::RefAST b,
			    antlr::RefAST c, antlr::RefAST d) = 0;
};
class VoidThreeRefFunc {
public:
    virtual ~VoidThreeRefFunc() {}
    virtual void operator()(antlr::RefAST a, antlr::RefAST b,
			    antlr::RefAST c) = 0;
};
class VoidTwoRefFunc {
public:
    virtual ~VoidTwoRefFunc() {}
    virtual void operator()(antlr::RefAST a, antlr::RefAST b) = 0;
};
class VoidOneRefFunc {
public:
    virtual ~VoidOneRefFunc() {}
    virtual void operator()(antlr::RefAST a) = 0;
};
class VoidVoidFunc {
public:
    virtual ~VoidVoidFunc() {}
    virtual void operator()() = 0;
};



#endif // LSST_QSERV_SQL_PARSER_BASE

