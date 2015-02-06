// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2014 LSST Corporation.
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

#ifndef LSST_QSERV_QUERY_FUNCEXPR_H
#define LSST_QSERV_QUERY_FUNCEXPR_H
/**
  * @file
  *
  * @brief FuncExpr is a SQL function expression including a name and a list of
  * parameters.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <string>

// Third-party headers
#include "boost/shared_ptr.hpp"

// Local headers
#include "query/types.h"
#include "query/ColumnRef.h"

namespace lsst {
namespace qserv {
namespace query {

// Forward
class QueryTemplate;

// FuncExpr is a function expression, e.g., foo(1,2,bar)
class FuncExpr {
public:
    typedef boost::shared_ptr<FuncExpr> Ptr;
    std::string getName() const;
    ValueExprPtrVector getParams() const;

    /// Construct a new FuncExpr like an existing one.
    static FuncExpr::Ptr newLike(FuncExpr const& src, std::string const& newName);
    /// Construct a new FuncExpr with a name and string arg
    static FuncExpr::Ptr newArg1(std::string const& newName,
                                 std::string const& arg1);
    /// Construct a new FuncExpr with a name and ValueExpr arg
    static FuncExpr::Ptr newArg1(std::string const& newName,
                                 ValueExprPtr ve);

    void findColumnRefs(ColumnRef::Vector& vector);

    // Fields
    std::string name;
    ValueExprPtrVector params;
    friend std::ostream& operator<<(std::ostream& os, FuncExpr const& fe);
    friend std::ostream& operator<<(std::ostream& os, FuncExpr const* fe);
    void renderTo(QueryTemplate& qt) const;
};

std::ostream& operator<<(std::ostream& os, FuncExpr const& fe);
std::ostream& operator<<(std::ostream& os, FuncExpr const* fe);

// output helpers
std::ostream& output(std::ostream& os, ValueExprPtrVector const& vel);
void renderList(QueryTemplate& qt, ValueExprPtrVector const& vel);

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_FUNCEXPR_H
