// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
 * @ingroup global
 *
 * @brief Query-related C++ types used by query, parser, and qana modules
 *
 * @author Fabrice Jammes, IN2P3/SLAC
 */

// System headers
#include <vector>

// Third-party headers

// Qserv headers

#ifndef QUERY_TYPES_H_
#define QUERY_TYPES_H_

namespace lsst {
namespace qserv {
namespace query {

class ValueExpr;
typedef std::shared_ptr<ValueExpr> ValueExprPtr;
typedef std::vector<ValueExprPtr> ValueExprPtrVector;
typedef ValueExprPtrVector::const_iterator ValueExprPtrVectorConstIter;

class SelectStmt;
typedef std::shared_ptr<SelectStmt> SelectStmtPtr;
typedef std::vector<std::shared_ptr<SelectStmt> > SelectStmtPtrVector;

}}} // namespace lsst::qserv::query

#endif /* QUERY_TYPES_H_ */
