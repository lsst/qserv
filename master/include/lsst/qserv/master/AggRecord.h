// -*- LSST-C++ -*-
/* 
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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
#ifndef LSST_QSERV_MASTER_AGGRECORD_H
#define LSST_QSERV_MASTER_AGGRECORD_H
/**
  * @file 
  *
  * @author Daniel L. Wang, SLAC
  */
#include "lsst/qserv/master/ValueExpr.h"
#include "lsst/qserv/master/ValueFactor.h"

namespace lsst { namespace qserv { namespace master {

/// AggRecord is a value class for the information needed to successfully
/// perform aggregation of distributed queries.  lbl and meaning record the
/// original aggregation invocation (+alias) orig, pass, and fixup record SQL
/// expressions
/// TODO: Consider renaming to AggEntry
struct AggRecord { 
public:
    typedef boost::shared_ptr<AggRecord> Ptr;
    /// Original ValueFactor representing the call (e.g., COUNT(ra_PS))
    lsst::qserv::master::ValueFactorPtr orig;
    /// List of expressions to pass for parallel execution.
    /// Some aggregations need more than one aggregation to be computed (per 
    /// chunk) in order to compute the final aggregation value (e.g., AVG)
    lsst::qserv::master::ValueExprList parallel;
    /// ValueFactor representing merge step. Not a list, because the original
    /// wasn't a list and we want the final result to correspond.
    lsst::qserv::master::ValueFactorPtr merge;
    std::ostream& printTo(std::ostream& os);
};

}}} // namespace lsst::qserv::master

#endif // LSST_QSERV_MASTER_AGGRECORD_H

