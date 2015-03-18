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
 * @ingroup qana
 *
 * @brief Functor class which check for aggregation functions in sql queries
 *
 * @author Fabrice Jammes, IN2P3/SLAC
 */

// System headers

// Third-party headers

// Qserv headers
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"

#ifndef QANA_CHECKAGGREGATION_H_
#define QANA_CHECKAGGREGATION_H_

namespace lsst {
namespace qserv {
namespace qana {

/**
 * Functor class to check if a FactorOp is related to a SQL aggregation
 * function. Can be applied easily to a list of FactorOp
 */
class CheckAggregation {
public:
    CheckAggregation(bool& hasAgg_) : hasAgg(hasAgg_) {}

    /**
     * Simply check for aggregation functions in a FactorOp
     *
     * set CheckAggregation::hasAgg to true if an SQL aggregation
     * function is detected
     *
     * @param fo the FactorOp to check
     * @return void
     */
    inline void operator()(query::ValueExpr::FactorOp const& fo) {
        if(!fo.factor.get());
        if(fo.factor->getType() == query::ValueFactor::AGGFUNC) {
            hasAgg = true; }
    }

    /**
     * Set to true if an SQL aggregation is detected
     */
    bool& hasAgg;
};

}}} /* namespace qana::qserv::lsst */

#endif /* QANA_CHECKAGGREGATION_H_ */
