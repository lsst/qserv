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

#ifndef LSST_QSERV_QUERY_AGGOP_H
#define LSST_QSERV_QUERY_AGGOP_H
/**
  * @file AggOp.h
  *
  * @brief AggOp and AggOp::Mgr declarations
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <map>

// Third-party headers
#include <boost/shared_ptr.hpp>

// Local headers
#include "query/AggRecord.h"
#include "query/ValueExpr.h"


namespace lsst {
namespace qserv {
namespace query {

/// class AggOp is a function object that creates AggRecords from
/// aggregation parameters. It is used to contain the differences in how
/// different aggregation functions (e.g., AVG, MAX, SUM, COUNT, etc) are
/// represented in the parallel and merging portions of query execution. A
/// ValueFactor object represents the column representation being aggregated.
/// TODO: Refactor to eliminate AggOp functors to make things simpler.
class AggOp {
public:
    typedef boost::shared_ptr<AggOp> Ptr;
    class Mgr;
    /// Produce an AggRecord from a ValueFactor.
    virtual AggRecord::Ptr operator()(ValueFactor const& orig) = 0;
    virtual ~AggOp() {}
protected:
    explicit AggOp(Mgr&m) : _mgr(m) {}
    Mgr& _mgr;
};

/// class AggOp::Mgr is a manager which provides lookup for specific AggOp
/// instances according to the proper aggregation operator. Specific AggOp
/// implementations are shielded from dependency. Typically, Mgr is wholly
/// contained within an AggregatePlugin instance. Plugins are created per-query
/// (top-level), because they may contain per-query state.
///
/// Note that AggOp::Mgr is concrete and is not meant to have subclasses
class AggOp::Mgr {
public:
    typedef std::map<std::string, AggOp::Ptr> OpMap;

    Mgr();
    AggOp::Ptr getOp(std::string const& name);
    AggRecord::Ptr applyOp(std::string const& name, ValueFactor const& orig);
    int getNextSeq() { return ++_seq; }
    std::string getAggName(std::string const& name);
    bool hasAggregate() const { return _hasAggregate; }
private:
    OpMap _map;
    int _seq;
    bool _hasAggregate;
};

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_AGGOP_H
