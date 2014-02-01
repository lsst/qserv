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
/**
  * @file AggOp.cc
  *
  * @brief PassAggOp, CountAggOp, AccumulateOp, AvgAggOp implementations
  *
  * @author Daniel L. Wang, SLAC
  */

#include "query/AggOp.h"

#include <sstream>
#include <stdexcept>
#include "query/FuncExpr.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"

namespace lsst {
namespace qserv {
namespace master {
////////////////////////////////////////////////////////////////////////
// AggOp specializations
// TODO: Refactor towards functions rather than functors
////////////////////////////////////////////////////////////////////////
/// PassAggOp is a pass-through aggregation. Unused now.
class PassAggOp : public AggOp {
public:
    explicit PassAggOp(AggOp::Mgr& mgr) : AggOp(mgr) {}

    virtual AggRecord::Ptr operator()(ValueFactor const& orig) {
        AggRecord::Ptr arp(new AggRecord());
        arp->orig = orig.clone();
        arp->parallel.push_back(ValueExpr::newSimple(orig.clone()));
        arp->merge = orig.clone();
        // Alias handling left to caller.
        return arp;
    }
};

/// CountAggOp implements COUNT() (COUNT followed by SUM)
class CountAggOp : public AggOp {
public:
    explicit CountAggOp(AggOp::Mgr& mgr) : AggOp(mgr) {}

    virtual AggRecord::Ptr operator()(ValueFactor const& orig) {
        AggRecord::Ptr arp(new AggRecord());
        std::string interName = _mgr.getAggName("COUNT");
        arp->orig = orig.clone();
        boost::shared_ptr<FuncExpr> fe;
        boost::shared_ptr<ValueFactor> vf;
        boost::shared_ptr<ValueExpr> parallelExpr;

        parallelExpr = ValueExpr::newSimple(orig.clone());
        parallelExpr->setAlias(interName);
        arp->parallel.push_back(parallelExpr);

        fe = FuncExpr::newArg1("SUM", interName);
        vf = ValueFactor::newFuncFactor(fe);
        // orig alias handled by caller.
        arp->merge = vf;
        return arp;
    }
};
/// AccumulateOp implements simple aggregations (MIN, MAX, SUM) where
/// the same action may be used in the parallel and merging phases.
class AccumulateOp : public AggOp {
public:
    typedef enum {MIN, MAX, SUM} Type;
    explicit AccumulateOp(AggOp::Mgr& mgr, Type t) : AggOp(mgr) {
        switch(t) {
        case MIN: accName = "MIN"; break;
        case MAX: accName = "MAX"; break;
        case SUM: accName = "SUM"; break;
        }
    }

    virtual AggRecord::Ptr operator()(ValueFactor const& orig) {
        AggRecord::Ptr arp(new AggRecord());
        std::string interName = _mgr.getAggName(accName);
        arp->orig = orig.clone();
        boost::shared_ptr<FuncExpr> fe;
        boost::shared_ptr<ValueFactor> vf;
        boost::shared_ptr<ValueExpr> parallelExpr;

        parallelExpr = ValueExpr::newSimple(orig.clone());
        parallelExpr->setAlias(interName);
        arp->parallel.push_back(parallelExpr);

        fe = FuncExpr::newArg1(accName, interName);
        vf = ValueFactor::newFuncFactor(fe);
        // orig alias handled by caller.
        arp->merge = vf;
        return arp;
    }
    std::string accName;
};

/// AvgAggOp implements AVG (SUM-COUNT followed by SUM/SUM)
class AvgAggOp : public AggOp {
public:
    explicit AvgAggOp(AggOp::Mgr& mgr) : AggOp(mgr) {}

    virtual AggRecord::Ptr operator()(ValueFactor const& orig) {

        AggRecord::Ptr arp(new AggRecord());
        arp->orig = orig.clone();
        // Parallel: get each aggregation subterm.
        boost::shared_ptr<FuncExpr> fe;
        boost::shared_ptr<ValueFactor const> origVf(orig.clone());
        boost::shared_ptr<ValueExpr> ve;
        std::string cAlias = _mgr.getAggName("COUNT");
        fe = FuncExpr::newLike(*origVf->getFuncExpr(), "COUNT");
        ve = ValueExpr::newSimple(ValueFactor::newFuncFactor(fe));
        ve->setAlias(cAlias);
        arp->parallel.push_back(ve);

        std::string sAlias = _mgr.getAggName("SUM");
        fe = FuncExpr::newLike(*origVf->getFuncExpr(), "SUM");
        ve = ValueExpr::newSimple(ValueFactor::newFuncFactor(fe));
        ve->setAlias(sAlias);
        arp->parallel.push_back(ve);

        boost::shared_ptr<FuncExpr> feSum;
        boost::shared_ptr<FuncExpr> feCount;
        feSum = FuncExpr::newArg1("SUM", sAlias);
        feCount = FuncExpr::newArg1("SUM", cAlias);
        ve.reset(new ValueExpr());
        ve->setAlias(orig.getAlias());
        ValueExpr::FactorOpList& factorOps = ve->getFactorOps();
        factorOps.clear();
        ValueExpr::FactorOp fo;
        fo.factor = ValueFactor::newFuncFactor(feSum);
        fo.op = ValueExpr::DIVIDE;
        factorOps.push_back(fo);
        fo.factor = ValueFactor::newFuncFactor(feCount);
        fo.op = ValueExpr::NONE;
        factorOps.push_back(fo);
        arp->merge = ValueFactor::newExprFactor(ve);
        return arp;
    }
};

////////////////////////////////////////////////////////////////////////
// class AggOp::Mgr
////////////////////////////////////////////////////////////////////////
AggOp::Mgr::Mgr() {
    // Load the map
    _map["COUNT"].reset(new CountAggOp(*this));
    _map["AVG"].reset(new AvgAggOp(*this));
    _map["MAX"].reset(new AccumulateOp(*this, AccumulateOp::MAX));
    _map["MIN"].reset(new AccumulateOp(*this, AccumulateOp::MIN));
    _map["SUM"].reset(new AccumulateOp(*this, AccumulateOp::SUM));
    _seq = 0; // Note: accessor return ++_seq
}

AggOp::Ptr
AggOp::Mgr::getOp(std::string const& name) {
    OpMap::const_iterator i = _map.find(name);
    if(i != _map.end()) return i->second;
    else return AggOp::Ptr();
}

AggRecord::Ptr
AggOp::Mgr::applyOp(std::string const& name, ValueFactor const& orig) {
    std::string n(name);
    std::transform(name.begin(), name.end(), n.begin(), ::toupper);
    AggOp::Ptr p = getOp(n);
    if(!p) {
        throw std::invalid_argument("Missing AggOp in applyOp()");
    }
    return (*p)(orig);
}

std::string AggOp::Mgr::getAggName(std::string const& name) {
    std::stringstream ss;
    int s = getNextSeq();
    ss << "QS" << s << "_" << name;
    return ss.str();
}
}}} // lsst::qserv::master
