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
// PlanWriter writes SelectPlans from SelectStmts. It does this using
// a sequence of rewrite rules applied on the query and a parameter
// set derived with help from physical table information.
#include "lsst/qserv/master/PlanWriter.h"

#include <functional>
#include <iostream>
#include <iterator>
#include "lsst/qserv/master/QueryTemplate.h"
#include "lsst/qserv/master/SelectStmt.h"
#include "lsst/qserv/master/WhereClause.h"
#include "lsst/qserv/master/transaction.h"


namespace qMaster=lsst::qserv::master;
using lsst::qserv::master::ChunkSpecList;
using lsst::qserv::master::Constraint;
using lsst::qserv::master::ConstraintVector;
using lsst::qserv::master::PlanWriter;
using lsst::qserv::master::QueryTemplate;
using lsst::qserv::master::QsRestrictor;
using lsst::qserv::master::SelectPlan;
using lsst::qserv::master::SelectStmt;
using lsst::qserv::master::WhereClause;

namespace { // File-scope helpers
Constraint makeConstraint(QsRestrictor::Ptr p) {
    Constraint c;
    assert(p.get());
    c.name = p->_name;
    std::copy(p->_params.begin(), p->_params.end(), 
              std::back_inserter(c.params));
    return c;
}
class MyMapping {
};
class Rule { // Operate on a SelectStmt in-place.
public:
    virtual ~Rule() {}
    virtual void operator()(SelectStmt& s) {}
};

class SpatialRestr : public Rule {
public :
    virtual void operator()(SelectStmt& s);
    boost::shared_ptr<ConstraintVector> getConstraints() {
        boost::shared_ptr<ConstraintVector> cv;
        return cv;
    }
    void dbgConstraint() {
    }
private:
    boost::shared_ptr<ConstraintVector> _cv;
};

void SpatialRestr::operator()(SelectStmt& s) {
        // For restrictors in where clause
        // replace with value and funcexprs.
        boost::shared_ptr<WhereClause const> wc = s.getWhere();
        if(!wc.get()) { return; }

        boost::shared_ptr<QsRestrictor::List const> restrs;
        restrs = wc->getRestrs();
        if(!restrs.get()) { return; }

        // Extract constraints
        _cv.reset(new ConstraintVector());
        std::transform(restrs->begin(), restrs->end(), 
                       std::back_inserter(*_cv), 
                       std::ptr_fun(makeConstraint)); 
        // Convert restrictors into expressions.
        // Add to And-statement.
    }
}
/// An container for a query statements after being decomposed into a
/// sequence of statements for dispatch ("map") and a sequence for
/// merging/collection ("reduce"). 
/// 
/// In this first implementation, there are no multi-step sequences
/// (though RefObject-like rules are expected later. Hence, we will
/// not implement specifics for multi-step sequences other than
/// allowing their storage (for now).
class PlanTemplate {
public:
    PlanTemplate(boost::shared_ptr<SelectStmt> stmt);
    qMaster::QueryTemplateList const& getExecList() const;
    
    qMaster::QueryTemplateList const& getMergeList() const;

private:
    qMaster::QueryTemplateList _exec;
    qMaster::QueryTemplateList _merge;
};

class MapPlan : public Rule {
public:
    virtual void operator()(SelectStmt& s) {
        // If aggregation, patch select list
        // and write new merge instructions
        // Otherwise, write simple merge instructions

        // To reuse existing infrastructure (don't re-implement code
        // for merging... yet), we should just compose a
        // TableMergerConfig, in which the portion of interest is the
        // MergeFixup class mergeTypes.h 
    }
    QueryTemplate const& getTemplate() const { return _mergeQt; };

private:
    void _makeSimpleMerge(SelectStmt& s) {
        // A simple aggregation/user result occurs via:
        // "select * from resulttable limit, orderby;"

        // Need: limit, orderby (no group-by)
    }
    void _makeAggrMerge(SelectStmt& s) {
        // Generating aggregation 
        // "select <aggr[col]> as <aggr/alias> from resulttable limit, orderby;"
        
        // Inputs:
        // limit, orderby, group-by ("having" already processed)
        
        // for_each column expr, translate into new column expr with alias.

        
    }
    void computeSelect() {
        // iterate over select list.
        // for each, find entry in agg record, grab pass and select.
        // if not found, then just reproduce orig.
        
        // Post: get group-by clause and reproduce.
    }

    QueryTemplate _mapQt; // parameterized: resultMaster, resultWorker
    QueryTemplate _mergeQt;
};


boost::shared_ptr<SelectPlan> 
PlanWriter::write(SelectStmt const& ss, ChunkSpecList const& specs) {
    boost::shared_ptr<SelectStmt> mapS = ss.copySyntax();
    QueryTemplate orig = ss.getTemplate();
    std::cout << "ORIGINAL: " << orig.dbgStr() << std::endl;
    MapPlan mp;
    mp(*mapS);
    // OBSOLETE.
    SpatialRestr sr;
    sr(*mapS);
    

    std::cout << "MAPPED: " << mp.getTemplate().dbgStr() << std::endl;
    return boost::shared_ptr<SelectPlan>();
}
