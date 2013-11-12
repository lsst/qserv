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
#ifndef LSST_QSERV_MASTER_GROUPBYCLAUSE_H
#define LSST_QSERV_MASTER_GROUPBYCLAUSE_H
/**
  * @file GroupByClause.h
  *
  * @brief GroupByClause is a representation of a group-by clause element.
  *
  * @author Daniel L. Wang, SLAC
  */
#include <boost/shared_ptr.hpp>
#include <deque>
#include <string>

namespace lsst { namespace qserv { namespace master {

// Forward
class QueryTemplate;
class ValueExpr;

// GroupByTerm is a element of a GroupByClause
class GroupByTerm {
public:
    class render;
    friend class render;

    GroupByTerm() {}
    ~GroupByTerm() {}

    boost::shared_ptr<const ValueExpr> getExpr();
    std::string getCollate() const;

private:
    friend std::ostream& operator<<(std::ostream& os, GroupByTerm const& gb);
    friend class ModFactory;

    boost::shared_ptr<ValueExpr> _expr;
    std::string _collate;
};
/// GroupByClause is a parsed GROUP BY ... element.
class GroupByClause {
public:
    typedef std::deque<GroupByTerm> List;

    GroupByClause() : _terms(new List()) {}
    ~GroupByClause() {}

    std::string getGenerated();
    void renderTo(QueryTemplate& qt) const;
    boost::shared_ptr<GroupByClause> copyDeep();
    boost::shared_ptr<GroupByClause> copySyntax();

private:
    friend std::ostream& operator<<(std::ostream& os, GroupByClause const& gc);
    friend class ModFactory;

    void _addTerm(GroupByTerm const& t) { _terms->push_back(t); }
    boost::shared_ptr<List> _terms;
};
}}} // namespace lsst::qserv::master
#endif // LSST_QSERV_MASTER_GROUPBYCLAUSE_H

