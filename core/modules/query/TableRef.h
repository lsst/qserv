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

#ifndef LSST_QSERV_QUERY_TABLEREF_H
#define LSST_QSERV_QUERY_TABLEREF_H
/**
  * @file
  *
  * @brief Declarations for TableRefN and subclasses SimpleTableN and JoinRefN
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

// Third-party headers
#include "boost/shared_ptr.hpp"

// Local headers
#include "query/DbTablePair.h"
#include "query/QueryTemplate.h"

namespace lsst {
namespace qserv {
namespace query {

class QueryTemplate; // Forward
class JoinSpec;
class JoinRef;
typedef std::vector<boost::shared_ptr<JoinRef> > JoinRefPtrVector;

/// TableRefN is a parsed table reference node
// table_ref :
//   table_ref_aux (options{greedy=true;}:qualified_join | cross_join)*
// table_ref_aux :
//   (n:table_name | /*derived_table*/q:table_subquery) ((as:"as")? c:correlation_name (LEFT_PAREN derived_column_list RIGHT_PAREN)?)?
class TableRef {
public:
    typedef boost::shared_ptr<TableRef> Ptr;
    typedef boost::shared_ptr<TableRef const> CPtr;

    TableRef(std::string const& db_, std::string const& table_,
               std::string const& alias_)
        : _alias(alias_), _db(db_), _table(table_)  {
        if(table_.empty()) { throw std::logic_error("TableRef without table"); }
    }
    virtual ~TableRef() {}

    std::ostream& putStream(std::ostream& os) const;
    void putTemplate(QueryTemplate& qt) const;

    bool isSimple() const { return _joinRefs.empty(); }
    std::string const& getDb() const { return _db; }
    std::string const& getTable() const { return _table; }
    std::string const& getAlias() const { return _alias; }
    JoinRefPtrVector const& getJoins() const { return _joinRefs; }

    // Modifiers
    void setAlias(std::string const& a) { _alias=a; }
    void setDb(std::string const& db_) { _db = db_; }
    void setTable(std::string const& table_) { _table = table_; }
    JoinRefPtrVector& getJoins() { return _joinRefs; }
    void addJoin(boost::shared_ptr<JoinRef> r);

    class Func {
    public:
        virtual ~Func() {}
        virtual void operator()(TableRef& t)=0;
    };
    class FuncC {
     public:
        virtual ~FuncC() {}
        virtual void operator()(TableRef const& t)=0;
    };
    void apply(Func& f);
    void apply(FuncC& f) const;

    TableRef::Ptr clone() const;

    class render;
private:

    std::string _alias;
    std::string _db;
    std::string _table;
    JoinRefPtrVector _joinRefs;
};

class TableRef::render {
public:
    render(QueryTemplate& qt) : _qt(qt), _count(0) {}
    void operator()(TableRef const& trn);
    inline void operator()(TableRef::Ptr const trn) {
        if(trn.get()) (*this)(*trn);
    }
    QueryTemplate& _qt;
    int _count;
};

std::ostream& operator<<(std::ostream& os, TableRef const& refN);
std::ostream& operator<<(std::ostream& os, TableRef const* refN);

// Containers
typedef std::vector<TableRef::Ptr> TableRefList;
typedef boost::shared_ptr<TableRefList> TableRefListPtr;

}}} // namespace lsst::qserv::query
#endif // LSST_QSERV_QUERY_TABLEREF_H
