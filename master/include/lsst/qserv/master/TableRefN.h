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
// TableRefN is a table ref node in a parsed query.
#ifndef LSST_QSERV_MASTER_TABLEREFN_H
#define LSST_QSERV_MASTER_TABLEREFN_H
#include <string>
#include <list>
#include <iostream>
#include <boost/shared_ptr.hpp>
#include "lsst/qserv/master/QueryTemplate.h"

namespace lsst {
namespace qserv {
namespace master {
class QueryTemplate; // Forward

class TableRefN {
public:
    typedef boost::shared_ptr<TableRefN> Ptr;
    virtual ~TableRefN() {}
    virtual std::string const& getAlias() const { return alias; }
    virtual std::string const& getDb() const = 0;
    virtual std::string const& getTable() const = 0;
    virtual std::ostream& putStream(std::ostream& os) const = 0;
    virtual void putTemplate(QueryTemplate& qt) const = 0;
    // Modifiers:
    virtual void setAlias(std::string const& a) { alias=a; }
    virtual void setDb(std::string const& db) = 0;
    virtual void setTable(std::string const& table) = 0;

    class Func {
    public:
        virtual void operator()(TableRefN& t) {}
    };
    template <class F>
    class Fwrapper {
    public:
        Fwrapper(F& f_) : f(f_) {}
        inline void operator()(TableRefN::Ptr& t) {
            if(t.get()) { t->apply(f); } }
        F& f;
    };
        
    // apply f() over all all tableRefns in depth-first order (for compound
    // tablerefs)
    virtual void apply(Func& f) {}

    class render;
protected:

    TableRefN(std::string const& alias_) : alias(alias_) {}
    inline void _putAlias(QueryTemplate& qt) const {
        if(!alias.empty()) { 
            qt.append("AS"); 
            qt.append(alias); 
        }
    }
    std::string alias;
    
};
std::ostream& operator<<(std::ostream& os, TableRefN const& refN);
std::ostream& operator<<(std::ostream& os, TableRefN const* refN);

class TableRefN::render {
public:
    render(QueryTemplate& qt) : _qt(qt), _count(0) {}
    void operator()(TableRefN const& trn);
    void operator()(TableRefN::Ptr const trn) {
        if(trn.get()) (*this)(*trn);
    }
    QueryTemplate& _qt;
    int _count;
};

class SimpleTableN : public TableRefN {
public:
    typedef boost::shared_ptr<SimpleTableN> Ptr;
    SimpleTableN(std::string const& db_, std::string const& table_,
                 std::string const& alias_) 
        : TableRefN(alias_), db(db_), table(table_)  {}
    
    virtual std::string const& getDb() const { return db; }
    virtual std::string const& getTable() const { return table; }
    virtual std::ostream& putStream(std::ostream& os) const {
        os << "Table(" << db << "." << table << ")";
        if(!alias.empty()) { os << " AS " << alias; }
        return os;
    }
    virtual void putTemplate(QueryTemplate& qt) const {
        qt.append(*this);
        _putAlias(qt);
    }
    // Modifiers
    virtual void setDb(std::string const& db_) { db = db_; }
    virtual void setTable(std::string const& table_) { table = table_; }
    virtual void apply(Func& f) { f(*this); }
protected:
//    std::string alias; // inherited
    std::string db;
    std::string table;
};

class JoinRefN : public TableRefN {
public:
    enum JoinType {DEFAULT, INNER, LEFT, RIGHT, NATURAL, CROSS, FULL};

    JoinRefN(std::string const& db1_, std::string const& table1_, 
             std::string const& db2_, std::string const& table2_,
             JoinType jt, std::string const& condition_, 
             std::string const& alias_) 
        : TableRefN(alias_),
          db1(db1_), table1(table1_), db2(db2_), table2(table2_),
          joinType(jt), condition(condition_) {}

    virtual std::string const& getTable() const { 
        static std::string s;
        return s; 
    }
    virtual std::string const& getDb() const { return getTable(); }

    JoinType getJoinType() const { return joinType; }
    std::string getDb1() { return db1; }
    std::string getDb2() { return db2; }
    std::string getTable1() { return table1; }
    std::string getTable2() { return table2; }
    std::string getCondition() { return condition; }

    virtual std::ostream& putStream(std::ostream& os) const {
        os << "Join(" << db1 << "." << table1 << ", "
           << db2 << "." << table2 << ", " << condition << ")";
        if(!alias.empty()) { os << " AS " << alias; }
        return os;
    }
    virtual void putTemplate(QueryTemplate& qt) const {
        // FIXME: need to pass Join decorator into template.
        qt.append(SimpleTableN(db1, table1, ""));
        qt.append("JOIN");
        qt.append(SimpleTableN(db2, table2, ""));
        _putAlias(qt);
    }
    // Modifiers
    virtual void setDb(std::string const&) {} // FIXME: ignore?
    virtual void setTable(std::string const&) {} // FIXME: ignore?
    virtual void apply(Func& f);

protected:
    std::string db1;
    std::string table1;
    std::string db2;
    std::string table2;
    JoinType joinType;
    std::string condition; // for now, use a dumb string.

};

// Containers
typedef std::list<TableRefN::Ptr> TableRefnList;
typedef boost::shared_ptr<TableRefnList> TableRefnListPtr;

}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_TABLEREFN_H
