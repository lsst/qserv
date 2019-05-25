// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2017 LSST Corporation.
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
  * @brief Declarations for TableRefN and subclasses SimpleTableN and JoinRefN
  *
  * @author Daniel L. Wang, SLAC
  */


#ifndef LSST_QSERV_QUERY_TABLEREF_H
#define LSST_QSERV_QUERY_TABLEREF_H


// System headers
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

// Local headers
#include "query/DbTablePair.h"
#include "query/QueryTemplate.h"


// Forward declarations
namespace lsst {
namespace qserv {
namespace query {
    class QueryTemplate;
    class JoinSpec;
    class JoinRef;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace query {


typedef std::vector<std::shared_ptr<JoinRef> > JoinRefPtrVector;


// TableRefBase is a TableRef without joins. It contains the database, table, and alias.
class TableRefBase {
public:
    TableRefBase(std::string const& db, std::string const& table, std::string const& alias);

    virtual ~TableRefBase() = default;

    std::string const& getDb() const;
    std::string const& getTable() const;
    std::string const& getAlias() const;

    void setDb(std::string const& db);
    void setTable(std::string const& table);
    void setAlias(std::string const& alias);

    bool hasDb() const;
    bool hasTable() const;
    bool hasAlias() const;

    virtual void verifyPopulated(std::string const& defaultDb);

    // nptodo this doesn't really work with the TableRef subclass (which has JoinRefs)
    // maybe there needs to be another subclass TableRefWithoutJoin or something more
    // elegant
    /**
     * @brief Find out if this TableRefBase is the same as another TableRefBase, where the database & column fields
     *        in this table ref may not be populated.
     *
     * For example, if the database is not populated in this it is ignored during comparison.
     * It is required that if the database is populated that the Table also be populated.
     * If the alias is populated it is included in the check.
     *
     * @return true if the populated fields of this match the populated fields of rhs *and* if database is
     *         populated that table is populated as well.
     * @return false if populated fields of this do not match popualted fields of rhs or if database is
     *         populated but table is not.
     */
    virtual bool isSubsetOf(TableRefBase const& rhs) const;

    /**
     * @brief Find out if this TableRefBase is using the alias of another TableRefBase
     *
     * If only the table is populated in this object and it matches the alias of the other object then this
     * object is the same as, the alias of, the other object.
     *
     * @param rhs
     * @return bool
     */
    virtual bool isAliasedBy(TableRefBase const& rhs) const;

    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void putTemplate(QueryTemplate& qt) const;
    virtual std::string sqlFragment() const;

    bool operator==(TableRefBase const& rhs) const;

    bool operator<(const TableRefBase& rhs) const;

    // Compare this TableRef to rhs and return true if it is less than the other. If useAlias is true this
    // will use the alias and igore the db and table.
    // That is, "x.y AS a" will be less than "a.b as b" because a < b.
    virtual bool lessThan(TableRefBase const& rhs, bool useAlias) const;

    virtual bool equal(TableRefBase const& rhs, bool useAlias) const;

protected:
    std::string _db;
    std::string _table;
    std::string _alias;

private:
    friend std::ostream& operator<<(std::ostream& os, TableRefBase const& refN);
    friend std::ostream& operator<<(std::ostream& os, TableRefBase const* refN);
};


/// TableRefN is a parsed table reference node
// table_ref :
//   table_ref_aux (options{greedy=true;}:qualified_join | cross_join)*
// table_ref_aux :
//   (n:table_name | /*derived_table*/q:table_subquery) ((as:"as")? c:correlation_name (LEFT_PAREN derived_column_list RIGHT_PAREN)?)?
class TableRef : public TableRefBase {
public:
    typedef std::shared_ptr<TableRef> Ptr;
    typedef std::shared_ptr<TableRef const> CPtr;

    TableRef(std::string const& db_, std::string const& table_,
               std::string const& alias_)
            : TableRefBase(db_, table_, alias_) {
        if(table_.empty() && _alias.empty()) { throw std::logic_error("TableRef without table or alias."); }
    }

    virtual ~TableRef() {}

    std::ostream& putStream(std::ostream& os) const override;
    void putTemplate(QueryTemplate& qt) const override;
    std::string sqlFragment() const override;

    bool isSimple() const { return _joinRefs.empty(); }
    JoinRefPtrVector const& getJoins() const { return _joinRefs; }
    /// Get all the db+table names used by this TableRef and all of its joins.
    void getRelatedDbTableInfo(std::vector<DbTablePair>& dbTablePairs) const;

    // Modifiers
    JoinRefPtrVector& getJoins() { return _joinRefs; }
    void addJoin(std::shared_ptr<JoinRef> r);
    void addJoins(const JoinRefPtrVector& r);

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

    /**
     * @brief Verify the table is set and set a database if one is not set. Recurses to all join refs.
     *
     * @throws If an empty string is passed for default then this will throw if the value is not set in the
     *         instance.
     *
     * @param defaultDb the default database to assign, or an empty string for no default.
     */
    void verifyPopulated(std::string const& defaultDb=std::string()) override;

    TableRef::Ptr clone() const;

    bool operator==(TableRef const& rhs) const;

    // bool lessThan(TableRefBase const& rhs, bool useAlias) const override;

    class render;
private:
    friend std::ostream& operator<<(std::ostream& os, TableRef const& refN);
    friend std::ostream& operator<<(std::ostream& os, TableRef const* refN);

    JoinRefPtrVector _joinRefs;
};


class TableRef::render {
public:
    render(QueryTemplate& qt) : _qt(qt), _count(0) {}
    void applyToQT(TableRef const& trn);
    inline void applyToQT(TableRef::Ptr const trn) {
        if(trn != nullptr) applyToQT(*trn);
    }
    QueryTemplate& _qt;
    int _count;
};


// Containers
typedef std::vector<TableRef::Ptr> TableRefList;
typedef std::shared_ptr<TableRefList> TableRefListPtr;


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_TABLEREF_H
