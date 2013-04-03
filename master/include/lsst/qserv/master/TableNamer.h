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
// TableNamer maintains a namespace for tables referenced in a query.
// Using the FROM clause, it tracks how the table was named in the query, what
// database qualifier was used (if any), what the alias is, and what physical
// table is referred to. Tracking aliasing is important in generating spatial
// restriction expressions.  Sometimes the database name is not available in the
// clause, in which case, the namer must apply the proper database context. 
//
// Other classes defined:
// AliasedRef 
// AliasFunc

#ifndef LSST_QSERV_MASTER_TABLENAMER_H
#define LSST_QSERV_MASTER_TABLENAMER_H
#include <set>
#include <map>
#include <deque>
#include <string>
#include <boost/shared_ptr.hpp>
#include "lsst/qserv/master/common.h"

namespace lsst {
namespace qserv {
namespace master {
class TableRefChecker; // Forward
class TableAliasFunc;

class TableNamer {
public:
    class AliasedRef {
    public:
        AliasedRef(std::string const& logical_,
                   std::string const& db_,
                   std::string const& table_,
                   bool isAlias_ = false)
            : logical(logical_), db(db_), table(table_), isAlias(isAlias_) {}
        std::string logical;
        std::string db;
        std::string table;
        std::string magic;
        bool isAlias;
    };

    friend std::ostream& operator<<(std::ostream& os, AliasedRef const& ar);
    typedef std::deque<AliasedRef> RefDeque;

    explicit TableNamer(TableRefChecker const& checker);
    
    void setDefaultDb(std::string const& db) {
        _defaultDb = db; }

    template <class C>
    void acceptAliases(C const& a) {
        typedef typename C::const_iterator I;
        for(I i=a.begin(); i != a.end(); ++i) {
            _acceptAlias(i->first, i->second);
        }
    }
    void resetTransient();

    boost::shared_ptr<TableAliasFunc> getTableAliasFunc();
    bool getHasChunks() const;
    bool getHasSubChunks() const;
    RefDeque const& getRefs() const { return _refs; }
    
    StringList getBadDbs() const;

    /// @return true if the ref refers to a chunked table.
    bool isChunked(AliasedRef const& r) const;

private:
    class AliasFunc;
    friend class AliasFunc;

    void _acceptAlias(std::string const& logical, 
                      std::string const& physical);
    AliasedRef _computeAliasedRef(std::string const& logical,
                                  std::string const& physical,
                                  bool isAlias); 
    void _computeChunking() const;

    TableRefChecker const& _checker;
    RefDeque _refs;
    
    std::string _defaultDb;
    mutable bool _computed;
    mutable bool _hasChunks;
    mutable bool _hasSubChunks; 
};

}}} // namespace lsst::qserv::master
#endif // LSST_QSERV_MASTER_TABLENAMER_H
