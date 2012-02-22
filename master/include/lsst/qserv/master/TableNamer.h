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
#ifndef LSST_QSERV_MASTER_TABLENAMER_H
#define LSST_QSERV_MASTER_TABLENAMER_H
#include <set>
#include <map>
#include <deque>
#include <string>
#include <boost/shared_ptr.hpp>

namespace lsst {
namespace qserv {
namespace master {
class TableRefChecker; // Forward

class TableNamer {
public:
    class AliasedRef {
    public:
        AliasedRef(std::string const& alias_,
                   std::string const& db_,
                   std::string const& table_) 
            : alias(alias_), db(db_), table(table_) {}
        std::string alias;
        std::string db;
        std::string table;
    };
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
    
    bool getHasChunks() const;
    bool getHasSubChunks() const;
    RefDeque const& getRefs() const { return _refs; }
    
private:
    void _acceptAlias(std::string const& logical, 
                      std::string const& physical);
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
