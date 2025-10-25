// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2017 AURA/LSST.
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
 * @brief TableRefN, SimpleTableN, JoinRefN implementations
 *
 * @author Daniel L. Wang, SLAC
 */

// Class header
#include "query/TableRef.h"

// System headers
#include <algorithm>
#include <sstream>
#include <stdexcept>

// Third-party headers
#include "boost/lexical_cast.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "query/JoinRef.h"
#include "query/JoinSpec.h"
#include "util/IterableFormatter.h"
#include "util/PointerCompare.h"

namespace {

lsst::qserv::query::JoinRef::Ptr joinRefClone(lsst::qserv::query::JoinRef::Ptr const& r) {
    return r->clone();
}

LOG_LOGGER _log = LOG_GET("lsst.qserv.query.TableRef");

}  // anonymous namespace

namespace lsst::qserv::query {

TableRef::TableRef() {}

TableRef::TableRef(std::string const& db, std::string const& table, std::string const& alias)
        : _db(db), _table(table), _alias(alias) {
    _verify();
}

void TableRef::setAlias(std::string const& alias) {
    // LOGS(_log, LOG_LVL_TRACE, *this << "; set alias:" << alias);
    _alias = alias;
}

void TableRef::setDb(std::string const& db) {
    // LOGS(_log, LOG_LVL_TRACE, *this << "; set db:" << db);
    _db = db;
    _verify();
}

void TableRef::setTable(std::string const& table) {
    // LOGS(_log, LOG_LVL_TRACE, *this << "; set table:" << table);
    _table = table;
    _verify();
}

bool TableRef::isSubsetOf(TableRef const& rhs) const {
    if (not isSimple() || not rhs.isSimple()) {
        // We can investigate adding support for this if needed but I don't think it will be.
        // I think it would also be worth considering if it would be a better abstraction if we removed
        // the JoinRef from TableRef and making the JoinRef contain the joined TableRefs.
        throw std::logic_error("TableRef does not support isSubsetOf with joins.");
    }
    // if the _table is empty, the _db must be empty
    if (not hasTable() && hasDb()) {
        return false;
    }
    if (not rhs.hasTable() && rhs.hasDb()) {
        return false;
    }

    if (hasAlias() && getAlias() != rhs.getAlias()) {
        return false;
    }
    if (hasDb() && getDb() != rhs.getDb()) {
        return false;
    }
    if (hasTable() && getTable() != rhs.getTable()) {
        return false;
    }
    return true;
}

bool TableRef::isAliasedBy(TableRef const& rhs) const {
    if (hasTable() && not hasDb() && not hasAlias()) {
        if (_table == rhs._alias) return true;
    }
    return false;
}

bool TableRef::isComplete() const {
    if (_table.empty()) return false;
    if (_db.empty()) return false;
    if (_alias.empty()) return false;
    for (auto&& joinRef : _joinRefs) {
        if (not joinRef->getRight()->isComplete()) {
            return false;
        }
    }
    return true;
}

bool TableRef::operator<(const TableRef& rhs) const {
    return std::tie(_db, _table, _alias) < std::tie(rhs._db, rhs._table, rhs._alias);
}

std::ostream& operator<<(std::ostream& os, TableRef const& ref) {
    os << "TableRef(";
    os << "\"" << ref._db << "\"";
    os << ", \"" << ref._table << "\"";
    os << ", \"" << ref._alias << "\"";
    if (!ref._joinRefs.empty()) {
        os << ", " << util::printable(ref._joinRefs, "", "");
    }
    os << ")";
    return os;
}

std::ostream& operator<<(std::ostream& os, TableRef const* ref) {
    if (nullptr == ref) {
        os << "nullptr";
    } else {
        os << *ref;
    }
    return os;
}

void TableRef::render::applyToQT(TableRef const& ref) {
    if (_count++ > 0) _qt.append(",");
    ref.putTemplate(_qt);
}

std::ostream& TableRef::putStream(std::ostream& os) const {
    os << "Table(" << _db << "." << _table << ")";
    if (!_alias.empty()) {
        os << " AS " << _alias;
    }
    typedef JoinRefPtrVector::const_iterator Iter;
    for (Iter i = _joinRefs.begin(), e = _joinRefs.end(); i != e; ++i) {
        JoinRef const& j = **i;
        os << " " << j;
    }
    return os;
}

std::string TableRef::sqlFragment() const {
    QueryTemplate qt;
    TableRef::render render(qt);
    render.applyToQT(*this);
    return boost::lexical_cast<std::string>(qt);
}

void TableRef::putTemplate(QueryTemplate& qt) const {
    auto aliasMode = qt.getTableAliasMode();
    if (QueryTemplate::USE == aliasMode) {
        if (hasAlias()) {
            qt.appendIdentifier(_alias);
        } else {
            if (!_db.empty()) {
                qt.appendIdentifier(_db);
                qt.append(".");
            }
            if (!_table.empty()) {
                qt.appendIdentifier(_table);
            }
        }
    } else {  // DEFINE or DONT_USE
        if (!_db.empty()) {
            qt.appendIdentifier(_db);
            qt.append(".");
        }
        if (!_table.empty()) {
            qt.appendIdentifier(_table);
        }
    }
    if (QueryTemplate::DEFINE == aliasMode) {
        if (hasAlias()) {
            qt.append("AS");
            qt.appendIdentifier(_alias);
        }
    }
    typedef JoinRefPtrVector::const_iterator Iter;
    for (Iter i = _joinRefs.begin(), e = _joinRefs.end(); i != e; ++i) {
        JoinRef const& j = **i;
        j.putTemplate(qt);
    }
}

void TableRef::addJoin(std::shared_ptr<JoinRef> r) { _joinRefs.push_back(r); }

void TableRef::addJoins(const JoinRefPtrVector& r) {
    _joinRefs.insert(std::end(_joinRefs), std::begin(r), std::end(r));
}

void TableRef::verifyPopulated(std::string const& defaultDb) {
    // it should not be possible to construct a TableRef with an empty table, but just to be sure:
    if (_table.empty()) {
        throw std::logic_error("No table in TableRef");
    }
    if (_db.empty()) {
        if (defaultDb.empty()) {
            throw std::logic_error("No db in TableRef");
        } else {
            _db = defaultDb;
        }
    }
    for (auto&& joinRef : _joinRefs) {
        auto&& rightTableRef = joinRef->getRight();
        if (rightTableRef != nullptr) rightTableRef->verifyPopulated(defaultDb);
    }
}

void TableRef::apply(TableRef::Func& f) {
    f(*this);
    typedef JoinRefPtrVector::iterator Iter;
    for (Iter i = _joinRefs.begin(), e = _joinRefs.end(); i != e; ++i) {
        JoinRef& j = **i;
        j.getRight()->apply(f);
    }
}

void TableRef::apply(TableRef::FuncC& f) const {
    f(*this);
    typedef JoinRefPtrVector::const_iterator Iter;
    for (Iter i = _joinRefs.begin(), e = _joinRefs.end(); i != e; ++i) {
        JoinRef const& j = **i;
        j.getRight()->apply(f);
    }
}

TableRef::Ptr TableRef::clone() const {
    TableRef::Ptr newCopy = std::make_shared<TableRef>(_db, _table, _alias);
    std::transform(_joinRefs.begin(), _joinRefs.end(), std::back_inserter(newCopy->_joinRefs), joinRefClone);
    return newCopy;
}

bool TableRef::operator==(TableRef const& rhs) const {
    if (std::tie(_db, _table, _alias) != std::tie(rhs._db, rhs._table, rhs._alias)) return false;
    return util::vectorPtrCompare<JoinRef>(_joinRefs, rhs._joinRefs);
}

void TableRef::_verify() const {
    if (_table.empty() && hasDb()) {
        throw std::logic_error("Table can not be empty when database is populated.");
    }
}

}  // Namespace lsst::qserv::query
