// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2016 AURA/LSST.
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

/// \file
/// \brief Table metadata class implementations.

// Class header
#include "qana/TableInfo.h"

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "query/ColumnRef.h"


namespace { // File-scope helpers



} // anonymous namespace


namespace {

using lsst::qserv::qana::ColumnRefConstPtr;
using lsst::qserv::query::ColumnRef;

LOG_LOGGER _log = LOG_GET("lsst.qserv.qana.TableInfo");

/// `appendColumnRefs` appends all possible references to the given
/// column to `columnRefs`. At most 3 references are appended.
void appendColumnRefs(std::string const& column,
                      std::string const& database,
                      std::string const& table,
                      std::string const& tableAlias,
                      std::vector<ColumnRefConstPtr>& refs)
{
    if (column.empty()) {
        return;
    }
    std::string const _; // an empty string
    refs.push_back(std::make_shared<lsst::qserv::query::ColumnRef>(_, _, column));
    if (!tableAlias.empty()) {
        // If a table alias has been introduced, then it is an error to
        // refer to a column using table.column or db.table.column
        refs.push_back(std::make_shared<ColumnRef>(_, tableAlias, column));
    } else if (!table.empty()) {
        refs.push_back(std::make_shared<ColumnRef>(_, table, column));
        if (!database.empty()) {
            refs.push_back(std::make_shared<ColumnRef>(database, table, column));
        }
    }
}

} // anonymous namespace

namespace lsst {
namespace qserv {
namespace qana {

std::string const TableInfo::CHUNK_TAG("%CC%");
std::string const TableInfo::SUBCHUNK_TAG("%SS%");


std::ostream& operator<<(std::ostream& os, TableInfo const& ti) {
    return os << "TI(" << ti.database << "." << ti.table << " kind=" << ti.kind << ")";
}


std::ostream& operator<<(std::ostream& os, ChildTableInfo const& cti) {
    os << "CTI(" << static_cast<TableInfo const&>(cti)
       << " fk=" << cti.fk
       << " director=(" << *cti.director
       << "))";
    return os;
}


std::ostream& operator<<(std::ostream& os, DirTableInfo const& dti) {
    os << "DTI(" << static_cast<TableInfo const&>(dti)
       << " pk=" << dti.pk
       << " lon=" << dti.lon
       << " lat=" << dti.lat
       << " partId=" << dti.partitioningId
       << ")";
    return os;
}


std::ostream& operator<<(std::ostream& os, MatchTableInfo const& mti) {
    os << "MTI(" << static_cast<TableInfo const&>( mti )
       << " director_1[" << *mti.director.first << "]"
       << " director_2[" << *mti.director.second << "]"
       << " fk_1=" << mti.fk.first
       << " fk_2=" << mti.fk.second
       << ")";
    return os;
}


std::string TableInfo::dump() const {
    std::ostringstream os;
    os << *this;
    return os.str();
}


std::string DirTableInfo::dump() const {
    std::ostringstream os;
    os << *this;
    return os.str();
}


std::string ChildTableInfo::dump() const {
    std::ostringstream os;
    os << *this;
    return os.str();
}


std::string MatchTableInfo::dump() const {
    std::ostringstream os;
    os << *this;
    return os.str();
}


std::vector<ColumnRefConstPtr> const DirTableInfo::makeColumnRefs(
    std::string const& tableAlias) const
{
    std::vector<ColumnRefConstPtr> refs;
    refs.reserve(9);
    appendColumnRefs(pk, database, table, tableAlias, refs);
    appendColumnRefs(lon, database, table, tableAlias, refs);
    appendColumnRefs(lat, database, table, tableAlias, refs);
    return refs;
}

std::vector<ColumnRefConstPtr> const ChildTableInfo::makeColumnRefs(
    std::string const& tableAlias) const
{
    std::vector<ColumnRefConstPtr> refs;
    refs.reserve(3);
    appendColumnRefs(fk, database, table, tableAlias, refs);
    return refs;
}

std::vector<ColumnRefConstPtr> const MatchTableInfo::makeColumnRefs(
    std::string const& tableAlias) const
{
    std::vector<ColumnRefConstPtr> refs;
    refs.reserve(6);
    appendColumnRefs(fk.first, database, table, tableAlias, refs);
    appendColumnRefs(fk.second, database, table, tableAlias, refs);
    return refs;
}

bool DirTableInfo::isEqPredAdmissible(DirTableInfo const& t,
                                      std::string const& a,
                                      std::string const& b,
                                      bool outer) const
{
    // An equality predicate between two directors is only
    // admissible for self joins on the director primary key.
    bool r1 = *this == t; // &&&
    bool r2 = a == pk; // &&&
    bool r3 = b == t.pk; // &&&
    bool res = r1 && r2 && r3; // &&&
    LOGS(_log, LOG_LVL_DEBUG, "&&& DirTableInfo::isEqPredAdmissible a res=" << res
            << " r1=" << r1 << " r2=" << r2 << " r3=" << r3);
    return *this == t && a == pk && b == t.pk;
}

bool DirTableInfo::isEqPredAdmissible(ChildTableInfo const& t,
                                      std::string const& a,
                                      std::string const& b,
                                      bool outer) const
{
    // An equality predicate between a director D and a child is only
    // admissible if the child's director is D, and the column names
    // correspond to the director primary key and child foreign key.
    bool r1 = *this == *t.director; // &&&
    bool r2 = a == pk;  // &&&
    bool r3 = b == t.fk;  // &&&
    bool res = r1 && r2 && r3; // &&&
    LOGS(_log, LOG_LVL_DEBUG, "&&& DirTableInfo::isEqPredAdmissible b res=" << res
                << " r1=" << r1 << " r2=" << r2 << " r3=" << r3);
    return *this == *t.director && a == pk && b == t.fk;
}

bool DirTableInfo::isEqPredAdmissible(MatchTableInfo const& t,
                                      std::string const& a,
                                      std::string const& b,
                                      bool outer) const
{
    // Equality predicates between director and match tables are not
    // admissible in the ON clauses of outer joins.
    if (outer) {
        LOGS(_log, LOG_LVL_DEBUG, "&&& DirTableInfo::isEqPredAdmissible outer false");
        return false;
    }
    // Column a from this table must refer to the primary key for the
    // predicate to be admissible.
    if (a != pk) {
        LOGS(_log, LOG_LVL_DEBUG, "&&& DirTableInfo::isEqPredAdmissible false a=" << a << " pk=" << pk);
        return false;
    }
    // For the predicate to be admissible, this table must be one of the
    // match table directors and b must refer to the corresponding foreign key.
    bool r1a = *this == *t.director.first;  // &&&
    bool r1b = b == t.fk.first; // &&&
    bool r1 = r1a && r1b; // &&&
    bool r2a = *this == *t.director.second; // &&&
    bool r2b = b == t.fk.second; // &&&
    bool r2 = r2a && r2b; // &&&
    bool res = r1 || r2; // &&&
    LOGS(_log, LOG_LVL_DEBUG, "&&& DirTableInfo::isEqPredAdmissible b res=" << res
                    << " r1a=" << r1a << " r1b=" << r1b
                    << " r2a=" << r2a << " r2b=" << r2b
                    << " r1=" << r1 << " r2=" << r2);
    return (*this == *t.director.first && b == t.fk.first) ||
           (*this == *t.director.second && b == t.fk.second);
}

bool ChildTableInfo::isEqPredAdmissible(ChildTableInfo const& t,
                                        std::string const& a,
                                        std::string const& b,
                                        bool outer) const
{
    // An equality predicate between two child tables is only admissible
    // if both tables have the same director, and the column names refer
    // to their foreign keys.
    bool r1 = *director == *t.director; // &&&
    bool r2 = a == fk; // &&&
    bool r3 = b == t.fk; // &&&
    bool res = r1 && r2 && r3; // &&&
    LOGS(_log, LOG_LVL_DEBUG, "&&& ChildTableInfo::isEqPredAdmissible a res=" << res
                << " r1=" << r1 << " r2=" << r2 << " r3=" << r3);
    return *director == *t.director && a == fk && b == t.fk;
}

bool ChildTableInfo::isEqPredAdmissible(MatchTableInfo const& t,
                                        std::string const& a,
                                        std::string const& b,
                                        bool outer) const
{
    // Equality predicates between director and child tables are not
    // admissible in the ON clauses of outer joins.
    if (outer) {
        LOGS(_log, LOG_LVL_DEBUG, "&&& ChildTableInfo::isEqPredAdmissible outer false");
        return false;
    }
    // Column a from this table must refer to the foreign key for the
    // predicate to be admissible.
    if (a != fk) {
        LOGS(_log, LOG_LVL_DEBUG, "&&& ChildTableInfo::isEqPredAdmissible false a=" << a << " fk=" << fk);
        return false;
    }
    // For the predicate to be admissible, the director for this table must be
    // one of the match table directors and b must refer to the corresponding
    // foreign key.
    bool r1a = *director == *t.director.first;  // &&&
    bool r1b = b == t.fk.first; // &&&
    bool r1 = r1a && r1b; // &&&
    bool r2a = *director == *t.director.second; // &&&
    bool r2b = b == t.fk.second; // &&&
    bool r2 = r2a && r2b; // &&&
    bool res = r1 || r2; // &&&
    LOGS(_log, LOG_LVL_DEBUG, "&&& ChildTableInfo::isEqPredAdmissible b res=" << res
            << " r1a=" << r1a << " r1b=" << r1b
            << " r2a=" << r2a << " r2b=" << r2b
            << " r1=" << r1 << " r2=" << r2);
    return (*director == *t.director.first && b == t.fk.first) ||
           (*director == *t.director.second && b == t.fk.second);
}

}}} // namespace lsst::qserv::qana
