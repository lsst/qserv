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


void TableInfo::dump(std::ostream& os) const {
    os << "TI(" << database << "." << table << " kind=" << kind << ")";
}

void DirTableInfo::dump(std::ostream& os) const {
    os << "DTI(" << database << "." << table << " kind=" << kind
       << " pk=" << pk
       << " lon=" << lon
       << " lat=" << lat
       << " partId=" << partitioningId
       << ")";
}

void ChildTableInfo::dump(std::ostream& os) const {
    os << "CTI(" << database << "." << table << " kind=" << kind
       << " fk=" << fk
       << " director=(" << *director
       << "))";
}

void MatchTableInfo::dump(std::ostream& os) const {
    os << "MTI(" << database << "." << table << " kind=" << kind
       << " director_1[" << *director.first << "]"
       << " director_2[" << *director.second << "]"
       << " fk_1=" << fk.first
       << " fk_2=" << fk.second
       << ")";
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
    bool selfJoin = (*this == t);
    bool aPK = (a == pk);
    bool bPK = (b == t.pk);
    bool admissible = (selfJoin && aPK && bPK);
    LOGS(_log, LOG_LVL_DEBUG, "a admissible=" << admissible
            << " selfJoin=" << selfJoin << " aPK=" << aPK << " bPK=" << bPK);
    return admissible;
}

bool DirTableInfo::isEqPredAdmissible(ChildTableInfo const& t,
                                      std::string const& a,
                                      std::string const& b,
                                      bool outer) const
{
    // An equality predicate between a director D and a child is only
    // admissible if the child's director is D, and the column names
    // correspond to the director primary key and child foreign key.
    bool childsDirector = (*this == *t.director);
    bool aPK = (a == pk);
    bool bFK = (b == t.fk);
    bool admissible = (childsDirector && aPK && bFK);
    LOGS(_log, LOG_LVL_DEBUG, "b admissible=" << admissible << " childsDirector=" << childsDirector
                           << " aPK=" << aPK << " bFK=" << bFK);
    return admissible;
}

bool DirTableInfo::isEqPredAdmissible(MatchTableInfo const& t,
                                      std::string const& a,
                                      std::string const& b,
                                      bool outer) const
{
    // Equality predicates between director and match tables are not
    // admissible in the ON clauses of outer joins.
    if (outer) {
        LOGS(_log, LOG_LVL_DEBUG, "admissible outer false");
        return false;
    }
    // Column a from this table must refer to the primary key for the
    // predicate to be admissible.
    if (a != pk) {
        LOGS(_log, LOG_LVL_DEBUG, "admissible false a=" << a << " pk=" << pk);
        return false;
    }
    // For the predicate to be admissible, this table must be one of the
    // match table directors and b must refer to the corresponding foreign key.
    bool directorA = (*this == *t.director.first);
    bool aFK = (b == t.fk.first);
    bool directorB = (*this == *t.director.second);
    bool bFK = (b == t.fk.second);
    bool admissible = (directorA && aFK) || (directorB && bFK);
    LOGS(_log, LOG_LVL_DEBUG, "c admissible=" << admissible
                    << " directorA=" << directorA << " aFK=" << aFK
                    << " directorB=" << directorB << " bFK=" << bFK);
    return admissible;
}

bool ChildTableInfo::isEqPredAdmissible(ChildTableInfo const& t,
                                        std::string const& a,
                                        std::string const& b,
                                        bool outer) const
{
    // An equality predicate between two child tables is only admissible
    // if both tables have the same director, and the column names refer
    // to their foreign keys.
    bool sameDirector = (*director == *t.director);
    bool aFK = (a == fk);
    bool bFK = (b == t.fk);
    bool admissible = sameDirector && aFK && bFK;
    LOGS(_log, LOG_LVL_DEBUG, "d admissible=" << admissible
                << " sameDirector=" << sameDirector << " aFK=" << aFK << " bFK=" << bFK);
    return admissible;
}

bool ChildTableInfo::isEqPredAdmissible(MatchTableInfo const& t,
                                        std::string const& a,
                                        std::string const& b,
                                        bool outer) const
{
    // Equality predicates between director and child tables are not
    // admissible in the ON clauses of outer joins.
    if (outer) {
        LOGS(_log, LOG_LVL_DEBUG, "admissible outer false");
        return false;
    }
    // Column a from this table must refer to the foreign key for the
    // predicate to be admissible.
    if (a != fk) {
        LOGS(_log, LOG_LVL_DEBUG, "admissible false a=" << a << " fk=" << fk);
        return false;
    }
    // For the predicate to be admissible, the director for this table must be
    // one of the match table directors and b must refer to the corresponding
    // foreign key.
    bool matchDirFirst = (*director == *t.director.first);
    bool bFKFirst = (b == t.fk.first);
    bool matchDirSecond = (*director == *t.director.second);
    bool fKSecond = (b == t.fk.second);
    bool admissible = (matchDirFirst && bFKFirst) || (matchDirSecond && fKSecond);
    LOGS(_log, LOG_LVL_DEBUG, "e admissible=" << admissible
            << " matchDirFirst=" << matchDirFirst << " bFKFirst=" << bFKFirst
            << " matchDirSecond=" << matchDirSecond << " fKSecond=" << fKSecond);
    return admissible;
}

}}} // namespace lsst::qserv::qana
