// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2016 AURA/LSST.
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


// Class header
#include "query/DbTablePair.h"

// Qserv headers
#include "query/TableAlias.h"
#include "query/TableRef.h"


namespace lsst {
namespace qserv {
namespace query {


std::shared_ptr<query::ValueExpr>
SelectListAliases::getValueExprMatch(std::shared_ptr<query::ValueExpr const> const& valExpr) const {
    for (auto&& aliasInfo : _aliasInfo) {
        if (valExpr->isSubsetOf(*aliasInfo.object)) {
            return aliasInfo.object;
        }
        if (valExpr->isColumnRef() && aliasInfo.object->isColumnRef()) {
            auto const& columnRef = valExpr->getColumnRef();
            auto const& aliasInfoColumnRef = aliasInfo.object->getColumnRef();
            if (columnRef->getColumn() != aliasInfoColumnRef->getColumn()) {
                continue;
            }
            if (columnRef->getTableRef()->isAliasedBy(*aliasInfo.object->getColumnRef()->getTableRef())) {
                return aliasInfo.object;
            }
        }
    }
    return nullptr;
}



std::pair<std::string, std::shared_ptr<query::TableRefBase>>
TableAliases::getAliasFor(std::string const& db, std::string const& table) const {
    for (auto&& aliasInfo : _aliasInfo) {
        if (not db.empty() && db != aliasInfo.object->getDb()) {
            continue;
        }
        if (table != aliasInfo.object->getTable()) {
            continue;
        }
        return std::make_pair(aliasInfo.alias, aliasInfo.object);
    }
    return std::make_pair(std::string(), nullptr);
}


std::shared_ptr<query::TableRefBase>
TableAliases::getTableRefMatch(std::shared_ptr<query::TableRefBase> const& tableRef) {
    if (nullptr == tableRef) {
        return nullptr;
    }
    for (auto&& aliasInfo : _aliasInfo) {
        if (tableRef->isSubsetOf(*aliasInfo.object)) {
            return aliasInfo.object;
        }
        if (tableRef->isAliasedBy(*aliasInfo.object)) {
            return aliasInfo.object;
        }
    }
    return nullptr;
}


}}} // namespace lsst::qserv::query
