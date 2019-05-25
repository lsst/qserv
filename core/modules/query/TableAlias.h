// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2019 LSST Corporation.
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


#ifndef LSST_QSERV_QUERY_TABLEALIAS_H
#define LSST_QSERV_QUERY_TABLEALIAS_H


// System headers
#include <map>
#include <vector>
#include <sstream>
#include <stdexcept>

// Third-party headers
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/identity.hpp>
#include <boost/multi_index/member.hpp>

// Local headers
#include "query/DbTablePair.h"
#include "query/TableRef.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"


namespace lsst {
namespace qserv {
namespace query {


template <typename T>
class Aliases {
public:
    Aliases() = default;
    virtual ~Aliases() = default;

    bool set(T const& object, std::string const& alias) {
        for (auto&& aliasInfo : _aliasInfo) {
            if (alias == aliasInfo.alias) {
                return false;
            }
        }
        _aliasInfo.emplace_back(object, alias);
        return true;
    }

    // get an alias for a given object
    std::string get(T const& object) const {
        for (auto&& aliasInfo : _aliasInfo) {
            if (object.compareValue(aliasInfo.object)) { // nptodo probably want a compare functor or something
                return aliasInfo.alias;
            }
        }
        return std::string();
    }

    // get the first-registered object for a given alias
    T& get(std::string const& alias) const {
        for (auto&& aliasInfo : _aliasInfo) {
            if (alias == aliasInfo.alias) {
                return aliasInfo.object;
            }
        }
        return T();
    }

protected:
    struct AliasInfo {
        T object;
        std::string alias;
        AliasInfo(T const& object_, std::string const& alias_) : object(object_), alias(alias_) {}
    };
    std::vector<AliasInfo> _aliasInfo;
};


class SelectListAliases : public Aliases<std::shared_ptr<query::ValueExpr>> {
public:
    SelectListAliases() = default;

    /**
     * @brief Get the alias for a ColumnRef
     *
     * Looks first for an exact match (all fields must match). Then looks for the first "subset" match
     * (for example "objectId" would match "Object.objectId").
     *
     * @param columnRef
     * @return std::string
     */
    std::pair<std::string, std::shared_ptr<query::ValueExpr>>
    getAliasFor(query::ColumnRef const& columnRef) const {
        AliasInfo const* subsetMatch = nullptr;
        for (auto&& aliasInfo : _aliasInfo) {
            auto&& factorOps = aliasInfo.object->getFactorOps();
            if (factorOps.size() == 1) {
                auto&& aliasColumnRef = factorOps[0].factor->getColumnRef();
                if (nullptr != aliasColumnRef) {
                    if (columnRef == *aliasColumnRef) {
                        return std::make_pair(aliasInfo.alias, aliasInfo.object);
                    }
                    if (nullptr == subsetMatch && columnRef.isSubsetOf(aliasColumnRef)) {
                        subsetMatch = &aliasInfo;
                    }
                }
            }
        }
        if (nullptr != subsetMatch) {
            return std::make_pair(subsetMatch->alias, subsetMatch->object);
        }
        return std::make_pair(std::string(), nullptr);
    }

    /**
     * @brief Get a ValueExpr from the list of ValueExprs used in the SELECT statement that matches a given
     *        ValueExpr.
     *
     * @param valExpr the expr to match
     * @return std::shared_ptr<query::ValueExpr> that matching expr from the SELECT list, or nullptr
     */
    std::shared_ptr<query::ValueExpr> getValueExprMatch(
        std::shared_ptr<query::ValueExpr const> const& valExpr) const;

};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_TABLEALIAS_H
